/*
 * Copyright (c) 2026 NVIDIA Corporation
 *
 * Licensed under the Apache License Version 2.0 with LLVM Exceptions
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *   https://llvm.org/LICENSE.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

// macOS-only: wraps FSEvents as a stdexec sequence sender.

#include <CoreServices/CoreServices.h>
#include <dispatch/dispatch.h>

#include "exec/libdispatch_queue.hpp"
#include "exec/sequence_senders.hpp"
#include "on_scheduler.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <exception>
#include <memory>
#include <optional>
#include <semaphore>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace fsx
{
  struct fs_event
  {
    std::string             path;
    FSEventStreamEventFlags flags;
    FSEventStreamEventId    id;
  };

  struct fs_batch
  {
    std::span<const fs_event> events;
    FSEventStreamEventId      last_id;
    bool                      had_drops;
    bool                      must_rescan;
  };

  inline constexpr FSEventStreamEventFlags kDropMask = kFSEventStreamEventFlagMustScanSubDirs
                                                     | kFSEventStreamEventFlagUserDropped
                                                     | kFSEventStreamEventFlagKernelDropped
                                                     | kFSEventStreamEventFlagRootChanged;

  inline auto is_drop_notice(const fs_event& __e) noexcept -> bool
  {
    return (__e.flags & kDropMask) != 0;
  }

  struct watch_options
  {
    FSEventStreamEventId     since   = kFSEventStreamEventIdSinceNow;
    CFAbsoluteTime           latency = 0.2;
    FSEventStreamCreateFlags create_flags = kFSEventStreamCreateFlagFileEvents
                                          | kFSEventStreamCreateFlagNoDefer
                                          | kFSEventStreamCreateFlagWatchRoot;
  };

  class fsevents_context;

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base()                        = default;
      virtual void deliver(fs_batch) noexcept     = 0;
    };

    template <class _Rcvr>
    struct __op;

    template <class _Rcvr>
    struct __next_receiver;

    struct __watch_sender;
  }  // namespace __detail

  class fsevents_context
  {
   public:
    explicit fsevents_context(std::vector<std::string> __paths)
      : __paths_{std::move(__paths)}
    {}

    ~fsevents_context() = default;

    fsevents_context(const fsevents_context&)                    = delete;
    auto operator=(const fsevents_context&) -> fsevents_context& = delete;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

    [[nodiscard]]
    auto last_completed_id() const noexcept -> FSEventStreamEventId
    {
      return __last_completed_id_.load(std::memory_order_acquire);
    }

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
    template <class _Rcvr>
    friend struct __detail::__next_receiver;
    friend struct __detail::__watch_sender;

    static void __callback(ConstFSEventStreamRef,
                           void* __ctx_ptr,
                           size_t __num_events,
                           void* __event_paths,
                           const FSEventStreamEventFlags* __flags,
                           const FSEventStreamEventId* __ids) noexcept;

    std::vector<std::string>           __paths_;
    std::atomic<__detail::__op_base*>  __active_{nullptr};
    std::atomic<FSEventStreamEventId>  __last_completed_id_{0};
  };

  namespace __detail
  {
    template <class _Rcvr>
    struct __next_receiver
    {
      using receiver_concept = stdexec::receiver_tag;

      __op<_Rcvr>* __self_;

      template <class... _Args>
      void set_value(_Args&&...) noexcept;

      void set_stopped() noexcept;

      template <class _E>
      void set_error(_E&&) noexcept;

      [[nodiscard]]
      auto get_env() const noexcept -> stdexec::env_of_t<_Rcvr>;
    };

    template <class _Rcvr>
    struct __op : __op_base
    {
      using __item_sender_t   = decltype(stdexec::just(std::declval<fs_batch>()));
      using __next_sender_t   = exec::next_sender_of_t<_Rcvr, __item_sender_t>;
      using __next_receiver_t = __next_receiver<_Rcvr>;
      using __next_op_t       = stdexec::connect_result_t<__next_sender_t, __next_receiver_t>;

      struct __on_stop_fn
      {
        __op* __self_;
        void  operator()() noexcept;
      };

      using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
      using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;

      fsevents_context*                 __ctx_;
      watch_options                     __opts_;
      _Rcvr                             __rcvr_;
      dispatch_queue_t                  __queue_{nullptr};
      FSEventStreamRef                  __stream_{nullptr};
      std::atomic<bool>                 __stop_requested_{false};
      std::binary_semaphore             __delivery_done_{0};
      int                               __delivery_state_{0};  // 1=value, 2=stopped, 3=error
      std::exception_ptr                __error_;
      FSEventStreamEventId              __batch_last_id_{};
      std::optional<__stop_callback_t>  __stop_cb_;
      std::unique_ptr<__next_op_t>      __next_op_;

      static auto __make_internal_queue(_Rcvr const & __r) -> dispatch_queue_t
      {
        auto __sch  = stdexec::get_scheduler(stdexec::get_env(__r));
        auto __attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL,
                                                              QOS_CLASS_UNSPECIFIED,
                                                              0);
        return dispatch_queue_create_with_target("fsx.fsevents", __attr, __sch.native_handle());
      }

      explicit __op(fsevents_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
        , __queue_{__make_internal_queue(__rcvr_)}
      { }

      ~__op()
      {
        if (__queue_)
          dispatch_release(__queue_);
      }

      void start() & noexcept
      {
        FSEventStreamEventId __initial = __opts_.since;
        if (__initial == kFSEventStreamEventIdSinceNow)
        {
          __initial = FSEventsGetCurrentEventId();
        }
        __ctx_->__last_completed_id_.store(__initial, std::memory_order_release);

        CFMutableArrayRef __cfpaths = CFArrayCreateMutable(
          kCFAllocatorDefault,
          static_cast<CFIndex>(__ctx_->__paths_.size()),
          &kCFTypeArrayCallBacks);
        for (const auto& __p : __ctx_->__paths_)
        {
          CFStringRef __s =
            CFStringCreateWithCString(kCFAllocatorDefault, __p.c_str(), kCFStringEncodingUTF8);
          CFArrayAppendValue(__cfpaths, __s);
          CFRelease(__s);
        }

        FSEventStreamContext __sctx{};
        __sctx.info = static_cast<__op_base*>(this);

        __stream_ = FSEventStreamCreate(kCFAllocatorDefault,
                                        &fsevents_context::__callback,
                                        &__sctx,
                                        __cfpaths,
                                        __opts_.since,
                                        __opts_.latency,
                                        __opts_.create_flags);

        CFRelease(__cfpaths);

        if (!__stream_)
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(
                               std::runtime_error{"FSEventStreamCreate failed"}));
          return;
        }

        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          FSEventStreamRelease(__stream_);
          __stream_ = nullptr;
          stdexec::set_error(
            static_cast<_Rcvr&&>(__rcvr_),
            std::make_exception_ptr(
              std::runtime_error{"fsevents_context already has an active watch"}));
          return;
        }

        FSEventStreamSetDispatchQueue(__stream_, __queue_);

        if (!FSEventStreamStart(__stream_))
        {
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          FSEventStreamInvalidate(__stream_);
          FSEventStreamRelease(__stream_);
          __stream_ = nullptr;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(
                               std::runtime_error{"FSEventStreamStart failed"}));
          return;
        }

        // Register stop callback last; if the token is already in stop state it
        // fires synchronously, which is now safe because the stream is fully up.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)),
                           __on_stop_fn{this});
      }

      // Called from the dispatch queue (from __callback).
      void deliver(fs_batch __batch) noexcept override
      {
        if (__stop_requested_.load(std::memory_order_acquire))
          return;

        __batch_last_id_  = __batch.last_id;
        __delivery_state_ = 0;

        try
        {
          __next_op_.reset(new __next_op_t(stdexec::connect(
            exec::set_next(__rcvr_, stdexec::just(__batch)),
            __next_receiver_t{this})));
          stdexec::start(*__next_op_);
        }
        catch (...)
        {
          __error_          = std::current_exception();
          __delivery_state_ = 3;
          __delivery_done_.release();
        }

        __delivery_done_.acquire();
        const int __state = __delivery_state_;
        __next_op_.reset();

        if (__state == 2)
        {
          __stop_requested_.store(true, std::memory_order_release);
          __schedule_finish_stopped();
        }
        else if (__state == 3)
        {
          __stop_requested_.store(true, std::memory_order_release);
          __schedule_finish_error(std::move(__error_));
        }
      }

      void __schedule_finish_stopped() noexcept
      {
        dispatch_async_f(__queue_, this, +[](void* __p) noexcept {
          auto* __o = static_cast<__op*>(__p);
          if (!__o->__stream_)
            return;
          __o->__teardown_stream();
          stdexec::set_stopped(static_cast<_Rcvr&&>(__o->__rcvr_));
        });
      }

      void __schedule_finish_error(std::exception_ptr __ep) noexcept
      {
        struct __closure
        {
          __op*              __o;
          std::exception_ptr __ep;
        };
        auto* __c = new __closure{this, std::move(__ep)};
        dispatch_async_f(__queue_, __c, +[](void* __p) noexcept {
          std::unique_ptr<__closure> __cu{static_cast<__closure*>(__p)};
          if (!__cu->__o->__stream_)
            return;
          __cu->__o->__teardown_stream();
          stdexec::set_error(static_cast<_Rcvr&&>(__cu->__o->__rcvr_), std::move(__cu->__ep));
        });
      }

      void __teardown_stream() noexcept
      {
        if (__stream_)
        {
          FSEventStreamStop(__stream_);
          FSEventStreamInvalidate(__stream_);
          FSEventStreamRelease(__stream_);
          __stream_ = nullptr;
        }
        __stop_cb_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);
      }
    };

    template <class _Rcvr>
    void __op<_Rcvr>::__on_stop_fn::operator()() noexcept
    {
      __self_->__stop_requested_.store(true, std::memory_order_release);
      // Cleanup must run on the dispatch queue to serialize with __callback.
      // If a delivery is currently blocked, downstream stop_token propagation
      // is responsible for completing the next-sender (with set_stopped),
      // which unblocks the callback so this enqueued work can run.
      dispatch_async_f(__self_->__queue_, __self_, +[](void* __p) noexcept {
        auto* __o = static_cast<__op*>(__p);
        if (!__o->__stream_)
          return;  // deliver() already finished us
        __o->__teardown_stream();
        stdexec::set_stopped(static_cast<_Rcvr&&>(__o->__rcvr_));
      });
    }

    template <class _Rcvr>
    template <class... _Args>
    void __next_receiver<_Rcvr>::set_value(_Args&&...) noexcept
    {
      __self_->__ctx_->__last_completed_id_.store(__self_->__batch_last_id_,
                                                  std::memory_order_release);
      __self_->__delivery_state_ = 1;
      __self_->__delivery_done_.release();
    }

    template <class _Rcvr>
    void __next_receiver<_Rcvr>::set_stopped() noexcept
    {
      __self_->__delivery_state_ = 2;
      __self_->__delivery_done_.release();
    }

    template <class _Rcvr>
    template <class _E>
    void __next_receiver<_Rcvr>::set_error(_E&& __e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<_E>, std::exception_ptr>)
      {
        __self_->__error_ = std::forward<_E>(__e);
      }
      else
      {
        __self_->__error_ = std::make_exception_ptr(std::forward<_E>(__e));
      }
      __self_->__delivery_state_ = 3;
      __self_->__delivery_done_.release();
    }

    template <class _Rcvr>
    auto __next_receiver<_Rcvr>::get_env() const noexcept -> stdexec::env_of_t<_Rcvr>
    {
      return stdexec::get_env(__self_->__rcvr_);
    }

    struct __watch_sender
    {
      using sender_concept        = exec::sequence_sender_tag;
      using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t(),
                                                                   stdexec::set_stopped_t(),
                                                                   stdexec::set_error_t(
                                                                     std::exception_ptr)>;

      using __item_sender_t = decltype(stdexec::just(std::declval<fs_batch>()));
      using item_types      = exec::item_types<__item_sender_t>;

      fsevents_context* __ctx_;
      watch_options     __opts_;

      template <stdexec::receiver _Rcvr>
        requires stdexec::__callable<stdexec::get_scheduler_t,
                                     stdexec::env_of_t<_Rcvr> const&>
              && std::same_as<
                   stdexec::__call_result_t<stdexec::get_scheduler_t,
                                            stdexec::env_of_t<_Rcvr> const&>,
                   exec::libdispatch_scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };

  }  // namespace __detail

  // env-injection adapter exposing a libdispatch_scheduler via
  // get_scheduler in the receiver env. See examples/on_scheduler.hpp
  // and examples/sequence_sender_on_scheduler.md.
  inline constexpr examples_detail::__on_scheduler_t on_queue{};

  inline auto fsevents_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }

  inline void fsevents_context::__callback(
    ConstFSEventStreamRef,
    void* __ctx_ptr,
    size_t __num_events,
    void* __event_paths,
    const FSEventStreamEventFlags* __flags,
    const FSEventStreamEventId* __ids) noexcept
  {
    auto*  __self  = static_cast<__detail::__op_base*>(__ctx_ptr);
    auto** __paths = static_cast<char**>(__event_paths);

    std::vector<fs_event> __staging;
    __staging.reserve(__num_events);
    bool                 __drops  = false;
    bool                 __rescan = false;
    FSEventStreamEventId __last   = 0;

    for (size_t __i = 0; __i < __num_events; ++__i)
    {
      __staging.push_back({std::string{__paths[__i]}, __flags[__i], __ids[__i]});
      __last = std::max(__last, __ids[__i]);
      __drops |= (__flags[__i] & kDropMask) != 0;
      __rescan |=
        (__flags[__i]
         & (kFSEventStreamEventFlagMustScanSubDirs | kFSEventStreamEventFlagRootChanged))
        != 0;
    }
    fs_batch __batch{__staging, __last, __drops, __rescan};
    __self->deliver(__batch);
  }
}  // namespace fsx
