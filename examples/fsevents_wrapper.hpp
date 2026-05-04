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
#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
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
    std::span<fs_event const> events;
    FSEventStreamEventId      last_id;
    bool                      had_drops;
    bool                      must_rescan;
  };

  inline constexpr FSEventStreamEventFlags kDropMask = kFSEventStreamEventFlagMustScanSubDirs
                                                     | kFSEventStreamEventFlagUserDropped
                                                     | kFSEventStreamEventFlagKernelDropped
                                                     | kFSEventStreamEventFlagRootChanged;

  inline auto is_drop_notice(fs_event const & e) noexcept -> bool
  {
    return (e.flags & kDropMask) != 0;
  }

  struct watch_options
  {
    FSEventStreamEventId     since        = kFSEventStreamEventIdSinceNow;
    CFAbsoluteTime           latency      = 0.2;
    FSEventStreamCreateFlags create_flags = kFSEventStreamCreateFlagFileEvents
                                          | kFSEventStreamCreateFlagNoDefer
                                          | kFSEventStreamCreateFlagWatchRoot;
  };

  class fsevents_context;

  namespace detail
  {
    struct op_base
    {
      virtual ~op_base()                    = default;
      virtual void deliver(fs_batch) noexcept = 0;

      // Reused across FSEvents callbacks (the dispatch queue is serial, so
      // only one callback at a time per op). Hoisted out of the callback
      // to avoid per-batch allocation churn.
      std::vector<fs_event> staging_;
    };

    template <class Rcvr>
    struct op;

    template <class Rcvr>
    struct next_receiver;

    struct watch_sender;
  }  // namespace detail

  class fsevents_context
  {
   public:
    explicit fsevents_context(std::vector<std::string> paths)
      : paths_{std::move(paths)}
    {}

    ~fsevents_context() = default;

    fsevents_context(fsevents_context const &)                    = delete;
    auto operator=(fsevents_context const &) -> fsevents_context& = delete;

    auto watch(watch_options opts = {}) -> detail::watch_sender;

    [[nodiscard]]
    auto last_completed_id() const noexcept -> FSEventStreamEventId
    {
      return last_completed_id_.load(std::memory_order_acquire);
    }

   private:
    template <class Rcvr>
    friend struct detail::op;
    template <class Rcvr>
    friend struct detail::next_receiver;
    friend struct detail::watch_sender;

    static void callback(ConstFSEventStreamRef,
                           void*                           ctx_ptr,
                           size_t                          num_events,
                           void*                           event_paths,
                           FSEventStreamEventFlags const * flags,
                           FSEventStreamEventId const *    ids) noexcept;

    std::vector<std::string>          paths_;
    std::atomic<detail::op_base*> active_{nullptr};
    std::atomic<FSEventStreamEventId> last_completed_id_{0};
  };

  namespace detail
  {
    template <class Rcvr>
    struct next_receiver
    {
      using receiver_concept = stdexec::receiver_tag;

      op<Rcvr>* self_;

      template <class... Args>
      void set_value(Args&&...) noexcept;

      void set_stopped() noexcept;

      template <class E>
      void set_error(E&&) noexcept;

      [[nodiscard]]
      auto get_env() const noexcept -> stdexec::env_of_t<Rcvr>;
    };

    template <class Rcvr>
    struct op : op_base
    {
      using item_sender_t   = decltype(stdexec::just(std::declval<fs_batch>()));
      using next_sender_t   = exec::next_sender_of_t<Rcvr, item_sender_t>;
      using next_receiver_t = next_receiver<Rcvr>;
      using next_op_t       = stdexec::connect_result_t<next_sender_t, next_receiver_t>;

      struct on_stop_fn
      {
        op* self_;
        void  operator()() noexcept;
      };

      using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
      using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;

      fsevents_context*                ctx_;
      watch_options                    opts_;
      Rcvr                            rcvr_;
      dispatch_queue_t                 queue_{nullptr};
      FSEventStreamRef                 stream_{nullptr};
      std::atomic<bool>                stop_requested_{false};
      std::binary_semaphore            delivery_done_{0};
      int                              delivery_state_{0};  // 1=value, 2=stopped, 3=error
      std::exception_ptr               error_;
      FSEventStreamEventId             batch_last_id_{};
      std::optional<stop_callback_t> stop_cb_;
      std::unique_ptr<next_op_t>     next_op_;

      static auto make_internal_queue(Rcvr const & r) -> dispatch_queue_t
      {
        auto sch  = stdexec::get_scheduler(stdexec::get_env(r));
        auto attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL,
                                                              QOS_CLASS_UNSPECIFIED,
                                                              0);
        return dispatch_queue_create_with_target("fsx.fsevents", attr, sch.native_handle());
      }

      explicit op(fsevents_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{o}
        , rcvr_{std::move(r)}
        , queue_{make_internal_queue(rcvr_)}
      {}

      ~op()
      {
        if (queue_)
          dispatch_release(queue_);
      }

      void start() & noexcept
      {
        FSEventStreamEventId initial = opts_.since;
        if (initial == kFSEventStreamEventIdSinceNow)
        {
          initial = FSEventsGetCurrentEventId();
        }
        ctx_->last_completed_id_.store(initial, std::memory_order_release);

        CFMutableArrayRef cfpaths = CFArrayCreateMutable(kCFAllocatorDefault,
                                                           static_cast<CFIndex>(
                                                             ctx_->paths_.size()),
                                                           &kCFTypeArrayCallBacks);
        for (auto const & p: ctx_->paths_)
        {
          CFStringRef s = CFStringCreateWithCString(kCFAllocatorDefault,
                                                      p.c_str(),
                                                      kCFStringEncodingUTF8);
          CFArrayAppendValue(cfpaths, s);
          CFRelease(s);
        }

        FSEventStreamContext sctx{};
        sctx.info = static_cast<op_base*>(this);

        stream_ = FSEventStreamCreate(kCFAllocatorDefault,
                                        &fsevents_context::callback,
                                        &sctx,
                                        cfpaths,
                                        opts_.since,
                                        opts_.latency,
                                        opts_.create_flags);

        CFRelease(cfpaths);

        if (!stream_)
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"FSEventStreamCreate "
                                                                        "failed"}));
          return;
        }

        op_base* expected = nullptr;
        if (!ctx_->active_.compare_exchange_strong(expected, this))
        {
          FSEventStreamRelease(stream_);
          stream_ = nullptr;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"fsevents_context already "
                                                                        "has an active watch"}));
          return;
        }

        FSEventStreamSetDispatchQueue(stream_, queue_);

        if (!FSEventStreamStart(stream_))
        {
          ctx_->active_.store(nullptr, std::memory_order_release);
          FSEventStreamInvalidate(stream_);
          FSEventStreamRelease(stream_);
          stream_ = nullptr;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"FSEventStreamStart "
                                                                        "failed"}));
          return;
        }

        // Register stop callback last; if the token is already in stop state it
        // fires synchronously, which is now safe because the stream is fully up.
        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});
      }

      // Called from the dispatch queue (from callback).
      void deliver(fs_batch batch) noexcept override
      {
        if (stop_requested_.load(std::memory_order_acquire))
          return;

        batch_last_id_  = batch.last_id;
        delivery_state_ = 0;

        try
        {
          next_op_.reset(
            new next_op_t(stdexec::connect(exec::set_next(rcvr_, stdexec::just(batch)),
                                             next_receiver_t{this})));
          stdexec::start(*next_op_);
        }
        catch (...)
        {
          error_          = std::current_exception();
          delivery_state_ = 3;
          delivery_done_.release();
        }

        delivery_done_.acquire();
        int const state = delivery_state_;
        next_op_.reset();

        if (state == 2)
        {
          stop_requested_.store(true, std::memory_order_release);
          schedule_finish_stopped();
        }
        else if (state == 3)
        {
          stop_requested_.store(true, std::memory_order_release);
          schedule_finish_error(std::move(error_));
        }
      }

      void schedule_finish_stopped() noexcept
      {
        dispatch_async_f(
          queue_,
          this,
          +[](void* p) noexcept
          {
            auto* o = static_cast<op*>(p);
            if (!o->stream_)
              return;
            o->teardown_stream();
            stdexec::set_stopped(static_cast<Rcvr&&>(o->rcvr_));
          });
      }

      void schedule_finish_error(std::exception_ptr ep) noexcept
      {
        struct closure
        {
          op*              o;
          std::exception_ptr ep;
        };
        auto* c = new closure{this, std::move(ep)};
        dispatch_async_f(
          queue_,
          c,
          +[](void* p) noexcept
          {
            std::unique_ptr<closure> cu{static_cast<closure*>(p)};
            if (!cu->o->stream_)
              return;
            cu->o->teardown_stream();
            stdexec::set_error(static_cast<Rcvr&&>(cu->o->rcvr_), std::move(cu->ep));
          });
      }

      void teardown_stream() noexcept
      {
        if (stream_)
        {
          FSEventStreamStop(stream_);
          FSEventStreamInvalidate(stream_);
          FSEventStreamRelease(stream_);
          stream_ = nullptr;
        }
        stop_cb_.reset();
        ctx_->active_.store(nullptr, std::memory_order_release);
      }
    };

    template <class Rcvr>
    void op<Rcvr>::on_stop_fn::operator()() noexcept
    {
      self_->stop_requested_.store(true, std::memory_order_release);
      // Cleanup must run on the dispatch queue to serialize with callback.
      // If a delivery is currently blocked, downstream stop_token propagation
      // is responsible for completing the next-sender (with set_stopped),
      // which unblocks the callback so this enqueued work can run.
      dispatch_async_f(
        self_->queue_,
        self_,
        +[](void* p) noexcept
        {
          auto* o = static_cast<op*>(p);
          if (!o->stream_)
            return;  // deliver() already finished us
          o->teardown_stream();
          stdexec::set_stopped(static_cast<Rcvr&&>(o->rcvr_));
        });
    }

    template <class Rcvr>
    template <class... Args>
    void next_receiver<Rcvr>::set_value(Args&&...) noexcept
    {
      self_->ctx_->last_completed_id_.store(self_->batch_last_id_,
                                                  std::memory_order_release);
      self_->delivery_state_ = 1;
      self_->delivery_done_.release();
    }

    template <class Rcvr>
    void next_receiver<Rcvr>::set_stopped() noexcept
    {
      self_->delivery_state_ = 2;
      self_->delivery_done_.release();
    }

    template <class Rcvr>
    template <class E>
    void next_receiver<Rcvr>::set_error(E&& e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<E>, std::exception_ptr>)
      {
        self_->error_ = std::forward<E>(e);
      }
      else
      {
        self_->error_ = std::make_exception_ptr(std::forward<E>(e));
      }
      self_->delivery_state_ = 3;
      self_->delivery_done_.release();
    }

    template <class Rcvr>
    auto next_receiver<Rcvr>::get_env() const noexcept -> stdexec::env_of_t<Rcvr>
    {
      return stdexec::get_env(self_->rcvr_);
    }

    struct watch_sender
    {
      using sender_concept = exec::sequence_sender_tag;
      using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(),
                                       stdexec::set_stopped_t(),
                                       stdexec::set_error_t(std::exception_ptr)>;

      using item_sender_t = decltype(stdexec::just(std::declval<fs_batch>()));
      using item_types      = exec::item_types<item_sender_t>;

      fsevents_context* ctx_;
      watch_options     opts_;

      template <stdexec::receiver Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>, exec::libdispatch_scheduler>
      auto subscribe(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ctx_, opts_, std::move(rcvr)};
      }
    };

  }  // namespace detail

  inline auto fsevents_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, opts};
  }

  inline void fsevents_context::callback(ConstFSEventStreamRef,
                                           void*                           ctx_ptr,
                                           size_t                          num_events,
                                           void*                           event_paths,
                                           FSEventStreamEventFlags const * flags,
                                           FSEventStreamEventId const *    ids) noexcept
  {
    auto*  self  = static_cast<detail::op_base*>(ctx_ptr);
    auto** paths = static_cast<char**>(event_paths);

    auto& staging = self->staging_;
    staging.clear();
    staging.reserve(num_events);
    bool                 drops  = false;
    bool                 rescan = false;
    FSEventStreamEventId last   = 0;

    for (size_t i = 0; i < num_events; ++i)
    {
      staging.push_back({std::string{paths[i]}, flags[i], ids[i]});
      last = std::max(last, ids[i]);
      drops |= (flags[i] & kDropMask) != 0;
      rescan |= (flags[i]
                   & (kFSEventStreamEventFlagMustScanSubDirs | kFSEventStreamEventFlagRootChanged))
               != 0;
    }
    fs_batch batch{staging, last, drops, rescan};
    self->deliver(batch);
  }
}  // namespace fsx
