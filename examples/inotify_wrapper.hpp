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

// Linux-only: wraps inotify as a stdexec sequence sender driven by
// exec::io_uring_context (IORING_OP_READ on the inotify fd).

#include <sys/inotify.h>
#include <unistd.h>

#include "exec/linux/io_uring_context.hpp"
#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace inx
{
  struct fs_event
  {
    int         wd;       // -1 means IN_Q_OVERFLOW (filtered from events span)
    std::uint32_t mask;
    std::uint32_t cookie;
    std::string name;
  };

  struct fs_batch
  {
    std::span<const fs_event> events;
    bool                      overflow;
  };

  struct watch_options
  {
    std::uint32_t mask = IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MODIFY
                       | IN_ATTRIB | IN_MOVED_FROM | IN_MOVED_TO | IN_MOVE_SELF
                       | IN_CLOSE_WRITE;
    std::size_t   buffer_size = 64 * 1024;
  };

  class inotify_context;

  namespace __detail
  {
    struct __op_base;
    template <class _Rcvr> struct __op;
    template <class _Rcvr> struct __next_receiver;
    struct __watch_sender;
  }

  class inotify_context
  {
   public:
    explicit inotify_context(std::vector<std::string> __initial_paths,
                             std::uint32_t __default_mask = watch_options{}.mask);
    ~inotify_context();

    inotify_context(const inotify_context&)                    = delete;
    auto operator=(const inotify_context&) -> inotify_context& = delete;

    auto add_watch(std::string_view __path,
                   std::optional<std::uint32_t> __mask = std::nullopt) -> int;

    auto remove_watch(int __wd) noexcept -> bool;

    auto path_for(int __wd) const -> std::optional<std::string>;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
    template <class _Rcvr>
    friend struct __detail::__next_receiver;
    friend struct __detail::__watch_sender;

    int                                       __fd_{-1};
    std::uint32_t                             __default_mask_;
    mutable std::mutex                        __map_mu_;
    std::unordered_map<int, std::string>      __wd_to_path_;
    std::atomic<__detail::__op_base*>         __active_{nullptr};
  };

  // env-injection adapter — mirrors fsx::on_queue / rdcx::pool::on_pool.
  inline constexpr exec::__on_scheduler_t on_ring{};

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base() = default;
      virtual void __on_read_complete(const ::io_uring_cqe&) noexcept = 0;
    };

    template <class _Rcvr>
    struct __op;

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

    // Thin __io_task base that defers to its outer __op via a back-pointer.
    struct __read_task
    {
      __op_base*                     __outer_;
      experimental::execution::__io_uring::__context*  __ctx_;
      int                            __fd_;
      void*                          __buf_;
      std::size_t                    __buf_len_;

      auto context() noexcept
        -> experimental::execution::__io_uring::__context&
      {
        return *__ctx_;
      }

      static constexpr auto ready() noexcept -> bool { return false; }

      void submit(::io_uring_sqe& __sqe) noexcept
      {
        std::memset(&__sqe, 0, sizeof(__sqe));
        __sqe.opcode = IORING_OP_READ;
        __sqe.fd     = __fd_;
        __sqe.addr   = reinterpret_cast<std::uint64_t>(__buf_);
        __sqe.len    = static_cast<std::uint32_t>(__buf_len_);
        __sqe.off    = 0;
        // user_data is set by __io_uring_context::submit().
      }

      void complete(const ::io_uring_cqe& __cqe) noexcept
      {
        __outer_->__on_read_complete(__cqe);
      }
    };

    using __read_op_t =
      experimental::execution::__io_uring::__io_task_facade<__read_task>;

    // Single-shot SQE that cancels another in-flight task by user_data.
    // The target task's user_data is its __task* (set by io_uring_context).
    struct __cancel_task
    {
      experimental::execution::__io_uring::__context* __ctx_;
      void*                                           __target_user_data_;

      auto context() noexcept
        -> experimental::execution::__io_uring::__context&
      {
        return *__ctx_;
      }

      static constexpr auto ready() noexcept -> bool { return false; }

      void submit(::io_uring_sqe& __sqe) noexcept
      {
        std::memset(&__sqe, 0, sizeof(__sqe));
        __sqe.opcode = IORING_OP_ASYNC_CANCEL;
        __sqe.addr   = reinterpret_cast<std::uint64_t>(__target_user_data_);
      }

      void complete(const ::io_uring_cqe&) noexcept
      {
        // Cancellation result is discarded — the target task's own complete()
        // path is what drives finish_stopped. -ENOENT (already done) and
        // 0 (cancelled) are both valid outcomes.
      }
    };

    using __cancel_op_t =
      experimental::execution::__io_uring::__io_task_facade<__cancel_task>;

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
        void  operator()() noexcept
        {
          __self_->__stop_requested_.store(true, std::memory_order_release);
          // If a read SQE is in flight, send a cancel SQE for it.
          // __read_op_.has_value() is the simplest gate: it's set across
          // the entire lifetime of an in-flight read, cleared only in
          // __teardown(). Submitting a cancel against an already-completed
          // read is harmless (kernel returns -ENOENT, complete() ignores it).
          if (__self_->__read_op_)
          {
            // user_data is the address of the __task base of the facade.
            void* __tgt = static_cast<
              experimental::execution::__io_uring::__task*>(&*__self_->__read_op_);
            __self_->__cancel_op_.emplace(std::in_place,
                                          __cancel_task{__self_->__ring_, __tgt});
            __self_->__cancel_op_->start();
          }
        }
      };

      using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
      using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;

      inotify_context*               __ctx_;
      watch_options                  __opts_;
      _Rcvr                          __rcvr_;
      experimental::execution::__io_uring::__context*  __ring_;
      // uint64_t (alignment 8) guarantees the >=4-byte alignment that
      // ::inotify_event requires for its int wd / uint32_t fields.
      // std::vector<std::byte>::data() is only aligned to alignof(byte)==1
      // by the standard. Same trick as rdc_pool_wrapper.hpp's vector<DWORD>.
      std::vector<std::uint64_t>     __buffer_;
      std::vector<fs_event>          __staging_;
      std::optional<__read_op_t>     __read_op_;
      std::optional<__cancel_op_t>   __cancel_op_;
      std::unique_ptr<__next_op_t>   __next_op_;
      std::optional<__stop_callback_t> __stop_cb_;
      std::atomic<bool>              __stop_requested_{false};
      std::exception_ptr             __error_;

      explicit __op(inotify_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
      {
        auto __sched = stdexec::get_scheduler(stdexec::get_env(__rcvr_));
        __ring_      = __sched.__context_;
        // Round buffer_size up to the next uint64_t.
        __buffer_.resize((__opts_.buffer_size + sizeof(std::uint64_t) - 1)
                         / sizeof(std::uint64_t));
      }

      void start() & noexcept
      {
        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{
                               "inotify_context already has an active watch"}));
          return;
        }
        __post_read();

        // Register stop callback last: if the token is already in stop state
        // it fires synchronously, which is now safe because the read is up.
        // Same pattern as fsevents_wrapper / rdc_wrapper.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)),
                           __on_stop_fn{this});
      }

      void __post_read() noexcept
      {
        // Each read posts a fresh __io_task_facade. The previous one (if any)
        // has already had its complete() returned. Destructing the previous
        // here is safe because we are not nested in its callback (we are
        // either in start() or in next_receiver::set_value).
        // Note: __io_task_facade has two ctor overloads; overload (B) takes
        // _Args... matching _Base's own ctor, overload (A) takes
        // (__task* parent, _Args...). __read_task is an aggregate with no
        // leading __task* field, so overload (B) wins and __base_ is
        // copy/move-constructed from our brace-init. If a future change
        // adds a leading __task* member to __read_task it would silently
        // flip overload selection — keep the leading member as __op_base*.
        __read_op_.emplace(std::in_place,
                           __read_task{
                             static_cast<__op_base*>(this),
                             __ring_,
                             __ctx_->__fd_,
                             __buffer_.data(),
                             __buffer_.size() * sizeof(std::uint64_t)});
        __read_op_->start();
      }

      // Called on the io_uring reactor thread.
      void __on_read_complete(const ::io_uring_cqe& __cqe) noexcept override
      {
        if (__cqe.res < 0)
        {
          if (__cqe.res == -ECANCELED || __stop_requested_.load(std::memory_order_acquire))
          {
            __finish_stopped();
            return;
          }
          __error_ = std::make_exception_ptr(std::system_error{
            -__cqe.res, std::system_category(), "inotify read"});
          __finish_error();
          return;
        }

        __parse_into_staging(static_cast<std::size_t>(__cqe.res));

        bool __overflow = false;
        // IN_Q_OVERFLOW arrives as a synthetic event with wd=-1; surface it
        // batch-level and remove it from the events span.
        std::erase_if(__staging_, [&](const fs_event& __e) {
          if (__e.wd == -1 && (__e.mask & IN_Q_OVERFLOW))
          {
            __overflow = true;
            return true;
          }
          return false;
        });

        fs_batch __batch{__staging_, __overflow};

        try
        {
          __next_op_.reset(new __next_op_t(stdexec::connect(
            exec::set_next(__rcvr_, stdexec::just(__batch)),
            __next_receiver_t{this})));
          stdexec::start(*__next_op_);
        }
        catch (...)
        {
          __error_ = std::current_exception();
          __finish_error();
        }
      }

      void __parse_into_staging(std::size_t __bytes) noexcept
      {
        __staging_.clear();
        const auto* __raw = reinterpret_cast<const std::byte*>(__buffer_.data());
        std::size_t __off = 0;
        while (__off + sizeof(::inotify_event) <= __bytes)
        {
          const auto* __ev = reinterpret_cast<const ::inotify_event*>(
            __raw + __off);
          std::size_t __record = sizeof(::inotify_event) + __ev->len;
          if (__off + __record > __bytes) break;

          // IN_IGNORED: the kernel has dropped this watch. Clean wd→path map.
          if ((__ev->mask & IN_IGNORED) && __ev->wd >= 0)
          {
            std::lock_guard __lk{__ctx_->__map_mu_};
            __ctx_->__wd_to_path_.erase(__ev->wd);
          }

          std::string __name;
          if (__ev->len > 0)
          {
            // name is NUL-padded; strlen gives the real size.
            __name.assign(__ev->name, ::strnlen(__ev->name, __ev->len));
          }
          __staging_.push_back({__ev->wd, __ev->mask, __ev->cookie,
                                std::move(__name)});

          __off += __record;
        }
      }

      void __on_next_value() noexcept
      {
        // Do NOT reset __next_op_ here — downstream may complete synchronously
        // inside set_next's start(); destroying the op from inside its own
        // set_value call would tear down the call stack. The next batch's
        // unique_ptr::reset(new ...) will destroy this child after start()
        // unwinds.
        if (__stop_requested_.load(std::memory_order_acquire))
        {
          __finish_stopped();
          return;
        }
        __post_read();
      }

      void __on_next_stopped() noexcept { __finish_stopped(); }

      void __on_next_error(std::exception_ptr __ep) noexcept
      {
        __error_ = std::move(__ep);
        __finish_error();
      }

      void __teardown() noexcept
      {
        // Drop the stop callback first so any in-flight invocation finishes
        // before we destroy state it might touch (cancel SQE submission,
        // __read_op_ pointer access). Same ordering as fsevents/rdc.
        //
        // After this point we are no longer reachable via the stop callback.
        // We then destroy the next-op (in-flight downstream pipe), the
        // read facade (now done — its CQE is what brought us here), and
        // the cancel facade (also done if it was ever submitted). Finally
        // release the active slot so a fresh subscribe can succeed.
        __stop_cb_.reset();
        __next_op_.reset();
        __cancel_op_.reset();
        __read_op_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);
      }

      void __finish_stopped() noexcept
      {
        __teardown();
        stdexec::set_stopped(static_cast<_Rcvr&&>(__rcvr_));
      }

      void __finish_error() noexcept
      {
        __teardown();
        stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), std::move(__error_));
      }
    };

    template <class _Rcvr>
    template <class... _Args>
    void __next_receiver<_Rcvr>::set_value(_Args&&...) noexcept
    {
      __self_->__on_next_value();
    }

    template <class _Rcvr>
    void __next_receiver<_Rcvr>::set_stopped() noexcept
    {
      __self_->__on_next_stopped();
    }

    template <class _Rcvr>
    template <class _E>
    void __next_receiver<_Rcvr>::set_error(_E&& __e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<_E>, std::exception_ptr>)
      {
        __self_->__on_next_error(std::forward<_E>(__e));
      }
      else
      {
        __self_->__on_next_error(std::make_exception_ptr(std::forward<_E>(__e)));
      }
    }

    template <class _Rcvr>
    auto __next_receiver<_Rcvr>::get_env() const noexcept -> stdexec::env_of_t<_Rcvr>
    {
      return stdexec::get_env(__self_->__rcvr_);
    }

    struct __watch_sender
    {
      using sender_concept = exec::sequence_sender_tag;
      using completion_signatures = stdexec::completion_signatures<
        stdexec::set_value_t(),
        stdexec::set_stopped_t(),
        stdexec::set_error_t(std::exception_ptr)>;

      using __item_sender_t = decltype(stdexec::just(std::declval<fs_batch>()));
      using item_types      = exec::item_types<__item_sender_t>;

      inotify_context* __ctx_;
      watch_options    __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                           exec::io_uring_scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };
  }

  // ---------- inotify_context impl ----------

  inline inotify_context::inotify_context(std::vector<std::string> __initial_paths,
                                          std::uint32_t __default_mask)
    : __default_mask_{__default_mask}
  {
    __fd_ = ::inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
    if (__fd_ < 0)
    {
      throw std::system_error{errno, std::system_category(), "inotify_init1"};
    }
    try
    {
      for (const auto& __p : __initial_paths)
      {
        add_watch(__p);
      }
    }
    catch (...)
    {
      ::close(__fd_);
      __fd_ = -1;
      throw;
    }
  }

  inline inotify_context::~inotify_context()
  {
    if (__fd_ >= 0)
    {
      ::close(__fd_);
    }
  }

  inline auto inotify_context::add_watch(std::string_view __path,
                                         std::optional<std::uint32_t> __mask) -> int
  {
    std::string __zpath{__path};  // inotify_add_watch needs NUL-terminated
    int __wd = ::inotify_add_watch(__fd_, __zpath.c_str(),
                                   __mask.value_or(__default_mask_));
    if (__wd < 0)
    {
      throw std::system_error{errno, std::system_category(), "inotify_add_watch"};
    }
    {
      std::lock_guard __lk{__map_mu_};
      __wd_to_path_[__wd] = std::move(__zpath);
    }
    return __wd;
  }

  inline auto inotify_context::remove_watch(int __wd) noexcept -> bool
  {
    // First confirm we know about this wd. If not, nothing to remove.
    {
      std::lock_guard __lk{__map_mu_};
      if (!__wd_to_path_.contains(__wd))
      {
        return false;
      }
    }
    // Call the kernel before erasing the map entry so path_for() reflects
    // the live watch set: a concurrent path_for(wd) during this window
    // returns the path (still true from the kernel's POV until rm_watch
    // succeeds). EINVAL means the kernel has already auto-removed the wd
    // (e.g. file unlinked) — treat that as success and clean up the stale
    // map entry. The IN_IGNORED that follows hits an empty map slot
    // (handler is a no-op then).
    int __rc = ::inotify_rm_watch(__fd_, __wd);
    if (__rc != 0 && errno != EINVAL)
    {
      return false;
    }
    {
      std::lock_guard __lk{__map_mu_};
      __wd_to_path_.erase(__wd);
    }
    return true;
  }

  inline auto inotify_context::path_for(int __wd) const -> std::optional<std::string>
  {
    std::lock_guard __lk{__map_mu_};
    auto __it = __wd_to_path_.find(__wd);
    if (__it == __wd_to_path_.end())
    {
      return std::nullopt;
    }
    return __it->second;
  }

  inline auto inotify_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace inx
