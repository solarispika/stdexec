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

// Windows-only: same shape as rdc_wrapper.hpp but the IO completions are
// driven by the Win32 thread pool (CreateThreadpoolIo) instead of a
// dedicated worker thread. No thread is owned per watch; pool worker
// threads are shared across all PTP_IO consumers in the process.
//
// Backpressure is now continuation-style: the next ReadDirectoryChangesW
// is posted from next_receiver::set_value rather than after a semaphore
// acquire. The IO callback never blocks; if it ran on a pool thread,
// it returns the thread to the pool as soon as set_next is dispatched.
//
// Caveat: if downstream does not propagate stop_token, the pipeline can
// stall (no callback, no progress). Same hazard as the dedicated-thread
// wrapper, just expressed differently — there the worker holds the
// semaphore; here the pool stops being scheduled altogether.

#ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#  define NOMINMAX
#endif
#include <windows.h>
// windows.h must come first
#include <threadpoolapiset.h>

#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/windows/windows_thread_pool.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace rdcx::pool
{
  struct fs_event
  {
    std::wstring path;
    DWORD        action;
  };

  struct fs_batch
  {
    std::span<fs_event const> events;
    bool                      overflow;
  };

  struct watch_options
  {
    bool  watch_subtree = true;
    DWORD filter        = FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME
                 | FILE_NOTIFY_CHANGE_ATTRIBUTES | FILE_NOTIFY_CHANGE_SIZE
                 | FILE_NOTIFY_CHANGE_LAST_WRITE | FILE_NOTIFY_CHANGE_CREATION;
    DWORD buffer_size = 64 * 1024;
  };

  class rdc_context;

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base() = default;
    };

    template <class _Rcvr>
    struct __op;

    template <class _Rcvr>
    struct __next_receiver;

    struct __watch_sender;

    enum __finish_kind : int
    {
      __finish_none    = 0,
      __finish_stopped = 1,
      __finish_error   = 2,
    };
  }  // namespace __detail

  class rdc_context
  {
   public:
    explicit rdc_context(std::wstring __path)
      : __path_{std::move(__path)}
    {}

    rdc_context(rdc_context const &)                    = delete;
    auto operator=(rdc_context const &) -> rdc_context& = delete;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
    template <class _Rcvr>
    friend struct __detail::__next_receiver;
    friend struct __detail::__watch_sender;

    std::wstring                      __path_;
    std::atomic<__detail::__op_base*> __active_{nullptr};
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

      rdc_context*                     __ctx_;
      watch_options                    __opts_;
      _Rcvr                            __rcvr_;
      TP_CALLBACK_ENVIRON              __env_;
      HANDLE                           __dir_{INVALID_HANDLE_VALUE};
      PTP_IO                           __io_{nullptr};
      PTP_WORK                         __cleanup_work_{nullptr};
      OVERLAPPED                       __ovl_{};
      std::vector<DWORD>               __buffer_;
      std::vector<fs_event>            __staging_;
      std::atomic<bool>                __stop_requested_{false};
      std::atomic<bool>                __cleanup_scheduled_{false};
      __finish_kind                    __finish_kind_{__finish_none};
      std::exception_ptr               __error_;
      std::optional<__stop_callback_t> __stop_cb_;
      std::unique_ptr<__next_op_t>     __next_op_;

      explicit __op(rdc_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
      {
        // nullptr from native_handle() is the documented sentinel for
        // "process default pool" that SetThreadpoolCallbackPool accepts.
        InitializeThreadpoolEnvironment(&__env_);
        auto __sch = stdexec::get_scheduler(stdexec::get_env(__rcvr_));
        SetThreadpoolCallbackPool(&__env_, __sch.native_handle());
      }

      ~__op() override
      {
        if (__io_)
        {
          // Safe even if a callback is in flight; the pool defers the
          // free until callbacks return. We only reach the destructor
          // after the receiver has been completed, which only happens
          // after the cleanup work has waited for all IO callbacks.
          CloseThreadpoolIo(__io_);
        }
        if (__cleanup_work_)
        {
          CloseThreadpoolWork(__cleanup_work_);
        }
        if (__dir_ != INVALID_HANDLE_VALUE)
        {
          CloseHandle(__dir_);
        }
        DestroyThreadpoolEnvironment(&__env_);
      }

      void start() & noexcept
      {
        __dir_ = CreateFileW(__ctx_->__path_.c_str(),
                             FILE_LIST_DIRECTORY,
                             FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                             nullptr,
                             OPEN_EXISTING,
                             FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
                             nullptr);
        if (__dir_ == INVALID_HANDLE_VALUE)
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(
                               std::system_error{static_cast<int>(GetLastError()),
                                                 std::system_category(),
                                                 "CreateFileW"}));
          return;
        }

        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"rdc_context already has "
                                                                        "an active watch"}));
          return;
        }

        __io_ = CreateThreadpoolIo(__dir_, &__io_callback, this, &__env_);
        if (!__io_)
        {
          DWORD __e = GetLastError();
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(__e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolIo"}));
          return;
        }

        __cleanup_work_ = CreateThreadpoolWork(&__cleanup_callback, this, &__env_);
        if (!__cleanup_work_)
        {
          DWORD __e = GetLastError();
          CloseThreadpoolIo(__io_);
          __io_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(__e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork"}));
          return;
        }

        std::size_t const __dwords = (__opts_.buffer_size + sizeof(DWORD) - 1) / sizeof(DWORD);
        try
        {
          __buffer_.resize(__dwords);
        }
        catch (...)
        {
          CloseThreadpoolWork(__cleanup_work_);
          __cleanup_work_ = nullptr;
          CloseThreadpoolIo(__io_);
          __io_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), std::current_exception());
          return;
        }

        // Register the stop callback before posting the first read so that
        // a stop request that races with start() can land on a valid handle.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)), __on_stop_fn{this});

        __post_read();
      }

      // Issue one ReadDirectoryChangesW. Either fires __io_callback later
      // (success / async pending) or schedules cleanup on synchronous error.
      void __post_read() noexcept
      {
        if (__stop_requested_.load(std::memory_order_acquire))
        {
          __schedule_cleanup(__finish_stopped);
          return;
        }

        __ovl_                     = OVERLAPPED{};
        const DWORD __byte_size    = static_cast<DWORD>(__buffer_.size() * sizeof(DWORD));
        DWORD       __unused_bytes = 0;

        StartThreadpoolIo(__io_);
        BOOL __ok = ReadDirectoryChangesW(__dir_,
                                          __buffer_.data(),
                                          __byte_size,
                                          __opts_.watch_subtree ? TRUE : FALSE,
                                          __opts_.filter,
                                          &__unused_bytes,
                                          &__ovl_,
                                          nullptr);
        if (!__ok)
        {
          DWORD __err = GetLastError();
          if (__err != ERROR_IO_PENDING)
          {
            // Synchronous failure: the IO callback will not be invoked, so
            // we must withdraw the StartThreadpoolIo expectation ourselves.
            CancelThreadpoolIo(__io_);
            if (__stop_requested_.load(std::memory_order_acquire))
            {
              __schedule_cleanup(__finish_stopped);
            }
            else
            {
              __error_ = std::make_exception_ptr(std::system_error{static_cast<int>(__err),
                                                                   std::system_category(),
                                                                   "ReadDirectoryChangesW"});
              __schedule_cleanup(__finish_error);
            }
            return;
          }
        }

        // Re-check stop in case it raced ahead of the post; the io_callback
        // will then receive ERROR_OPERATION_ABORTED.
        if (__stop_requested_.load(std::memory_order_acquire))
        {
          CancelIoEx(__dir_, &__ovl_);
        }
      }

      // ---------- IO completion path (runs on a pool worker thread) -------

      static void CALLBACK __io_callback(PTP_CALLBACK_INSTANCE,
                                         void*     __ctx_ptr,
                                         void*     __ovl_ptr,
                                         ULONG     __io_result,
                                         ULONG_PTR __bytes,
                                         PTP_IO) noexcept
      {
        auto* __self = static_cast<__op*>(__ctx_ptr);
        (void) __ovl_ptr;

        if (__io_result == ERROR_OPERATION_ABORTED
            || __self->__stop_requested_.load(std::memory_order_acquire))
        {
          __self->__schedule_cleanup(__finish_stopped);
          return;
        }
        if (__io_result != NO_ERROR)
        {
          __self->__error_ = std::make_exception_ptr(
            std::system_error{static_cast<int>(__io_result),
                              std::system_category(),
                              "ReadDirectoryChangesW (completion)"});
          __self->__schedule_cleanup(__finish_error);
          return;
        }

        bool const __overflow = (__bytes == 0);
        __self->__staging_.clear();
        if (!__overflow)
        {
          auto const * __raw = reinterpret_cast<std::byte const *>(__self->__buffer_.data());
          std::size_t  __off = 0;
          while (__off < __bytes)
          {
            auto const * __fni = reinterpret_cast<const FILE_NOTIFY_INFORMATION*>(__raw + __off);
            std::size_t const __chars = __fni->FileNameLength / sizeof(WCHAR);
            __self->__staging_.push_back({
              std::wstring{__fni->FileName, __chars},
              __fni->Action
            });
            if (__fni->NextEntryOffset == 0)
              break;
            __off += __fni->NextEntryOffset;
          }
        }

        fs_batch __batch{__self->__staging_, __overflow};

        try
        {
          __self->__next_op_.reset(new __next_op_t(
            stdexec::connect(exec::set_next(__self->__rcvr_, stdexec::just(__batch)),
                             __next_receiver_t{__self})));
          stdexec::start(*__self->__next_op_);
        }
        catch (...)
        {
          __self->__error_ = std::current_exception();
          __self->__schedule_cleanup(__finish_error);
        }
      }

      // ---------- next-sender continuation path ---------------------------

      void __on_next_value() noexcept
      {
        // We do NOT reset __next_op_ here because a synchronous downstream
        // completes inside stdexec::start(*__next_op_); destroying it from
        // within set_value() would tear down the call stack. The next
        // __post_read -> __io_callback -> deliver path overwrites it via
        // unique_ptr::reset(new ...), which destroys the previous op only
        // after start() has returned all the way back up.
        if (__stop_requested_.load(std::memory_order_acquire))
        {
          __schedule_cleanup(__finish_stopped);
          return;
        }
        __post_read();
      }

      void __on_next_stopped() noexcept
      {
        __schedule_cleanup(__finish_stopped);
      }

      void __on_next_error(std::exception_ptr __ep) noexcept
      {
        __error_ = std::move(__ep);
        __schedule_cleanup(__finish_error);
      }

      // ---------- cleanup path --------------------------------------------

      void __schedule_cleanup(__finish_kind __kind) noexcept
      {
        bool __expected = false;
        if (!__cleanup_scheduled_.compare_exchange_strong(__expected, true))
          return;
        __finish_kind_ = __kind;
        SubmitThreadpoolWork(__cleanup_work_);
      }

      static void CALLBACK __cleanup_callback(PTP_CALLBACK_INSTANCE,
                                              void* __ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* __self = static_cast<__op*>(__ctx_ptr);
        __self->__teardown_and_complete();
      }

      void __teardown_and_complete() noexcept
      {
        // Drop the stop callback first so an in-flight invocation finishes
        // before we touch the directory handle below.
        __stop_cb_.reset();

        // Cancel any pending Read so its io_callback fires (with
        // ERROR_OPERATION_ABORTED) and gets drained by the wait below.
        if (__dir_ != INVALID_HANDLE_VALUE)
        {
          CancelIoEx(__dir_, nullptr);
        }
        // Wait for any in-flight io_callback to return. Safe here because
        // we are running in a *work* callback, not an IO callback; calling
        // WaitForThreadpoolIoCallbacks from inside an IO callback would
        // deadlock.
        if (__io_)
        {
          WaitForThreadpoolIoCallbacks(__io_, /*fCancelPendingCallbacks*/ TRUE);
        }

        // Drop the previous-batch's child op state explicitly. By the time
        // we run, no IO callback can be issuing a fresh next_op, and we are
        // not nested inside its start(), so destruction here is safe.
        __next_op_.reset();

        __ctx_->__active_.store(nullptr, std::memory_order_release);

        // Move receiver and error out before completing — the parent op
        // may destroy *this* synchronously inside the completion call.
        auto       __local_rcvr = static_cast<_Rcvr&&>(__rcvr_);
        auto       __ep         = std::move(__error_);
        auto const __kind       = __finish_kind_;

        if (__kind == __finish_error)
        {
          stdexec::set_error(std::move(__local_rcvr), std::move(__ep));
        }
        else
        {
          stdexec::set_stopped(std::move(__local_rcvr));
        }
      }
    };

    template <class _Rcvr>
    void __op<_Rcvr>::__on_stop_fn::operator()() noexcept
    {
      __self_->__stop_requested_.store(true, std::memory_order_release);
      // CancelIoEx returns ERROR_NOT_FOUND if no Read is pending; that's
      // fine — the next __post_read re-checks the flag and cancels itself.
      if (__self_->__dir_ != INVALID_HANDLE_VALUE)
      {
        CancelIoEx(__self_->__dir_, &__self_->__ovl_);
      }
      // Don't schedule cleanup here. The flag + CancelIoEx route the stop
      // through the io_callback / next_receiver paths so cleanup runs on a
      // pool thread, not on whichever thread requested the stop.
    }

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
      using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(),
                                       stdexec::set_stopped_t(),
                                       stdexec::set_error_t(std::exception_ptr)>;

      using __item_sender_t = decltype(stdexec::just(std::declval<fs_batch>()));
      using item_types      = exec::item_types<__item_sender_t>;

      rdc_context*  __ctx_;
      watch_options __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                           exec::windows_thread_pool::scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };

  }  // namespace __detail

  inline auto rdc_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }

  // See examples/sequence_sender_on_scheduler.md.
  inline constexpr exec::__on_scheduler_t on_pool{};
}  // namespace rdcx::pool
