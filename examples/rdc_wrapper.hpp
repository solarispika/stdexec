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

// Windows-only: wraps ReadDirectoryChangesW as a stdexec sequence sender.

#ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#  define NOMINMAX
#endif
#include <windows.h>

#include "exec/sequence_senders.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <semaphore>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace rdcx
{
  struct fs_event
  {
    std::wstring path;  // path relative to the watched root
    DWORD action;  // FILE_ACTION_ADDED / REMOVED / MODIFIED / RENAMED_OLD_NAME / RENAMED_NEW_NAME
  };

  // A batch corresponds to one ReadDirectoryChangesW completion.
  // `overflow == true` means the kernel ring buffer outpaced the user buffer
  // (analogous to FSEvents' MustScanSubDirs): all events for this completion
  // were dropped and the caller must rescan the tree manually.
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
    // Buffer is rounded up to a multiple of sizeof(DWORD) for alignment.
    // Larger buffers reduce the chance of `overflow` under bursty load.
    DWORD buffer_size = 64 * 1024;
  };

  class rdc_context;

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base()                    = default;
      virtual void deliver(fs_batch) noexcept = 0;
    };

    template <class _Rcvr>
    struct __op;

    template <class _Rcvr>
    struct __next_receiver;

    struct __watch_sender;
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
      HANDLE                           __dir_{INVALID_HANDLE_VALUE};
      HANDLE                           __ovl_event_{nullptr};
      OVERLAPPED                       __ovl_{};
      std::vector<DWORD>               __buffer_;  // DWORD-aligned storage
      std::vector<fs_event>            __staging_;
      std::thread                      __thread_;
      std::atomic<bool>                __stop_requested_{false};
      std::binary_semaphore            __delivery_done_{0};
      int                              __delivery_state_{0};  // 1=value, 2=stopped, 3=error
      std::exception_ptr               __error_;
      std::optional<__stop_callback_t> __stop_cb_;
      std::unique_ptr<__next_op_t>     __next_op_;

      explicit __op(rdc_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
      {}

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

        __ovl_event_ = CreateEventW(nullptr,
                                    /*bManualReset*/ FALSE,
                                    /*bInitialState*/ FALSE,
                                    nullptr);
        if (!__ovl_event_)
        {
          DWORD __e = GetLastError();
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(__e),
                                                                       std::system_category(),
                                                                       "CreateEventW"}));
          return;
        }

        std::size_t const __dwords = (__opts_.buffer_size + sizeof(DWORD) - 1) / sizeof(DWORD);
        try
        {
          __buffer_.resize(__dwords);
        }
        catch (...)
        {
          CloseHandle(__ovl_event_);
          __ovl_event_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), std::current_exception());
          return;
        }

        try
        {
          __thread_ = std::thread{[this] { this->__run_loop(); }};
        }
        catch (...)
        {
          CloseHandle(__ovl_event_);
          __ovl_event_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), std::current_exception());
          return;
        }

        // Register stop callback last; if the token is already in stop state
        // it fires synchronously, which is now safe because the worker is up.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)), __on_stop_fn{this});
      }

      // Runs on the dedicated worker thread.
      void __run_loop() noexcept
      {
        const DWORD __byte_size = static_cast<DWORD>(__buffer_.size() * sizeof(DWORD));

        while (!__stop_requested_.load(std::memory_order_acquire))
        {
          __ovl_         = OVERLAPPED{};
          __ovl_.hEvent  = __ovl_event_;
          DWORD __unused = 0;

          BOOL __ok = ReadDirectoryChangesW(__dir_,
                                            __buffer_.data(),
                                            __byte_size,
                                            __opts_.watch_subtree ? TRUE : FALSE,
                                            __opts_.filter,
                                            &__unused,
                                            &__ovl_,
                                            nullptr);
          if (!__ok)
          {
            DWORD __err = GetLastError();
            if (__stop_requested_.load(std::memory_order_acquire))
            {
              __finish_stopped();
              return;
            }
            __finish_error(std::make_exception_ptr(std::system_error{static_cast<int>(__err),
                                                                     std::system_category(),
                                                                     "ReadDirectoryChangesW"}));
            return;
          }

          // If a stop request raced ahead of the call, cancel right away so
          // GetOverlappedResult below returns ERROR_OPERATION_ABORTED.
          if (__stop_requested_.load(std::memory_order_acquire))
          {
            CancelIoEx(__dir_, &__ovl_);
          }

          DWORD __got = 0;
          BOOL  __res = GetOverlappedResult(__dir_, &__ovl_, &__got, /*bWait*/ TRUE);
          if (!__res)
          {
            DWORD __err = GetLastError();
            if (__err == ERROR_OPERATION_ABORTED
                || __stop_requested_.load(std::memory_order_acquire))
            {
              __finish_stopped();
              return;
            }
            __finish_error(std::make_exception_ptr(std::system_error{static_cast<int>(__err),
                                                                     std::system_category(),
                                                                     "GetOverlappedResult"}));
            return;
          }

          // got == 0 with success means the kernel buffer was too small; all
          // events for this window are gone.
          bool const __overflow = (__got == 0);
          __staging_.clear();
          if (!__overflow)
          {
            auto const * __raw = reinterpret_cast<std::byte const *>(__buffer_.data());
            std::size_t  __off = 0;
            while (__off < __got)
            {
              auto const * __fni = reinterpret_cast<const FILE_NOTIFY_INFORMATION*>(__raw + __off);
              std::size_t const __chars = __fni->FileNameLength / sizeof(WCHAR);
              __staging_.push_back({
                std::wstring{__fni->FileName, __chars},
                __fni->Action
              });
              if (__fni->NextEntryOffset == 0)
                break;
              __off += __fni->NextEntryOffset;
            }
          }
          fs_batch __batch{__staging_, __overflow};
          deliver(__batch);

          if (__delivery_state_ == 2)
          {
            __finish_stopped();
            return;
          }
          if (__delivery_state_ == 3)
          {
            __finish_error(std::move(__error_));
            return;
          }
        }

        __finish_stopped();
      }

      // Called from __run_loop on the worker thread.
      void deliver(fs_batch __batch) noexcept override
      {
        __delivery_state_ = 0;

        try
        {
          __next_op_.reset(
            new __next_op_t(stdexec::connect(exec::set_next(__rcvr_, stdexec::just(__batch)),
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
        __next_op_.reset();
      }

      void __teardown() noexcept
      {
        // Drop the stop callback first so any in-flight invocation finishes
        // before we close the handles it might touch (CancelIoEx).
        __stop_cb_.reset();
        if (__dir_ != INVALID_HANDLE_VALUE)
        {
          CloseHandle(__dir_);
          __dir_ = INVALID_HANDLE_VALUE;
        }
        if (__ovl_event_)
        {
          CloseHandle(__ovl_event_);
          __ovl_event_ = nullptr;
        }
        __ctx_->__active_.store(nullptr, std::memory_order_release);
      }

      // Both __finish_* run on the worker thread as the last step before
      // the thread function returns. We detach the thread before completing
      // the receiver so the op state's destructor (potentially triggered by
      // the receiver completion) does not std::terminate on a joinable thread.
      void __finish_stopped() noexcept
      {
        __teardown();
        auto __local = static_cast<_Rcvr&&>(__rcvr_);
        __thread_.detach();
        stdexec::set_stopped(std::move(__local));
      }

      void __finish_error(std::exception_ptr __ep) noexcept
      {
        __teardown();
        auto __local = static_cast<_Rcvr&&>(__rcvr_);
        __thread_.detach();
        stdexec::set_error(std::move(__local), std::move(__ep));
      }
    };

    template <class _Rcvr>
    void __op<_Rcvr>::__on_stop_fn::operator()() noexcept
    {
      __self_->__stop_requested_.store(true, std::memory_order_release);
      // CancelIoEx on a handle without a pending IO returns ERROR_NOT_FOUND;
      // that's harmless — the worker thread re-checks __stop_requested_ after
      // posting Read and will cancel itself if it raced ahead.
      if (__self_->__dir_ != INVALID_HANDLE_VALUE)
      {
        CancelIoEx(__self_->__dir_, &__self_->__ovl_);
      }
      // If the worker is currently semaphore-blocked in deliver(), downstream
      // stop_token propagation must complete the next sender (typically with
      // set_stopped) to release the semaphore. Without that, the worker
      // deadlocks holding the semaphore — same caveat as fsevents_wrapper.
    }

    template <class _Rcvr>
    template <class... _Args>
    void __next_receiver<_Rcvr>::set_value(_Args&&...) noexcept
    {
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
}  // namespace rdcx
