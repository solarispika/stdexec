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

  namespace detail
  {
    struct op_base
    {
      virtual ~op_base()                    = default;
      virtual void deliver(fs_batch) noexcept = 0;
    };

    template <class Rcvr>
    struct op;

    template <class Rcvr>
    struct next_receiver;

    struct watch_sender;
  }  // namespace detail

  class rdc_context
  {
   public:
    explicit rdc_context(std::wstring path)
      : path_{std::move(path)}
    {}

    rdc_context(rdc_context const &)                    = delete;
    auto operator=(rdc_context const &) -> rdc_context& = delete;

    auto watch(watch_options opts = {}) -> detail::watch_sender;

   private:
    template <class Rcvr>
    friend struct detail::op;
    template <class Rcvr>
    friend struct detail::next_receiver;
    friend struct detail::watch_sender;

    std::wstring                      path_;
    std::atomic<detail::op_base*> active_{nullptr};
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

      rdc_context*                     ctx_;
      watch_options                    opts_;
      Rcvr                            rcvr_;
      HANDLE                           dir_{INVALID_HANDLE_VALUE};
      HANDLE                           ovl_event_{nullptr};
      OVERLAPPED                       ovl_{};
      std::vector<DWORD>               buffer_;  // DWORD-aligned storage
      std::vector<fs_event>            staging_;
      std::thread                      thread_;
      std::atomic<bool>                stop_requested_{false};
      std::binary_semaphore            delivery_done_{0};
      int                              delivery_state_{0};  // 1=value, 2=stopped, 3=error
      std::exception_ptr               error_;
      std::optional<stop_callback_t> stop_cb_;
      std::unique_ptr<next_op_t>     next_op_;

      explicit op(rdc_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{o}
        , rcvr_{std::move(r)}
      {}

      void start() & noexcept
      {
        dir_ = CreateFileW(ctx_->path_.c_str(),
                             FILE_LIST_DIRECTORY,
                             FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                             nullptr,
                             OPEN_EXISTING,
                             FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
                             nullptr);
        if (dir_ == INVALID_HANDLE_VALUE)
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(
                               std::system_error{static_cast<int>(GetLastError()),
                                                 std::system_category(),
                                                 "CreateFileW"}));
          return;
        }

        op_base* expected = nullptr;
        if (!ctx_->active_.compare_exchange_strong(expected, this))
        {
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"rdc_context already has "
                                                                        "an active watch"}));
          return;
        }

        ovl_event_ = CreateEventW(nullptr,
                                    /*bManualReset*/ FALSE,
                                    /*bInitialState*/ FALSE,
                                    nullptr);
        if (!ovl_event_)
        {
          DWORD e = GetLastError();
          ctx_->active_.store(nullptr, std::memory_order_release);
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(e),
                                                                       std::system_category(),
                                                                       "CreateEventW"}));
          return;
        }

        std::size_t const dwords = (opts_.buffer_size + sizeof(DWORD) - 1) / sizeof(DWORD);
        try
        {
          buffer_.resize(dwords);
        }
        catch (...)
        {
          CloseHandle(ovl_event_);
          ovl_event_ = nullptr;
          ctx_->active_.store(nullptr, std::memory_order_release);
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_), std::current_exception());
          return;
        }

        try
        {
          thread_ = std::thread{[this] { this->run_loop(); }};
        }
        catch (...)
        {
          CloseHandle(ovl_event_);
          ovl_event_ = nullptr;
          ctx_->active_.store(nullptr, std::memory_order_release);
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_), std::current_exception());
          return;
        }

        // Register stop callback last; if the token is already in stop state
        // it fires synchronously, which is now safe because the worker is up.
        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});
      }

      // Runs on the dedicated worker thread.
      void run_loop() noexcept
      {
        const DWORD byte_size = static_cast<DWORD>(buffer_.size() * sizeof(DWORD));

        while (!stop_requested_.load(std::memory_order_acquire))
        {
          ovl_         = OVERLAPPED{};
          ovl_.hEvent  = ovl_event_;
          DWORD unused = 0;

          BOOL ok = ReadDirectoryChangesW(dir_,
                                            buffer_.data(),
                                            byte_size,
                                            opts_.watch_subtree ? TRUE : FALSE,
                                            opts_.filter,
                                            &unused,
                                            &ovl_,
                                            nullptr);
          if (!ok)
          {
            DWORD err = GetLastError();
            if (stop_requested_.load(std::memory_order_acquire))
            {
              finish_stopped();
              return;
            }
            finish_error(std::make_exception_ptr(std::system_error{static_cast<int>(err),
                                                                     std::system_category(),
                                                                     "ReadDirectoryChangesW"}));
            return;
          }

          // If a stop request raced ahead of the call, cancel right away so
          // GetOverlappedResult below returns ERROR_OPERATION_ABORTED.
          if (stop_requested_.load(std::memory_order_acquire))
          {
            CancelIoEx(dir_, &ovl_);
          }

          DWORD got = 0;
          BOOL  res = GetOverlappedResult(dir_, &ovl_, &got, /*bWait*/ TRUE);
          if (!res)
          {
            DWORD err = GetLastError();
            if (err == ERROR_OPERATION_ABORTED
                || stop_requested_.load(std::memory_order_acquire))
            {
              finish_stopped();
              return;
            }
            finish_error(std::make_exception_ptr(std::system_error{static_cast<int>(err),
                                                                     std::system_category(),
                                                                     "GetOverlappedResult"}));
            return;
          }

          // got == 0 with success means the kernel buffer was too small; all
          // events for this window are gone.
          bool const overflow = (got == 0);
          staging_.clear();
          if (!overflow)
          {
            auto const * raw = reinterpret_cast<std::byte const *>(buffer_.data());
            std::size_t  off = 0;
            while (off < got)
            {
              auto const * fni = reinterpret_cast<const FILE_NOTIFY_INFORMATION*>(raw + off);
              std::size_t const chars = fni->FileNameLength / sizeof(WCHAR);
              staging_.push_back({
                std::wstring{fni->FileName, chars},
                fni->Action
              });
              if (fni->NextEntryOffset == 0)
                break;
              off += fni->NextEntryOffset;
            }
          }
          fs_batch batch{staging_, overflow};
          deliver(batch);

          if (delivery_state_ == 2)
          {
            finish_stopped();
            return;
          }
          if (delivery_state_ == 3)
          {
            finish_error(std::move(error_));
            return;
          }
        }

        finish_stopped();
      }

      // Called from run_loop on the worker thread.
      void deliver(fs_batch batch) noexcept override
      {
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
        next_op_.reset();
      }

      void teardown() noexcept
      {
        // Drop the stop callback first so any in-flight invocation finishes
        // before we close the handles it might touch (CancelIoEx).
        stop_cb_.reset();
        if (dir_ != INVALID_HANDLE_VALUE)
        {
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
        }
        if (ovl_event_)
        {
          CloseHandle(ovl_event_);
          ovl_event_ = nullptr;
        }
        ctx_->active_.store(nullptr, std::memory_order_release);
      }

      // Both finish_* run on the worker thread as the last step before
      // the thread function returns. We detach the thread before completing
      // the receiver so the op state's destructor (potentially triggered by
      // the receiver completion) does not std::terminate on a joinable thread.
      void finish_stopped() noexcept
      {
        teardown();
        auto local = static_cast<Rcvr&&>(rcvr_);
        thread_.detach();
        stdexec::set_stopped(std::move(local));
      }

      void finish_error(std::exception_ptr ep) noexcept
      {
        teardown();
        auto local = static_cast<Rcvr&&>(rcvr_);
        thread_.detach();
        stdexec::set_error(std::move(local), std::move(ep));
      }
    };

    template <class Rcvr>
    void op<Rcvr>::on_stop_fn::operator()() noexcept
    {
      self_->stop_requested_.store(true, std::memory_order_release);
      // CancelIoEx on a handle without a pending IO returns ERROR_NOT_FOUND;
      // that's harmless — the worker thread re-checks stop_requested_ after
      // posting Read and will cancel itself if it raced ahead.
      if (self_->dir_ != INVALID_HANDLE_VALUE)
      {
        CancelIoEx(self_->dir_, &self_->ovl_);
      }
      // If the worker is currently semaphore-blocked in deliver(), downstream
      // stop_token propagation must complete the next sender (typically with
      // set_stopped) to release the semaphore. Without that, the worker
      // deadlocks holding the semaphore — same caveat as fsevents_wrapper.
    }

    template <class Rcvr>
    template <class... Args>
    void next_receiver<Rcvr>::set_value(Args&&...) noexcept
    {
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

      rdc_context*  ctx_;
      watch_options opts_;

      template <stdexec::receiver Rcvr>
      auto subscribe(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ctx_, opts_, std::move(rcvr)};
      }
    };
  }  // namespace detail

  inline auto rdc_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, opts};
  }
}  // namespace rdcx
