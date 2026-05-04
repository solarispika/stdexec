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

  namespace detail
  {
    struct op_base
    {
      virtual ~op_base() = default;
    };

    template <class Rcvr>
    struct op;

    template <class Rcvr>
    struct next_receiver;

    struct watch_sender;

    enum finish_kind : int
    {
      finish_none    = 0,
      finish_stopped = 1,
      finish_error   = 2,
    };
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
      TP_CALLBACK_ENVIRON              env_;
      HANDLE                           dir_{INVALID_HANDLE_VALUE};
      PTP_IO                           io_{nullptr};
      PTP_WORK                         cleanup_work_{nullptr};
      OVERLAPPED                       ovl_{};
      std::vector<DWORD>               buffer_;
      std::vector<fs_event>            staging_;
      std::atomic<bool>                stop_requested_{false};
      std::atomic<bool>                cleanup_scheduled_{false};
      finish_kind                    finish_kind_{finish_none};
      std::exception_ptr               error_;
      std::optional<stop_callback_t> stop_cb_;
      std::unique_ptr<next_op_t>     next_op_;

      explicit op(rdc_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{o}
        , rcvr_{std::move(r)}
      {
        // nullptr from native_handle() is the documented sentinel for
        // "process default pool" that SetThreadpoolCallbackPool accepts.
        InitializeThreadpoolEnvironment(&env_);
        auto sch = stdexec::get_scheduler(stdexec::get_env(rcvr_));
        SetThreadpoolCallbackPool(&env_, sch.native_handle());
      }

      ~op() override
      {
        if (io_)
        {
          // Safe even if a callback is in flight; the pool defers the
          // free until callbacks return. We only reach the destructor
          // after the receiver has been completed, which only happens
          // after the cleanup work has waited for all IO callbacks.
          CloseThreadpoolIo(io_);
        }
        if (cleanup_work_)
        {
          CloseThreadpoolWork(cleanup_work_);
        }
        if (dir_ != INVALID_HANDLE_VALUE)
        {
          CloseHandle(dir_);
        }
        DestroyThreadpoolEnvironment(&env_);
      }

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

        io_ = CreateThreadpoolIo(dir_, &io_callback, this, &env_);
        if (!io_)
        {
          DWORD e = GetLastError();
          ctx_->active_.store(nullptr, std::memory_order_release);
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolIo"}));
          return;
        }

        cleanup_work_ = CreateThreadpoolWork(&cleanup_callback, this, &env_);
        if (!cleanup_work_)
        {
          DWORD e = GetLastError();
          CloseThreadpoolIo(io_);
          io_ = nullptr;
          ctx_->active_.store(nullptr, std::memory_order_release);
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork"}));
          return;
        }

        std::size_t const dwords = (opts_.buffer_size + sizeof(DWORD) - 1) / sizeof(DWORD);
        try
        {
          buffer_.resize(dwords);
        }
        catch (...)
        {
          CloseThreadpoolWork(cleanup_work_);
          cleanup_work_ = nullptr;
          CloseThreadpoolIo(io_);
          io_ = nullptr;
          ctx_->active_.store(nullptr, std::memory_order_release);
          CloseHandle(dir_);
          dir_ = INVALID_HANDLE_VALUE;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_), std::current_exception());
          return;
        }

        // Register the stop callback before posting the first read so that
        // a stop request that races with start() can land on a valid handle.
        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});

        post_read();
      }

      // Issue one ReadDirectoryChangesW. Either fires io_callback later
      // (success / async pending) or schedules cleanup on synchronous error.
      void post_read() noexcept
      {
        if (stop_requested_.load(std::memory_order_acquire))
        {
          schedule_cleanup(finish_stopped);
          return;
        }

        ovl_                     = OVERLAPPED{};
        const DWORD byte_size    = static_cast<DWORD>(buffer_.size() * sizeof(DWORD));
        DWORD       unused_bytes = 0;

        StartThreadpoolIo(io_);
        BOOL ok = ReadDirectoryChangesW(dir_,
                                          buffer_.data(),
                                          byte_size,
                                          opts_.watch_subtree ? TRUE : FALSE,
                                          opts_.filter,
                                          &unused_bytes,
                                          &ovl_,
                                          nullptr);
        if (!ok)
        {
          DWORD err = GetLastError();
          if (err != ERROR_IO_PENDING)
          {
            // Synchronous failure: the IO callback will not be invoked, so
            // we must withdraw the StartThreadpoolIo expectation ourselves.
            CancelThreadpoolIo(io_);
            if (stop_requested_.load(std::memory_order_acquire))
            {
              schedule_cleanup(finish_stopped);
            }
            else
            {
              error_ = std::make_exception_ptr(std::system_error{static_cast<int>(err),
                                                                   std::system_category(),
                                                                   "ReadDirectoryChangesW"});
              schedule_cleanup(finish_error);
            }
            return;
          }
        }

        // Re-check stop in case it raced ahead of the post; the io_callback
        // will then receive ERROR_OPERATION_ABORTED.
        if (stop_requested_.load(std::memory_order_acquire))
        {
          CancelIoEx(dir_, &ovl_);
        }
      }

      // ---------- IO completion path (runs on a pool worker thread) -------

      static void CALLBACK io_callback(PTP_CALLBACK_INSTANCE,
                                         void*     ctx_ptr,
                                         void*     ovl_ptr,
                                         ULONG     io_result,
                                         ULONG_PTR bytes,
                                         PTP_IO) noexcept
      {
        auto* self = static_cast<op*>(ctx_ptr);
        (void) ovl_ptr;

        if (io_result == ERROR_OPERATION_ABORTED
            || self->stop_requested_.load(std::memory_order_acquire))
        {
          self->schedule_cleanup(finish_stopped);
          return;
        }
        if (io_result != NO_ERROR)
        {
          self->error_ = std::make_exception_ptr(
            std::system_error{static_cast<int>(io_result),
                              std::system_category(),
                              "ReadDirectoryChangesW (completion)"});
          self->schedule_cleanup(finish_error);
          return;
        }

        bool const overflow = (bytes == 0);
        self->staging_.clear();
        if (!overflow)
        {
          auto const * raw = reinterpret_cast<std::byte const *>(self->buffer_.data());
          std::size_t  off = 0;
          while (off < bytes)
          {
            auto const * fni = reinterpret_cast<const FILE_NOTIFY_INFORMATION*>(raw + off);
            std::size_t const chars = fni->FileNameLength / sizeof(WCHAR);
            self->staging_.push_back({
              std::wstring{fni->FileName, chars},
              fni->Action
            });
            if (fni->NextEntryOffset == 0)
              break;
            off += fni->NextEntryOffset;
          }
        }

        fs_batch batch{self->staging_, overflow};

        try
        {
          self->next_op_.reset(new next_op_t(
            stdexec::connect(exec::set_next(self->rcvr_, stdexec::just(batch)),
                             next_receiver_t{self})));
          stdexec::start(*self->next_op_);
        }
        catch (...)
        {
          self->error_ = std::current_exception();
          self->schedule_cleanup(finish_error);
        }
      }

      // ---------- next-sender continuation path ---------------------------

      void on_next_value() noexcept
      {
        // We do NOT reset next_op_ here because a synchronous downstream
        // completes inside stdexec::start(*next_op_); destroying it from
        // within set_value() would tear down the call stack. The next
        // post_read -> io_callback -> deliver path overwrites it via
        // unique_ptr::reset(new ...), which destroys the previous op only
        // after start() has returned all the way back up.
        if (stop_requested_.load(std::memory_order_acquire))
        {
          schedule_cleanup(finish_stopped);
          return;
        }
        post_read();
      }

      void on_next_stopped() noexcept
      {
        schedule_cleanup(finish_stopped);
      }

      void on_next_error(std::exception_ptr ep) noexcept
      {
        error_ = std::move(ep);
        schedule_cleanup(finish_error);
      }

      // ---------- cleanup path --------------------------------------------

      void schedule_cleanup(finish_kind kind) noexcept
      {
        bool expected = false;
        if (!cleanup_scheduled_.compare_exchange_strong(expected, true))
          return;
        finish_kind_ = kind;
        SubmitThreadpoolWork(cleanup_work_);
      }

      static void CALLBACK cleanup_callback(PTP_CALLBACK_INSTANCE,
                                              void* ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* self = static_cast<op*>(ctx_ptr);
        self->teardown_and_complete();
      }

      void teardown_and_complete() noexcept
      {
        // Drop the stop callback first so an in-flight invocation finishes
        // before we touch the directory handle below.
        stop_cb_.reset();

        // Cancel any pending Read so its io_callback fires (with
        // ERROR_OPERATION_ABORTED) and gets drained by the wait below.
        if (dir_ != INVALID_HANDLE_VALUE)
        {
          CancelIoEx(dir_, nullptr);
        }
        // Wait for any in-flight io_callback to return. Safe here because
        // we are running in a *work* callback, not an IO callback; calling
        // WaitForThreadpoolIoCallbacks from inside an IO callback would
        // deadlock.
        if (io_)
        {
          WaitForThreadpoolIoCallbacks(io_, /*fCancelPendingCallbacks*/ TRUE);
        }

        // Drop the previous-batch's child op state explicitly. By the time
        // we run, no IO callback can be issuing a fresh next_op, and we are
        // not nested inside its start(), so destruction here is safe.
        next_op_.reset();

        ctx_->active_.store(nullptr, std::memory_order_release);

        // Move receiver and error out before completing — the parent op
        // may destroy *this* synchronously inside the completion call.
        auto       local_rcvr = static_cast<Rcvr&&>(rcvr_);
        auto       ep         = std::move(error_);
        auto const kind       = finish_kind_;

        if (kind == finish_error)
        {
          stdexec::set_error(std::move(local_rcvr), std::move(ep));
        }
        else
        {
          stdexec::set_stopped(std::move(local_rcvr));
        }
      }
    };

    template <class Rcvr>
    void op<Rcvr>::on_stop_fn::operator()() noexcept
    {
      self_->stop_requested_.store(true, std::memory_order_release);
      // CancelIoEx returns ERROR_NOT_FOUND if no Read is pending; that's
      // fine — the next post_read re-checks the flag and cancels itself.
      if (self_->dir_ != INVALID_HANDLE_VALUE)
      {
        CancelIoEx(self_->dir_, &self_->ovl_);
      }
      // Don't schedule cleanup here. The flag + CancelIoEx route the stop
      // through the io_callback / next_receiver paths so cleanup runs on a
      // pool thread, not on whichever thread requested the stop.
    }

    template <class Rcvr>
    template <class... Args>
    void next_receiver<Rcvr>::set_value(Args&&...) noexcept
    {
      self_->on_next_value();
    }

    template <class Rcvr>
    void next_receiver<Rcvr>::set_stopped() noexcept
    {
      self_->on_next_stopped();
    }

    template <class Rcvr>
    template <class E>
    void next_receiver<Rcvr>::set_error(E&& e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<E>, std::exception_ptr>)
      {
        self_->on_next_error(std::forward<E>(e));
      }
      else
      {
        self_->on_next_error(std::make_exception_ptr(std::forward<E>(e)));
      }
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
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>,
                                           exec::windows_thread_pool::scheduler>
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
}  // namespace rdcx::pool
