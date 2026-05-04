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

// Windows — powerx suspend/resume wrapper.
//
// Wraps `RegisterSuspendResumeNotification(DEVICE_NOTIFY_CALLBACK, …)` as a
// stdexec sequence sender driven by exec::windows_thread_pool. Mirrors the
// velx wrapper (examples/velx_wrapper.hpp): notifications fire on a Windows
// system worker thread we don't own, so events are handed off via an MPSC
// queue + drainer pool work item that performs the DA-style
// binary_semaphore handshake on the user's pool. See
// docs/plans/2026-05-04-powerx-netx-system-signal-senders-design.md.
//
// The macOS counterpart lives in examples/powerx_mac_wrapper.hpp; both
// define `powerx::power_context` so downstream code can `#if`-select.
//
// Replaces OrangeDrive's WindowsPowerDetector
// (lib/detector/windows/power-detector.cpp), which spins up a hidden
// message-only window class and a `GetMessage` pump on its own jthread.
// Since Windows 8 a callback subscription is supported directly — no HWND
// or message pump needed.

#if !defined(_WIN32)
#  error "powerx_win_wrapper.hpp is Windows-only; on macOS include powerx_mac_wrapper.hpp"
#endif

#ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#  define NOMINMAX
#endif
#include <windows.h>
// windows.h must come first
#include <powerbase.h>
#include <powrprof.h>

#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/windows/windows_thread_pool.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace powerx
{
  enum class power_event_kind
  {
    suspend,  // PBT_APMSUSPEND
    resume,   // PBT_APMRESUMEAUTOMATIC
  };

  struct power_event
  {
    power_event_kind kind;
    // v1: no payload. battery / display / thermal events are future work.
  };

  struct watch_options
  {
    bool watch_suspend{true};
    bool watch_resume{true};
    // No approval surface in v1 — see design §5. Windows has no equivalent
    // veto path post-Vista (PBT_APMQUERYSUSPEND was deprecated).
  };

  class power_context;

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

  class power_context
  {
   public:
    power_context()  = default;
    ~power_context() = default;

    power_context(power_context const &)                    = delete;
    auto operator=(power_context const &) -> power_context& = delete;

    auto watch(watch_options opts = {}) -> detail::watch_sender;

   private:
    template <class Rcvr>
    friend struct detail::op;
    template <class Rcvr>
    friend struct detail::next_receiver;
    friend struct detail::watch_sender;

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
      using item_sender_t   = decltype(stdexec::just(std::declval<power_event>()));
      using next_sender_t   = exec::next_sender_of_t<Rcvr, item_sender_t>;
      using next_receiver_t = next_receiver<Rcvr>;
      using next_op_t       = stdexec::connect_result_t<next_sender_t, next_receiver_t>;

      power_context*      ctx_;
      watch_options       opts_;
      Rcvr               rcvr_;
      TP_CALLBACK_ENVIRON env_{};
      HPOWERNOTIFY        hnotify_{nullptr};
      PTP_WORK            drainer_work_{nullptr};
      PTP_WORK            cleanup_work_{nullptr};

      // MPSC queue (system thread → drainer pool work item).
      std::mutex              queue_mu_;
      std::deque<power_event> queue_;
      std::atomic<bool>       drainer_running_{false};

      std::binary_semaphore delivery_done_{0};
      int                   delivery_state_{0};  // 1=value 2=stopped 3=error

      std::atomic<bool>            stop_requested_{false};
      finish_kind                finish_kind_{finish_none};
      std::exception_ptr           error_;
      std::unique_ptr<next_op_t> next_op_;

      struct on_stop_fn
      {
        op* self_;
        void  operator()() noexcept
        {
          self_->stop_requested_.store(true, std::memory_order_release);
          self_->schedule_cleanup(finish_stopped);
        }
      };

      using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
      using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;

      std::atomic<bool>                cleanup_scheduled_{false};
      std::optional<stop_callback_t> stop_cb_;

      explicit op(power_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{o}
        , rcvr_{std::move(r)}
      {
        InitializeThreadpoolEnvironment(&env_);
        auto sched = stdexec::get_scheduler(stdexec::get_env(rcvr_));
        SetThreadpoolCallbackPool(&env_, sched.native_handle());
      }

      ~op() override
      {
        if (hnotify_)
          ::PowerUnregisterSuspendResumeNotification(hnotify_);
        if (drainer_work_)
          CloseThreadpoolWork(drainer_work_);
        if (cleanup_work_)
          CloseThreadpoolWork(cleanup_work_);
        DestroyThreadpoolEnvironment(&env_);
      }

      // Notification callback. Runs on a system worker thread we don't
      // own — must NOT block. Push to MPSC and kick the drainer.
      static auto WINAPI power_callback(PVOID ctx, ULONG type, PVOID /*setting*/) -> ULONG
      {
        auto* self = static_cast<op*>(ctx);

        std::optional<power_event> ev;
        switch (type)
        {
        case PBT_APMSUSPEND:
          if (self->opts_.watch_suspend)
            ev = power_event{power_event_kind::suspend};
          break;
        case PBT_APMRESUMEAUTOMATIC:
          if (self->opts_.watch_resume)
            ev = power_event{power_event_kind::resume};
          break;
        // Other PBT_* (POWERSTATUSCHANGE, BATTERYLOW, RESUMECRITICAL,
        // POWERSETTINGCHANGE, ...) intentionally ignored in v1.
        default:
          break;
        }

        if (!ev)
          return ERROR_SUCCESS;

        bool submit = false;
        {
          std::lock_guard lk{self->queue_mu_};
          if (self->stop_requested_.load(std::memory_order_acquire))
            return ERROR_SUCCESS;
          self->queue_.push_back(std::move(*ev));
          submit = !self->drainer_running_.exchange(true, std::memory_order_acq_rel);
        }
        if (submit)
          SubmitThreadpoolWork(self->drainer_work_);
        return ERROR_SUCCESS;
      }

      static void CALLBACK drainer_callback(PTP_CALLBACK_INSTANCE,
                                              void* ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* self = static_cast<op*>(ctx_ptr);
        for (;;)
        {
          power_event ev{};
          {
            std::lock_guard lk{self->queue_mu_};
            if (self->stop_requested_.load(std::memory_order_acquire))
            {
              self->drainer_running_.store(false, std::memory_order_release);
              break;
            }
            if (self->queue_.empty())
            {
              self->drainer_running_.store(false, std::memory_order_release);
              return;
            }
            ev = std::move(self->queue_.front());
            self->queue_.pop_front();
          }

          self->delivery_state_ = 0;
          try
          {
            self->next_op_.reset(new next_op_t(
              stdexec::connect(exec::set_next(self->rcvr_, stdexec::just(std::move(ev))),
                               next_receiver_t{self})));
            stdexec::start(*self->next_op_);
          }
          catch (...)
          {
            self->error_          = std::current_exception();
            self->delivery_state_ = 3;
            self->delivery_done_.release();
          }

          self->delivery_done_.acquire();
          int const state = self->delivery_state_;
          self->next_op_.reset();

          if (state == 2)
          {
            self->schedule_cleanup(finish_stopped);
            return;
          }
          if (state == 3)
          {
            self->schedule_cleanup(finish_error);
            return;
          }
        }
        self->schedule_cleanup(finish_stopped);
      }

      void schedule_cleanup(finish_kind k) noexcept
      {
        bool expected = false;
        if (!cleanup_scheduled_.compare_exchange_strong(expected,
                                                          true,
                                                          std::memory_order_acq_rel))
          return;
        finish_kind_ = k;
        SubmitThreadpoolWork(cleanup_work_);
      }

      static void CALLBACK cleanup_callback(PTP_CALLBACK_INSTANCE,
                                              void* ctx_ptr,
                                              PTP_WORK) noexcept
      {
        static_cast<op*>(ctx_ptr)->teardown_and_complete();
      }

      void teardown_and_complete() noexcept
      {
        stop_cb_.reset();

        if (hnotify_)
        {
          ::PowerUnregisterSuspendResumeNotification(hnotify_);
          hnotify_ = nullptr;
        }

        if (drainer_work_)
          WaitForThreadpoolWorkCallbacks(drainer_work_, /*fCancelPendingCallbacks*/ FALSE);

        next_op_.reset();
        ctx_->active_.store(nullptr, std::memory_order_release);

        auto                local_rcvr = static_cast<Rcvr&&>(rcvr_);
        auto                ep         = std::move(error_);
        finish_kind const kind       = finish_kind_;

        if (kind == finish_error)
          stdexec::set_error(std::move(local_rcvr), std::move(ep));
        else
          stdexec::set_stopped(std::move(local_rcvr));
      }

      void start() & noexcept
      {
        op_base* expected = nullptr;
        if (!ctx_->active_.compare_exchange_strong(expected, this))
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"power_context already has "
                                                                        "an active watch"}));
          return;
        }

        drainer_work_ = CreateThreadpoolWork(&drainer_callback, this, &env_);
        if (!drainer_work_)
        {
          DWORD const e = GetLastError();
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork "
                                                                       "(drainer)"}));
          return;
        }

        cleanup_work_ = CreateThreadpoolWork(&cleanup_callback, this, &env_);
        if (!cleanup_work_)
        {
          DWORD const e = GetLastError();
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork "
                                                                       "(cleanup)"}));
          return;
        }

        DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS params{};
        params.Callback = &power_callback;
        params.Context  = this;
        if (DWORD const cr = ::PowerRegisterSuspendResumeNotification(DEVICE_NOTIFY_CALLBACK,
                                                                        &params,
                                                                        &hnotify_);
            cr != ERROR_SUCCESS)
        {
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(cr),
                                                                       std::system_category(),
                                                                       "PowerRegisterSuspendResumeN"
                                                                       "otification"}));
          return;
        }

        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});
      }
    };

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
        self_->error_ = std::forward<E>(e);
      else
        self_->error_ = std::make_exception_ptr(std::forward<E>(e));
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

      using item_sender_t = decltype(stdexec::just(std::declval<power_event>()));
      using item_types      = exec::item_types<item_sender_t>;

      power_context* ctx_;
      watch_options  opts_;

      template <stdexec::receiver Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>,
                                           exec::windows_thread_pool::scheduler>
      auto subscribe(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ctx_, opts_, std::move(rcvr)};
      }
    };
  }  // namespace detail

  inline auto power_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, opts};
  }
}  // namespace powerx
