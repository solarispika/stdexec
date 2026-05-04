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

// Windows — netx interface-change wrapper.
//
// Wraps `NotifyIpInterfaceChange` as a stdexec sequence sender driven by
// exec::windows_thread_pool. Mirrors the velx wrapper
// (examples/velx_wrapper.hpp): IP-helper callbacks fire on a Windows
// system worker thread we don't own, so events are handed off via an MPSC
// queue + drainer pool work item that performs the DA-style
// binary_semaphore handshake on the user's pool. See
// docs/plans/2026-05-04-powerx-netx-system-signal-senders-design.md.
//
// The macOS counterpart lives in examples/netx_mac_wrapper.hpp; both
// define `netx::net_context` so downstream code can `#if`-select.
//
// Replaces OrangeDrive's WindowsNetworkDetector
// (lib/detector/windows/network-detector.cpp), which spins up its own
// jthread + WaitForSingleObject(shutdownEvent) loop. The OrangeDrive
// reference also performs an interface-checksum debounce against
// GetAdaptersAddresses to suppress spurious change events; that filter is
// application policy and stays in downstream `transform_each(then(...))`.
// This wrapper passes through every notification the IP-helper API emits.

#if !defined(_WIN32)
#  error "netx_win_wrapper.hpp is Windows-only; on macOS include netx_mac_wrapper.hpp"
#endif

#ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#  define NOMINMAX
#endif
// Order is load-bearing here: winsock2.h MUST come before any windows.h
// (winsock2.h sets _WINSOCKAPI_ and pulls in the parts of windows.h it
// needs, suppressing the legacy winsock.h that windows.h would otherwise
// drag in). Don't let clang-format alphabetize.
// clang-format off
#include <winsock2.h>
#include <ws2ipdef.h>
#include <ws2tcpip.h>
#include <iphlpapi.h>
#include <netioapi.h>
#include <windows.h>
// clang-format on

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

namespace netx
{
  // Hint-only event: "the IPv4/IPv6 interface table may have changed;
  // re-read it." Matches NotifyIpInterfaceChange semantics.
  struct interface_change_event
  {};

  struct watch_options
  {
    // v1: empty. Future fields might include address-family scoping
    // (IPv4-only / IPv6-only). The current default is AF_UNSPEC.
  };

  class net_context;

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

  class net_context
  {
   public:
    net_context()  = default;
    ~net_context() = default;

    net_context(net_context const &)                    = delete;
    auto operator=(net_context const &) -> net_context& = delete;

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
      using item_sender_t   = decltype(stdexec::just(std::declval<interface_change_event>()));
      using next_sender_t   = exec::next_sender_of_t<Rcvr, item_sender_t>;
      using next_receiver_t = next_receiver<Rcvr>;
      using next_op_t       = stdexec::connect_result_t<next_sender_t, next_receiver_t>;

      net_context*        ctx_;
      watch_options       opts_;
      Rcvr               rcvr_;
      TP_CALLBACK_ENVIRON env_{};
      HANDLE              notify_handle_{nullptr};
      PTP_WORK            drainer_work_{nullptr};
      PTP_WORK            cleanup_work_{nullptr};

      // MPSC queue (system thread → drainer pool work item).
      std::mutex                         queue_mu_;
      std::deque<interface_change_event> queue_;
      std::atomic<bool>                  drainer_running_{false};

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

      explicit op(net_context* c, watch_options o, Rcvr r)
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
        if (notify_handle_)
          CancelMibChangeNotify2(notify_handle_);
        if (drainer_work_)
          CloseThreadpoolWork(drainer_work_);
        if (cleanup_work_)
          CloseThreadpoolWork(cleanup_work_);
        DestroyThreadpoolEnvironment(&env_);
      }

      // IP-helper callback. Runs on a system worker thread we don't own —
      // must NOT block, must NOT call CancelMibChangeNotify2 from inside
      // (deadlock per MSDN). Push to MPSC and kick the drainer.
      static void NETIOAPI_API_ on_change_cb(PVOID ctx,
                                               PMIB_IPINTERFACE_ROW /*row*/,
                                               MIB_NOTIFICATION_TYPE /*type*/) noexcept
      {
        auto* self   = static_cast<op*>(ctx);
        bool  submit = false;
        {
          std::lock_guard lk{self->queue_mu_};
          if (self->stop_requested_.load(std::memory_order_acquire))
            return;
          self->queue_.push_back(interface_change_event{});
          submit = !self->drainer_running_.exchange(true, std::memory_order_acq_rel);
        }
        if (submit)
          SubmitThreadpoolWork(self->drainer_work_);
      }

      static void CALLBACK drainer_callback(PTP_CALLBACK_INSTANCE,
                                              void* ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* self = static_cast<op*>(ctx_ptr);
        for (;;)
        {
          interface_change_event ev{};
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

        // CancelMibChangeNotify2 blocks until any in-flight callback for
        // this handle returns. Safe from inside a TP_WORK callback (we are
        // NOT in on_change_cb's frame).
        if (notify_handle_)
        {
          CancelMibChangeNotify2(notify_handle_);
          notify_handle_ = nullptr;
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
                             std::make_exception_ptr(std::runtime_error{"net_context already has "
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

        // initialNotification = FALSE: do not synthesize a sync replay.
        // Consumers that want a baseline snapshot should query
        // GetAdaptersAddresses themselves before starting the watch.
        if (DWORD const cr =
              NotifyIpInterfaceChange(AF_UNSPEC, &on_change_cb, this, FALSE, &notify_handle_);
            cr != NO_ERROR)
        {
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(cr),
                                                                       std::system_category(),
                                                                       "NotifyIpInterfaceChange"}));
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

      using item_sender_t = decltype(stdexec::just(std::declval<interface_change_event>()));
      using item_types      = exec::item_types<item_sender_t>;

      net_context*  ctx_;
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

  inline auto net_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, opts};
  }
}  // namespace netx
