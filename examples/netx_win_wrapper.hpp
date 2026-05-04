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

  class net_context
  {
   public:
    net_context()  = default;
    ~net_context() = default;

    net_context(net_context const &)                    = delete;
    auto operator=(net_context const &) -> net_context& = delete;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
    template <class _Rcvr>
    friend struct __detail::__next_receiver;
    friend struct __detail::__watch_sender;

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
      using __item_sender_t   = decltype(stdexec::just(std::declval<interface_change_event>()));
      using __next_sender_t   = exec::next_sender_of_t<_Rcvr, __item_sender_t>;
      using __next_receiver_t = __next_receiver<_Rcvr>;
      using __next_op_t       = stdexec::connect_result_t<__next_sender_t, __next_receiver_t>;

      net_context*        __ctx_;
      watch_options       __opts_;
      _Rcvr               __rcvr_;
      TP_CALLBACK_ENVIRON __env_{};
      HANDLE              __notify_handle_{nullptr};
      PTP_WORK            __drainer_work_{nullptr};
      PTP_WORK            __cleanup_work_{nullptr};

      // MPSC queue (system thread → drainer pool work item).
      std::mutex                         __queue_mu_;
      std::deque<interface_change_event> __queue_;
      std::atomic<bool>                  __drainer_running_{false};

      std::binary_semaphore __delivery_done_{0};
      int                   __delivery_state_{0};  // 1=value 2=stopped 3=error

      std::atomic<bool>            __stop_requested_{false};
      __finish_kind                __finish_kind_{__finish_none};
      std::exception_ptr           __error_;
      std::unique_ptr<__next_op_t> __next_op_;

      struct __on_stop_fn
      {
        __op* __self_;
        void  operator()() noexcept
        {
          __self_->__stop_requested_.store(true, std::memory_order_release);
          __self_->__schedule_cleanup(__finish_stopped);
        }
      };

      using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
      using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;

      std::atomic<bool>                __cleanup_scheduled_{false};
      std::optional<__stop_callback_t> __stop_cb_;

      explicit __op(net_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
      {
        InitializeThreadpoolEnvironment(&__env_);
        auto __sched = stdexec::get_scheduler(stdexec::get_env(__rcvr_));
        SetThreadpoolCallbackPool(&__env_, __sched.native_handle());
      }

      ~__op() override
      {
        if (__notify_handle_)
          CancelMibChangeNotify2(__notify_handle_);
        if (__drainer_work_)
          CloseThreadpoolWork(__drainer_work_);
        if (__cleanup_work_)
          CloseThreadpoolWork(__cleanup_work_);
        DestroyThreadpoolEnvironment(&__env_);
      }

      // IP-helper callback. Runs on a system worker thread we don't own —
      // must NOT block, must NOT call CancelMibChangeNotify2 from inside
      // (deadlock per MSDN). Push to MPSC and kick the drainer.
      static void NETIOAPI_API_ __on_change_cb(PVOID __ctx,
                                               PMIB_IPINTERFACE_ROW /*row*/,
                                               MIB_NOTIFICATION_TYPE /*type*/) noexcept
      {
        auto* __self   = static_cast<__op*>(__ctx);
        bool  __submit = false;
        {
          std::lock_guard __lk{__self->__queue_mu_};
          if (__self->__stop_requested_.load(std::memory_order_acquire))
            return;
          __self->__queue_.push_back(interface_change_event{});
          __submit = !__self->__drainer_running_.exchange(true, std::memory_order_acq_rel);
        }
        if (__submit)
          SubmitThreadpoolWork(__self->__drainer_work_);
      }

      static void CALLBACK __drainer_callback(PTP_CALLBACK_INSTANCE,
                                              void* __ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* __self = static_cast<__op*>(__ctx_ptr);
        for (;;)
        {
          interface_change_event __ev{};
          {
            std::lock_guard __lk{__self->__queue_mu_};
            if (__self->__stop_requested_.load(std::memory_order_acquire))
            {
              __self->__drainer_running_.store(false, std::memory_order_release);
              break;
            }
            if (__self->__queue_.empty())
            {
              __self->__drainer_running_.store(false, std::memory_order_release);
              return;
            }
            __ev = std::move(__self->__queue_.front());
            __self->__queue_.pop_front();
          }

          __self->__delivery_state_ = 0;
          try
          {
            __self->__next_op_.reset(new __next_op_t(
              stdexec::connect(exec::set_next(__self->__rcvr_, stdexec::just(std::move(__ev))),
                               __next_receiver_t{__self})));
            stdexec::start(*__self->__next_op_);
          }
          catch (...)
          {
            __self->__error_          = std::current_exception();
            __self->__delivery_state_ = 3;
            __self->__delivery_done_.release();
          }

          __self->__delivery_done_.acquire();
          int const __state = __self->__delivery_state_;
          __self->__next_op_.reset();

          if (__state == 2)
          {
            __self->__schedule_cleanup(__finish_stopped);
            return;
          }
          if (__state == 3)
          {
            __self->__schedule_cleanup(__finish_error);
            return;
          }
        }
        __self->__schedule_cleanup(__finish_stopped);
      }

      void __schedule_cleanup(__finish_kind __k) noexcept
      {
        bool __expected = false;
        if (!__cleanup_scheduled_.compare_exchange_strong(__expected,
                                                          true,
                                                          std::memory_order_acq_rel))
          return;
        __finish_kind_ = __k;
        SubmitThreadpoolWork(__cleanup_work_);
      }

      static void CALLBACK __cleanup_callback(PTP_CALLBACK_INSTANCE,
                                              void* __ctx_ptr,
                                              PTP_WORK) noexcept
      {
        static_cast<__op*>(__ctx_ptr)->__teardown_and_complete();
      }

      void __teardown_and_complete() noexcept
      {
        __stop_cb_.reset();

        // CancelMibChangeNotify2 blocks until any in-flight callback for
        // this handle returns. Safe from inside a TP_WORK callback (we are
        // NOT in __on_change_cb's frame).
        if (__notify_handle_)
        {
          CancelMibChangeNotify2(__notify_handle_);
          __notify_handle_ = nullptr;
        }

        if (__drainer_work_)
          WaitForThreadpoolWorkCallbacks(__drainer_work_, /*fCancelPendingCallbacks*/ FALSE);

        __next_op_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);

        auto                __local_rcvr = static_cast<_Rcvr&&>(__rcvr_);
        auto                __ep         = std::move(__error_);
        __finish_kind const __kind       = __finish_kind_;

        if (__kind == __finish_error)
          stdexec::set_error(std::move(__local_rcvr), std::move(__ep));
        else
          stdexec::set_stopped(std::move(__local_rcvr));
      }

      void start() & noexcept
      {
        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"net_context already has "
                                                                        "an active watch"}));
          return;
        }

        __drainer_work_ = CreateThreadpoolWork(&__drainer_callback, this, &__env_);
        if (!__drainer_work_)
        {
          DWORD const __e = GetLastError();
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(__e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork "
                                                                       "(drainer)"}));
          return;
        }

        __cleanup_work_ = CreateThreadpoolWork(&__cleanup_callback, this, &__env_);
        if (!__cleanup_work_)
        {
          DWORD const __e = GetLastError();
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(__e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork "
                                                                       "(cleanup)"}));
          return;
        }

        // initialNotification = FALSE: do not synthesize a sync replay.
        // Consumers that want a baseline snapshot should query
        // GetAdaptersAddresses themselves before starting the watch.
        if (DWORD const __cr =
              NotifyIpInterfaceChange(AF_UNSPEC, &__on_change_cb, this, FALSE, &__notify_handle_);
            __cr != NO_ERROR)
        {
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(__cr),
                                                                       std::system_category(),
                                                                       "NotifyIpInterfaceChange"}));
          return;
        }

        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)), __on_stop_fn{this});
      }
    };

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
        __self_->__error_ = std::forward<_E>(__e);
      else
        __self_->__error_ = std::make_exception_ptr(std::forward<_E>(__e));
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

      using __item_sender_t = decltype(stdexec::just(std::declval<interface_change_event>()));
      using item_types      = exec::item_types<__item_sender_t>;

      net_context*  __ctx_;
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

  inline auto net_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace netx
