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

// macOS — powerx suspend/resume wrapper.
//
// Wraps IOPMLib (`IORegisterForSystemPower`) as a stdexec sequence sender.
// Mirrors the DA wrapper (examples/da_wrapper.hpp) and the shared
// libdispatch-sequence-sender pattern documented in
// docs/plans/2026-05-04-powerx-netx-system-signal-senders-design.md.
// The Windows counterpart lives in examples/powerx_win_wrapper.hpp; both
// define `powerx::power_context` so downstream code can `#if`-select.
//
// Replaces OrangeDrive's MacPowerDetector
// (lib/detector/mac/power-detector.cpp), which spins up its own jthread +
// CFRunLoop. The IOKit notification port is bound directly to a libdispatch
// queue via IONotificationPortSetDispatchQueue, so no runloop / worker
// thread is needed.

#if !defined(__APPLE__) || !defined(__MACH__)
#  error "powerx_mac_wrapper.hpp is macOS-only; on Windows include powerx_win_wrapper.hpp"
#endif

#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOMessage.h>
#include <IOKit/pwr_mgt/IOPMLib.h>
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
#include <stdexcept>
#include <utility>

namespace powerx
{
  enum class power_event_kind
  {
    suspend,  // kIOMessageSystemWillSleep — already-decided, non-vetoable
    resume,   // kIOMessageSystemHasPoweredOn
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
    // No approval surface in v1. The kIOMessageCanSystemSleep veto path is
    // reserved for a future `system_will_sleep_approval` field typed
    // approval::bounded<sleep_request>; see design §5.
  };

  class power_context;

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
  }  // namespace __detail

  class power_context
  {
   public:
    power_context()  = default;
    ~power_context() = default;

    power_context(power_context const &)                    = delete;
    auto operator=(power_context const &) -> power_context& = delete;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
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
      using __item_sender_t   = decltype(stdexec::just(std::declval<power_event>()));
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

      power_context*        __ctx_;
      watch_options         __opts_;
      _Rcvr                 __rcvr_;
      dispatch_queue_t      __queue_{nullptr};
      io_connect_t          __power_port_{MACH_PORT_NULL};
      IONotificationPortRef __notif_port_{nullptr};
      io_object_t           __notifier_{IO_OBJECT_NULL};

      std::atomic<bool>                __stop_requested_{false};
      std::binary_semaphore            __delivery_done_{0};
      int                              __delivery_state_{0};  // 1=value 2=stopped 3=error
      std::exception_ptr               __error_;
      std::optional<__stop_callback_t> __stop_cb_;
      std::unique_ptr<__next_op_t>     __next_op_;

      static auto __make_internal_queue(_Rcvr const & __r) -> dispatch_queue_t
      {
        auto __sch  = stdexec::get_scheduler(stdexec::get_env(__r));
        auto __attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL,
                                                              QOS_CLASS_UNSPECIFIED,
                                                              0);
        return dispatch_queue_create_with_target("powerx.session", __attr, __sch.native_handle());
      }

      explicit __op(power_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
        , __queue_{__make_internal_queue(__rcvr_)}
      {}

      ~__op() override
      {
        if (__queue_)
          dispatch_release(__queue_);
      }

      // IOKit C callback. Runs on __queue_ because we bound the notification
      // port via IONotificationPortSetDispatchQueue at the end of start().
      static void __on_power_event_cb(void* __ctx,
                                      io_service_t /*service*/,
                                      natural_t __message_type,
                                      void*     __message_argument) noexcept
      {
        auto* __self = static_cast<__op*>(__ctx);

        switch (__message_type)
        {
        case kIOMessageSystemWillSleep:
          // IOKit gives us ~30s to allow/cancel before the system stalls.
          // Acknowledge BEFORE delivery so a slow consumer cannot stall
          // suspend. v1 does not surface the veto path; consumers see
          // fait-accompli "suspend allowed" semantics.
          ::IOAllowPowerChange(__self->__power_port_,
                               reinterpret_cast<intptr_t>(__message_argument));
          if (__self->__opts_.watch_suspend)
            __self->__deliver({power_event_kind::suspend});
          break;

        case kIOMessageSystemHasPoweredOn:
          if (__self->__opts_.watch_resume)
            __self->__deliver({power_event_kind::resume});
          break;

        // Other IOPMLib messages (CanSystemSleep, WillPowerOn, DeviceWill*,
        // etc.) are intentionally ignored in v1.
        default:
          break;
        }
      }

      void start() & noexcept
      {
        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"power_context already has "
                                                                        "an active watch"}));
          return;
        }

        __power_port_ =
          IORegisterForSystemPower(this, &__notif_port_, &__on_power_event_cb, &__notifier_);
        if (__power_port_ == MACH_PORT_NULL)
        {
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"IORegisterForSystemPower "
                                                                        "failed"}));
          return;
        }

        IONotificationPortSetDispatchQueue(__notif_port_, __queue_);

        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)), __on_stop_fn{this});
      }

      void __deliver(power_event __ev) noexcept
      {
        if (__stop_requested_.load(std::memory_order_acquire))
          return;

        __delivery_state_ = 0;

        try
        {
          __next_op_.reset(new __next_op_t(
            stdexec::connect(exec::set_next(__rcvr_, stdexec::just(std::move(__ev))),
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
        int const __state = __delivery_state_;
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
        dispatch_async_f(
          __queue_,
          this,
          +[](void* __p) noexcept
          {
            auto* __o = static_cast<__op*>(__p);
            if (__o->__notifier_ == IO_OBJECT_NULL)
              return;
            __o->__teardown_session();
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
        dispatch_async_f(
          __queue_,
          __c,
          +[](void* __p) noexcept
          {
            std::unique_ptr<__closure> __cu{static_cast<__closure*>(__p)};
            if (__cu->__o->__notifier_ == IO_OBJECT_NULL)
              return;
            __cu->__o->__teardown_session();
            stdexec::set_error(static_cast<_Rcvr&&>(__cu->__o->__rcvr_), std::move(__cu->__ep));
          });
      }

      void __teardown_session() noexcept
      {
        if (__notifier_ == IO_OBJECT_NULL)
          return;
        if (__notif_port_)
          IONotificationPortSetDispatchQueue(__notif_port_, nullptr);
        IODeregisterForSystemPower(&__notifier_);
        __notifier_ = IO_OBJECT_NULL;
        if (__power_port_ != MACH_PORT_NULL)
        {
          IOServiceClose(__power_port_);
          __power_port_ = MACH_PORT_NULL;
        }
        if (__notif_port_)
        {
          IONotificationPortDestroy(__notif_port_);
          __notif_port_ = nullptr;
        }
        __stop_cb_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);
      }
    };

    template <class _Rcvr>
    void __op<_Rcvr>::__on_stop_fn::operator()() noexcept
    {
      __self_->__stop_requested_.store(true, std::memory_order_release);
      dispatch_async_f(
        __self_->__queue_,
        __self_,
        +[](void* __p) noexcept
        {
          auto* __o = static_cast<__op*>(__p);
          if (__o->__notifier_ == IO_OBJECT_NULL)
            return;
          __o->__teardown_session();
          stdexec::set_stopped(static_cast<_Rcvr&&>(__o->__rcvr_));
        });
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

      using __item_sender_t = decltype(stdexec::just(std::declval<power_event>()));
      using item_types      = exec::item_types<__item_sender_t>;

      power_context* __ctx_;
      watch_options  __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>, exec::libdispatch_scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };
  }  // namespace __detail

  inline auto power_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace powerx
