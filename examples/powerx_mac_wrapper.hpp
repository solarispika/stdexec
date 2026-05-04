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

      struct on_stop_fn
      {
        op* self_;
        void  operator()() noexcept;
      };

      using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
      using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;

      power_context*        ctx_;
      watch_options         opts_;
      Rcvr                 rcvr_;
      dispatch_queue_t      queue_{nullptr};
      io_connect_t          power_port_{MACH_PORT_NULL};
      IONotificationPortRef notif_port_{nullptr};
      io_object_t           notifier_{IO_OBJECT_NULL};

      std::atomic<bool>                stop_requested_{false};
      std::binary_semaphore            delivery_done_{0};
      int                              delivery_state_{0};  // 1=value 2=stopped 3=error
      std::exception_ptr               error_;
      std::optional<stop_callback_t> stop_cb_;
      std::unique_ptr<next_op_t>     next_op_;

      static auto make_internal_queue(Rcvr const & r) -> dispatch_queue_t
      {
        auto sch  = stdexec::get_scheduler(stdexec::get_env(r));
        auto attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL,
                                                              QOS_CLASS_UNSPECIFIED,
                                                              0);
        return dispatch_queue_create_with_target("powerx.session", attr, sch.native_handle());
      }

      explicit op(power_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{o}
        , rcvr_{std::move(r)}
        , queue_{make_internal_queue(rcvr_)}
      {}

      ~op() override
      {
        if (queue_)
          dispatch_release(queue_);
      }

      // IOKit C callback. Runs on queue_ because we bound the notification
      // port via IONotificationPortSetDispatchQueue at the end of start().
      static void on_power_event_cb(void* ctx,
                                      io_service_t /*service*/,
                                      natural_t message_type,
                                      void*     message_argument) noexcept
      {
        auto* self = static_cast<op*>(ctx);

        switch (message_type)
        {
        case kIOMessageSystemWillSleep:
          // IOKit gives us ~30s to allow/cancel before the system stalls.
          // Acknowledge BEFORE delivery so a slow consumer cannot stall
          // suspend. v1 does not surface the veto path; consumers see
          // fait-accompli "suspend allowed" semantics.
          ::IOAllowPowerChange(self->power_port_,
                               reinterpret_cast<intptr_t>(message_argument));
          if (self->opts_.watch_suspend)
            self->deliver({power_event_kind::suspend});
          break;

        case kIOMessageSystemHasPoweredOn:
          if (self->opts_.watch_resume)
            self->deliver({power_event_kind::resume});
          break;

        // Other IOPMLib messages (CanSystemSleep, WillPowerOn, DeviceWill*,
        // etc.) are intentionally ignored in v1.
        default:
          break;
        }
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

        power_port_ =
          IORegisterForSystemPower(this, &notif_port_, &on_power_event_cb, &notifier_);
        if (power_port_ == MACH_PORT_NULL)
        {
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"IORegisterForSystemPower "
                                                                        "failed"}));
          return;
        }

        IONotificationPortSetDispatchQueue(notif_port_, queue_);

        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});
      }

      void deliver(power_event ev) noexcept
      {
        if (stop_requested_.load(std::memory_order_acquire))
          return;

        delivery_state_ = 0;

        try
        {
          next_op_.reset(new next_op_t(
            stdexec::connect(exec::set_next(rcvr_, stdexec::just(std::move(ev))),
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
            if (o->notifier_ == IO_OBJECT_NULL)
              return;
            o->teardown_session();
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
            if (cu->o->notifier_ == IO_OBJECT_NULL)
              return;
            cu->o->teardown_session();
            stdexec::set_error(static_cast<Rcvr&&>(cu->o->rcvr_), std::move(cu->ep));
          });
      }

      void teardown_session() noexcept
      {
        if (notifier_ == IO_OBJECT_NULL)
          return;
        if (notif_port_)
          IONotificationPortSetDispatchQueue(notif_port_, nullptr);
        IODeregisterForSystemPower(&notifier_);
        notifier_ = IO_OBJECT_NULL;
        if (power_port_ != MACH_PORT_NULL)
        {
          IOServiceClose(power_port_);
          power_port_ = MACH_PORT_NULL;
        }
        if (notif_port_)
        {
          IONotificationPortDestroy(notif_port_);
          notif_port_ = nullptr;
        }
        stop_cb_.reset();
        ctx_->active_.store(nullptr, std::memory_order_release);
      }
    };

    template <class Rcvr>
    void op<Rcvr>::on_stop_fn::operator()() noexcept
    {
      self_->stop_requested_.store(true, std::memory_order_release);
      dispatch_async_f(
        self_->queue_,
        self_,
        +[](void* p) noexcept
        {
          auto* o = static_cast<op*>(p);
          if (o->notifier_ == IO_OBJECT_NULL)
            return;
          o->teardown_session();
          stdexec::set_stopped(static_cast<Rcvr&&>(o->rcvr_));
        });
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
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>, exec::libdispatch_scheduler>
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
