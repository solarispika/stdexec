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

// macOS — netx interface-change wrapper.
//
// Wraps SCDynamicStore IPv4/IPv6 global-entity change notifications as a
// stdexec sequence sender. Mirrors the DA wrapper (examples/da_wrapper.hpp)
// and the shared libdispatch-sequence-sender pattern documented in
// docs/plans/2026-05-04-powerx-netx-system-signal-senders-design.md.
// The Windows counterpart lives in examples/netx_win_wrapper.hpp; both
// define `netx::net_context` so downstream code can `#if`-select.
//
// Replaces OrangeDrive's MacNetworkDetector
// (lib/detector/mac/network-detector.cpp), which spins up its own jthread +
// CFRunLoop. SCDynamicStore binds directly to a libdispatch queue via
// SCDynamicStoreSetDispatchQueue, so no runloop / worker thread is needed.
//
// Event payload is intentionally empty — SCDynamicStore is a hint API.
// Callers re-query interface state (getifaddrs / SCNetworkInterfaceCopyAll)
// in their downstream then(). OrangeDrive-side filters (BeeDriveTap*
// adapter skipping, checksum debounce) are application policy and do NOT
// belong in this wrapper.

#if !defined(__APPLE__) || !defined(__MACH__)
#  error "netx_mac_wrapper.hpp is macOS-only; on Windows include netx_win_wrapper.hpp"
#endif

#include <CoreFoundation/CoreFoundation.h>
#include <SystemConfiguration/SystemConfiguration.h>
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

namespace netx
{
  // Hint-only event: "the IPv4/IPv6 interface table may have changed;
  // re-read it." Matches SCDynamicStore semantics and the void() callback
  // signature OrangeDrive's NetworkDetector exposes today.
  struct interface_change_event
  {};

  struct watch_options
  {
    // v1: empty. Future fields might include address-family scoping
    // (IPv4-only / IPv6-only) or a list of SCDynamicStore patterns to OR
    // together — but the current default (kSCEntNetIPv4 + kSCEntNetIPv6
    // global state) covers OrangeDrive's only use case.
  };

  class net_context;

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base()                                  = default;
      virtual void deliver(interface_change_event) noexcept = 0;
    };

    template <class _Rcvr>
    struct __op;

    template <class _Rcvr>
    struct __next_receiver;

    struct __watch_sender;
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

    // Build the {IPv4, IPv6} CFArray of global-state pattern keys passed to
    // SCDynamicStoreSetNotificationKeys. Caller owns the returned CFArrayRef
    // and must CFRelease it (SCDynamicStoreSetNotificationKeys retains an
    // internal copy).
    inline auto __make_pattern_array() -> CFArrayRef
    {
      CFStringRef  __ipv4    = SCDynamicStoreKeyCreateNetworkGlobalEntity(nullptr,
                                                                      kSCDynamicStoreDomainState,
                                                                      kSCEntNetIPv4);
      CFStringRef  __ipv6    = SCDynamicStoreKeyCreateNetworkGlobalEntity(nullptr,
                                                                      kSCDynamicStoreDomainState,
                                                                      kSCEntNetIPv6);
      void const * __vals[2] = {__ipv4, __ipv6};
      CFArrayRef   __arr     = CFArrayCreate(nullptr, __vals, 2, &kCFTypeArrayCallBacks);
      if (__ipv4)
        CFRelease(__ipv4);
      if (__ipv6)
        CFRelease(__ipv6);
      return __arr;
    }

    template <class _Rcvr>
    struct __op : __op_base
    {
      using __item_sender_t   = decltype(stdexec::just(std::declval<interface_change_event>()));
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

      net_context*      __ctx_;
      watch_options     __opts_;
      _Rcvr             __rcvr_;
      dispatch_queue_t  __queue_{nullptr};
      SCDynamicStoreRef __store_{nullptr};

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
        return dispatch_queue_create_with_target("netx.session", __attr, __sch.native_handle());
      }

      explicit __op(net_context* __c, watch_options __o, _Rcvr __r)
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

      // SCDynamicStore C callback. Runs on __queue_ once
      // SCDynamicStoreSetDispatchQueue has been called. We do not inspect
      // changedKeys (hint-only event semantics).
      static void
      __on_change_cb(SCDynamicStoreRef /*store*/, CFArrayRef /*changedKeys*/, void* __ctx) noexcept
      {
        static_cast<__op_base*>(__ctx)->deliver(interface_change_event{});
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

        SCDynamicStoreContext __sc_ctx{};
        __sc_ctx.info = static_cast<__op_base*>(this);
        __store_      = SCDynamicStoreCreate(nullptr, CFSTR("netx"), &__on_change_cb, &__sc_ctx);
        if (!__store_)
        {
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"SCDynamicStoreCreate "
                                                                        "failed"}));
          return;
        }

        CFArrayRef __pats = __make_pattern_array();
        if (!__pats)
        {
          CFRelease(__store_);
          __store_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"netx pattern "
                                                                        "CFArrayCreate failed"}));
          return;
        }
        Boolean const __ok = SCDynamicStoreSetNotificationKeys(__store_, nullptr, __pats);
        CFRelease(__pats);
        if (!__ok)
        {
          CFRelease(__store_);
          __store_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"SCDynamicStoreSetNotificat"
                                                                        "ionKeys failed"}));
          return;
        }

        // Bind the store to our serial dispatch queue. From this point on,
        // __on_change_cb fires on __queue_.
        if (!SCDynamicStoreSetDispatchQueue(__store_, __queue_))
        {
          CFRelease(__store_);
          __store_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"SCDynamicStoreSetDispatchQ"
                                                                        "ueue failed"}));
          return;
        }

        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)), __on_stop_fn{this});
      }

      void deliver(interface_change_event __ev) noexcept override
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
            if (!__o->__store_)
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
            if (!__cu->__o->__store_)
              return;
            __cu->__o->__teardown_session();
            stdexec::set_error(static_cast<_Rcvr&&>(__cu->__o->__rcvr_), std::move(__cu->__ep));
          });
      }

      void __teardown_session() noexcept
      {
        if (!__store_)
          return;
        // Detach dispatch queue first to bar new callbacks before release.
        SCDynamicStoreSetDispatchQueue(__store_, nullptr);
        CFRelease(__store_);
        __store_ = nullptr;
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
          if (!__o->__store_)
            return;  // deliver() already finished us
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

      using __item_sender_t = decltype(stdexec::just(std::declval<interface_change_event>()));
      using item_types      = exec::item_types<__item_sender_t>;

      net_context*  __ctx_;
      watch_options __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>, exec::libdispatch_scheduler>
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
