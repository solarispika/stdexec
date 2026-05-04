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

// Linux-only: wraps libudev's monitor on the kernel netlink uevent
// stream as a stdexec sequence sender driven by exec::io_uring_context
// (IORING_OP_POLL_ADD on the udev_monitor netlink fd).
//
// Device-level Linux companion to dax (DiskArbitration on macOS) and
// velx (CM_Register_Notification on Windows). Same four invariants:
// single-active subscription per context (CAS-guarded), per-event
// delivery of a flat device_event, env-injected scheduler, native
// cancellation routed through stop_callback. Approval is structurally
// absent because the kernel uevent layer is broadcast-only.

#include <libudev.h>
#include <poll.h>
#include <unistd.h>

#include "exec/linux/io_uring_context.hpp"
#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace udx
{
  enum class device_kind
  {
    add,
    remove,
    change,
    online,
    offline,
    bind,
    unbind,
    move,
    unknown
  };

  struct device_event
  {
    device_kind                kind;
    std::string                subsystem;
    std::string                devtype;
    std::string                sysname;
    std::optional<std::string> devnode;
    std::optional<std::string> syspath;
    // Populated only when watch_options::want_properties is true.
    std::vector<std::pair<std::string, std::string>> properties;
  };

  struct watch_options
  {
    // udev_monitor_filter_add_match_subsystem_devtype: subsystem string
    // (e.g. "block", "usb"). The "block" default matches the device-level
    // storage scope of dax / velx. To monitor a different subsystem,
    // change this; to monitor multiple subsystems concurrently, open
    // multiple udev_context (intentional v1 limitation).
    std::string subsystem = "block";
    // Optional devtype filter. nullopt means "match any devtype" — for
    // subsystem="block" this means both "disk" (whole device) and
    // "partition" surface.
    std::optional<std::string> devtype = std::nullopt;
    // Synthesize an `add` event per currently-present matching device on
    // subscribe (via udev_enumerate). Mirrors velx's CM_Get_Device_Interface_List_PRESENT
    // and DA daemon's per-disk replay.
    bool initial_replay = true;
    // If true, populate device_event::properties from
    // udev_device_get_properties_list_entry. If `property_keys` is empty
    // and want_properties is true, all properties are copied; otherwise
    // only the named keys.
    bool                     want_properties = false;
    std::vector<std::string> property_keys;
  };

  class udev_context;

  namespace __detail
  {
    struct __op_base;
    template <class _Rcvr>
    struct __op;
    template <class _Rcvr>
    struct __next_receiver;
    struct __watch_sender;
  }  // namespace __detail

  class udev_context
  {
   public:
    udev_context()  = default;
    ~udev_context() = default;

    udev_context(udev_context const &)                    = delete;
    auto operator=(udev_context const &) -> udev_context& = delete;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
    friend struct __detail::__watch_sender;

    std::atomic<__detail::__op_base*> __active_{nullptr};
  };

  namespace __detail
  {
    // RAII wrappers around libudev's reference-counted handles. Each
    // _ref/_unref API returns its argument so the ref-incrementing
    // accessors (e.g. udev_monitor_get_udev) can chain naturally if
    // ever needed; we always own a fresh ref here.
    struct __udev_deleter
    {
      void operator()(::udev* __p) const noexcept
      {
        if (__p)
          ::udev_unref(__p);
      }
    };
    using __udev_ptr = std::unique_ptr<::udev, __udev_deleter>;

    struct __monitor_deleter
    {
      void operator()(::udev_monitor* __p) const noexcept
      {
        if (__p)
          ::udev_monitor_unref(__p);
      }
    };
    using __monitor_ptr = std::unique_ptr<::udev_monitor, __monitor_deleter>;

    struct __device_deleter
    {
      void operator()(::udev_device* __p) const noexcept
      {
        if (__p)
          ::udev_device_unref(__p);
      }
    };
    using __device_ptr = std::unique_ptr<::udev_device, __device_deleter>;

    struct __enumerate_deleter
    {
      void operator()(::udev_enumerate* __p) const noexcept
      {
        if (__p)
          ::udev_enumerate_unref(__p);
      }
    };
    using __enumerate_ptr = std::unique_ptr<::udev_enumerate, __enumerate_deleter>;

    inline auto __action_to_kind(char const * __act) noexcept -> device_kind
    {
      if (!__act)
        return device_kind::unknown;
      // udev_device_get_action returns one of: "add", "remove", "change",
      // "online", "offline", "bind", "unbind", "move". Fixed-string
      // comparison; not localized.
      if (std::strcmp(__act, "add") == 0)
        return device_kind::add;
      if (std::strcmp(__act, "remove") == 0)
        return device_kind::remove;
      if (std::strcmp(__act, "change") == 0)
        return device_kind::change;
      if (std::strcmp(__act, "online") == 0)
        return device_kind::online;
      if (std::strcmp(__act, "offline") == 0)
        return device_kind::offline;
      if (std::strcmp(__act, "bind") == 0)
        return device_kind::bind;
      if (std::strcmp(__act, "unbind") == 0)
        return device_kind::unbind;
      if (std::strcmp(__act, "move") == 0)
        return device_kind::move;
      return device_kind::unknown;
    }

    inline auto __opt_string(char const * __s) -> std::optional<std::string>
    {
      if (__s)
        return std::string{__s};
      return std::nullopt;
    }

    inline auto __safe_string(char const * __s) -> std::string
    {
      return __s ? std::string{__s} : std::string{};
    }

    inline auto __build_event(::udev_device*        __dev,
                              device_kind           __synthesized_kind,
                              bool                  __synthesized,
                              watch_options const & __opts) -> device_event
    {
      device_event __ev;
      __ev.kind      = __synthesized ? __synthesized_kind
                                     : __action_to_kind(::udev_device_get_action(__dev));
      __ev.subsystem = __safe_string(::udev_device_get_subsystem(__dev));
      __ev.devtype   = __safe_string(::udev_device_get_devtype(__dev));
      __ev.sysname   = __safe_string(::udev_device_get_sysname(__dev));
      __ev.devnode   = __opt_string(::udev_device_get_devnode(__dev));
      __ev.syspath   = __opt_string(::udev_device_get_syspath(__dev));

      if (__opts.want_properties)
      {
        if (__opts.property_keys.empty())
        {
          for (auto* __e = ::udev_device_get_properties_list_entry(__dev); __e != nullptr;
               __e       = ::udev_list_entry_get_next(__e))
          {
            char const * __k = ::udev_list_entry_get_name(__e);
            char const * __v = ::udev_list_entry_get_value(__e);
            if (__k)
              __ev.properties.emplace_back(__safe_string(__k), __safe_string(__v));
          }
        }
        else
        {
          for (auto const & __key: __opts.property_keys)
          {
            char const * __v = ::udev_device_get_property_value(__dev, __key.c_str());
            if (__v)
              __ev.properties.emplace_back(__key, __v);
          }
        }
      }
      return __ev;
    }

    struct __op_base
    {
      virtual ~__op_base()                                               = default;
      virtual void __on_poll_complete(::io_uring_cqe const &) noexcept   = 0;
      virtual void __on_cancel_complete(::io_uring_cqe const &) noexcept = 0;
      virtual void __on_finalize_complete() noexcept                     = 0;
    };

    template <class _Rcvr>
    struct __op;

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

    // Single-shot POLL_ADD on the udev_monitor netlink fd. The CQE's
    // res field carries the revents bitmask on success or a negative
    // errno on failure (-ECANCELED for explicit ASYNC_CANCEL, -EBADF
    // for fd close, etc.).
    struct __poll_task
    {
      __op_base*                                      __outer_;
      experimental::execution::__io_uring::__context* __ctx_;
      int                                             __fd_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *__ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& __sqe) noexcept
      {
        std::memset(&__sqe, 0, sizeof(__sqe));
        __sqe.opcode = IORING_OP_POLL_ADD;
        __sqe.fd     = __fd_;
        // poll32_events is the word-explicit member of the SQE union;
        // see linux/io_uring.h. POLLIN fits in 16 bits, so on
        // little-endian (x86_64, our only target) the byte layout is
        // identical to the legacy poll_events field.
        __sqe.poll32_events = POLLIN | POLLERR | POLLHUP;
      }

      void complete(::io_uring_cqe const & __cqe) noexcept
      {
        __outer_->__on_poll_complete(__cqe);
      }
    };

    using __poll_op_t = experimental::execution::__io_uring::__io_task_facade<__poll_task>;

    // Cancel the in-flight POLL_ADD by user_data. Mirrors the inotify
    // wrapper's __cancel_task; the only difference is the target type.
    struct __cancel_task
    {
      __op_base*                                      __outer_;
      experimental::execution::__io_uring::__context* __ctx_;
      void*                                           __target_user_data_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *__ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& __sqe) noexcept
      {
        std::memset(&__sqe, 0, sizeof(__sqe));
        __sqe.opcode = IORING_OP_ASYNC_CANCEL;
        __sqe.addr   = reinterpret_cast<std::uint64_t>(__target_user_data_);
      }

      void complete(::io_uring_cqe const & __cqe) noexcept
      {
        __outer_->__on_cancel_complete(__cqe);
      }
    };

    using __cancel_op_t = experimental::execution::__io_uring::__io_task_facade<__cancel_task>;

    // Deferred-finalize trampoline: same shape as inotify's __finalize_task.
    // See its comment for why the indirection through a NOP CQE is the
    // unique safe site for tearing down the op.
    struct __finalize_task
    {
      __op_base*                                      __outer_;
      experimental::execution::__io_uring::__context* __ctx_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *__ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& __sqe) noexcept
      {
        std::memset(&__sqe, 0, sizeof(__sqe));
        __sqe.opcode = IORING_OP_NOP;
      }

      void complete(::io_uring_cqe const &) noexcept
      {
        __outer_->__on_finalize_complete();
      }
    };

    using __finalize_op_t = experimental::execution::__io_uring::__io_task_facade<__finalize_task>;

    template <class _Rcvr>
    struct __op : __op_base
    {
      using __item_sender_t   = decltype(stdexec::just(std::declval<device_event>()));
      using __next_sender_t   = exec::next_sender_of_t<_Rcvr, __item_sender_t>;
      using __next_receiver_t = __next_receiver<_Rcvr>;
      using __next_op_t       = stdexec::connect_result_t<__next_sender_t, __next_receiver_t>;

      enum class __finish_kind
      {
        __none,
        __stopped,
        __error
      };

      struct __on_stop_fn
      {
        __op* __self_;
        void  operator()() noexcept
        {
          __self_->__stop_requested_.store(true, std::memory_order_release);

          if (__self_->__cancel_op_.has_value())
          {
            return;
          }

          // Same off-thread shadow-pointer dance as inotify: __on_stop_fn
          // can fire from any thread, but only the reactor mutates
          // __poll_op_. Read the in-flight POLL's user_data atomically
          // and submit a cancel for it.
          auto* __tgt = __self_->__poll_user_data_.load(std::memory_order_acquire);
          if (__tgt == nullptr)
          {
            return;
          }
          __self_->__pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
          __self_->__cancel_op_.emplace(std::in_place,
                                        __cancel_task{static_cast<__op_base*>(__self_),
                                                      __self_->__ring_,
                                                      __tgt});
          __self_->__cancel_op_->start();
        }
      };

      using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
      using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;

      udev_context*                                   __ctx_;
      watch_options                                   __opts_;
      _Rcvr                                           __rcvr_;
      experimental::execution::__io_uring::__context* __ring_;
      __udev_ptr                                      __udev_;
      __monitor_ptr                                   __monitor_;
      int                                             __mon_fd_{-1};

      // Single deque feeds both initial-enumerate-synthesized events and
      // events drained from POLL_ADD CQEs. The drainer pops one and
      // delivers via set_next; on set_value, drains again or arms POLL.
      std::deque<device_event> __pending_;

      std::optional<__poll_op_t>       __poll_op_;
      std::optional<__cancel_op_t>     __cancel_op_;
      std::optional<__finalize_op_t>   __finalize_op_;
      std::unique_ptr<__next_op_t>     __next_op_;
      std::optional<__stop_callback_t> __stop_cb_;
      std::atomic<bool>                __stop_requested_{false};
      std::atomic<bool>                __finalize_scheduled_{false};
      // Shadow of in-flight POLL facade's __task*. Published (release)
      // by __arm_poll after emplace, read (acquire) by __on_stop_fn
      // off-thread.
      std::atomic<experimental::execution::__io_uring::__task*> __poll_user_data_{nullptr};
      std::atomic<int>                                          __pending_cqes_{0};
      __finish_kind      __finish_kind_{__finish_kind::__none};
      std::exception_ptr __error_;

      explicit __op(udev_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{std::move(__o)}
        , __rcvr_{std::move(__r)}
      {
        auto __sched = stdexec::get_scheduler(stdexec::get_env(__rcvr_));
        __ring_      = __sched.__context_;
      }

      void start() & noexcept
      {
        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"udev_context already "
                                                                        "has an active watch"}));
          return;
        }

        try
        {
          __setup_udev();
          __seed_initial_replay();
        }
        catch (...)
        {
          // Setup failure: undo the CAS, complete with error before any
          // CQE has been submitted.
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), std::current_exception());
          return;
        }

        __drain_or_poll();

        // Stop callback last (matches inotify / fsevents / rdc / dax /
        // velx): if the token is already in stop state it fires
        // synchronously, which is now safe because we're either past
        // a set_next that's already in flight, or armed on POLL.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)), __on_stop_fn{this});
      }

      void __setup_udev()
      {
        __udev_.reset(::udev_new());
        if (!__udev_)
        {
          throw std::runtime_error{"udev_new failed"};
        }
        __monitor_.reset(::udev_monitor_new_from_netlink(__udev_.get(), "udev"));
        if (!__monitor_)
        {
          throw std::runtime_error{"udev_monitor_new_from_netlink failed"};
        }
        char const * __devtype = __opts_.devtype ? __opts_.devtype->c_str() : nullptr;
        int          __rc      = ::udev_monitor_filter_add_match_subsystem_devtype(__monitor_.get(),
                                                                     __opts_.subsystem.c_str(),
                                                                     __devtype);
        if (__rc < 0)
        {
          throw std::system_error{-__rc,
                                  std::system_category(),
                                  "udev_monitor_filter_add_match_subsystem_devtype"};
        }
        __rc = ::udev_monitor_enable_receiving(__monitor_.get());
        if (__rc < 0)
        {
          throw std::system_error{-__rc, std::system_category(), "udev_monitor_enable_receiving"};
        }
        __mon_fd_ = ::udev_monitor_get_fd(__monitor_.get());
        if (__mon_fd_ < 0)
        {
          throw std::runtime_error{"udev_monitor_get_fd returned invalid fd"};
        }
      }

      void __seed_initial_replay()
      {
        if (!__opts_.initial_replay)
        {
          return;
        }
        __enumerate_ptr __enum{::udev_enumerate_new(__udev_.get())};
        if (!__enum)
        {
          throw std::runtime_error{"udev_enumerate_new failed"};
        }
        int __rc = ::udev_enumerate_add_match_subsystem(__enum.get(), __opts_.subsystem.c_str());
        if (__rc < 0)
        {
          throw std::system_error{-__rc,
                                  std::system_category(),
                                  "udev_enumerate_add_match_subsystem"};
        }
        __rc = ::udev_enumerate_scan_devices(__enum.get());
        if (__rc < 0)
        {
          throw std::system_error{-__rc, std::system_category(), "udev_enumerate_scan_devices"};
        }
        for (auto* __e = ::udev_enumerate_get_list_entry(__enum.get()); __e != nullptr;
             __e       = ::udev_list_entry_get_next(__e))
        {
          char const * __syspath = ::udev_list_entry_get_name(__e);
          if (!__syspath)
            continue;
          __device_ptr __dev{::udev_device_new_from_syspath(__udev_.get(), __syspath)};
          if (!__dev)
            continue;
          // devtype filter (libudev's enumerate_add_match_subsystem
          // does not also filter on devtype). Skip non-matching devices
          // explicitly so initial replay agrees with the live filter.
          if (__opts_.devtype)
          {
            char const * __dt = ::udev_device_get_devtype(__dev.get());
            if (!__dt || *__opts_.devtype != __dt)
              continue;
          }
          __pending_.push_back(__build_event(__dev.get(),
                                             device_kind::add,
                                             /*synthesized=*/true,
                                             __opts_));
        }
      }

      // The drainer state machine: pending non-empty → deliver one
      // and let next_receiver::set_value re-invoke us; pending empty →
      // arm POLL_ADD and let the CQE handler push more then re-invoke.
      void __drain_or_poll() noexcept
      {
        if (__stop_requested_.load(std::memory_order_acquire))
        {
          __request_finalize(__finish_kind::__stopped);
          return;
        }
        if (__pending_.empty())
        {
          __arm_poll();
          return;
        }
        __deliver_front();
      }

      void __deliver_front() noexcept
      {
        device_event __ev = std::move(__pending_.front());
        __pending_.pop_front();
        try
        {
          __next_op_.reset(new __next_op_t(
            stdexec::connect(exec::set_next(__rcvr_, stdexec::just(std::move(__ev))),
                             __next_receiver_t{this})));
          stdexec::start(*__next_op_);
        }
        catch (...)
        {
          __error_ = std::current_exception();
          __request_finalize(__finish_kind::__error);
        }
      }

      void __arm_poll() noexcept
      {
        __pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
        __poll_op_.emplace(std::in_place,
                           __poll_task{static_cast<__op_base*>(this), __ring_, __mon_fd_});
        auto* __tgt = static_cast<experimental::execution::__io_uring::__task*>(&*__poll_op_);
        __poll_user_data_.store(__tgt, std::memory_order_release);
        __poll_op_->start();
      }

      void __request_finalize(__finish_kind __k) noexcept
      {
        bool __expected = false;
        if (!__finalize_scheduled_.compare_exchange_strong(__expected,
                                                           true,
                                                           std::memory_order_acq_rel))
        {
          return;
        }
        __finish_kind_ = __k;
        __pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
        __finalize_op_.emplace(std::in_place,
                               __finalize_task{static_cast<__op_base*>(this), __ring_});
        __finalize_op_->start();
      }

      void __on_poll_complete(::io_uring_cqe const & __cqe) noexcept override
      {
        __poll_user_data_.store(nullptr, std::memory_order_release);

        if (__cqe.res < 0)
        {
          if (__cqe.res == -ECANCELED || __stop_requested_.load(std::memory_order_acquire))
          {
            __request_finalize(__finish_kind::__stopped);
          }
          else
          {
            __error_ = std::make_exception_ptr(
              std::system_error{-__cqe.res, std::system_category(), "udev_monitor poll"});
            __request_finalize(__finish_kind::__error);
          }
          __pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
          return;
        }

        // POLLERR / POLLHUP arrive in cqe.res as event-mask bits when
        // POLL_ADD fires. Treat these as fatal — the netlink socket
        // is gone or in error state, no point continuing.
        if ((__cqe.res & (POLLERR | POLLHUP)) && !(__cqe.res & POLLIN))
        {
          __error_ = std::make_exception_ptr(std::runtime_error{"udev_monitor netlink socket "
                                                                "POLLERR/POLLHUP"});
          __request_finalize(__finish_kind::__error);
          __pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
          return;
        }

        // Drain everything currently readable. udev_monitor_receive_device
        // returns NULL on EAGAIN OR when libudev's internal filter rejects
        // a message — both are normal. Loop until NULL.
        try
        {
          while (true)
          {
            __device_ptr __dev{::udev_monitor_receive_device(__monitor_.get())};
            if (!__dev)
              break;
            __pending_.push_back(__build_event(__dev.get(),
                                               device_kind::unknown,
                                               /*synthesized=*/false,
                                               __opts_));
          }
        }
        catch (...)
        {
          __error_ = std::current_exception();
          __request_finalize(__finish_kind::__error);
          __pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
          return;
        }

        __pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
        __drain_or_poll();
      }

      void __on_cancel_complete(::io_uring_cqe const &) noexcept override
      {
        __pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
      }

      void __on_finalize_complete() noexcept override
      {
        if (__pending_cqes_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
          __finalize_and_complete();
        }
      }

      void __on_next_value() noexcept
      {
        if (__stop_requested_.load(std::memory_order_acquire))
        {
          __request_finalize(__finish_kind::__stopped);
          return;
        }
        __drain_or_poll();
      }

      void __on_next_stopped() noexcept
      {
        __request_finalize(__finish_kind::__stopped);
      }

      void __on_next_error(std::exception_ptr __ep) noexcept
      {
        __error_ = std::move(__ep);
        __request_finalize(__finish_kind::__error);
      }

      void __finalize_and_complete() noexcept
      {
        __stop_cb_.reset();
        __next_op_.reset();
        __cancel_op_.reset();
        __poll_op_.reset();
        __finalize_op_.reset();
        // monitor_unref closes the netlink fd; udev_unref drops the
        // udev*. Order does not matter (each holds its own refcount).
        __monitor_.reset();
        __udev_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);

        if (__finish_kind_ == __finish_kind::__error)
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), std::move(__error_));
        }
        else
        {
          stdexec::set_stopped(static_cast<_Rcvr&&>(__rcvr_));
        }
      }
    };

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

      using __item_sender_t = decltype(stdexec::just(std::declval<device_event>()));
      using item_types      = exec::item_types<__item_sender_t>;

      udev_context* __ctx_;
      watch_options __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>, exec::io_uring_scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };
  }  // namespace __detail

  inline auto udev_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, std::move(__opts)};
  }
}  // namespace udx
