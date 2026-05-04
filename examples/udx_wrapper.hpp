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

  namespace detail
  {
    struct op_base;
    template <class Rcvr>
    struct op;
    template <class Rcvr>
    struct next_receiver;
    struct watch_sender;
  }  // namespace detail

  class udev_context
  {
   public:
    udev_context()  = default;
    ~udev_context() = default;

    udev_context(udev_context const &)                    = delete;
    auto operator=(udev_context const &) -> udev_context& = delete;

    auto watch(watch_options opts = {}) -> detail::watch_sender;

   private:
    template <class Rcvr>
    friend struct detail::op;
    friend struct detail::watch_sender;

    std::atomic<detail::op_base*> active_{nullptr};
  };

  namespace detail
  {
    // RAII wrappers around libudev's reference-counted handles. Each
    // _ref/_unref API returns its argument so the ref-incrementing
    // accessors (e.g. udev_monitor_get_udev) can chain naturally if
    // ever needed; we always own a fresh ref here.
    struct udev_deleter
    {
      void operator()(::udev* p) const noexcept
      {
        if (p)
          ::udev_unref(p);
      }
    };
    using udev_ptr = std::unique_ptr<::udev, udev_deleter>;

    struct monitor_deleter
    {
      void operator()(::udev_monitor* p) const noexcept
      {
        if (p)
          ::udev_monitor_unref(p);
      }
    };
    using monitor_ptr = std::unique_ptr<::udev_monitor, monitor_deleter>;

    struct device_deleter
    {
      void operator()(::udev_device* p) const noexcept
      {
        if (p)
          ::udev_device_unref(p);
      }
    };
    using device_ptr = std::unique_ptr<::udev_device, device_deleter>;

    struct enumerate_deleter
    {
      void operator()(::udev_enumerate* p) const noexcept
      {
        if (p)
          ::udev_enumerate_unref(p);
      }
    };
    using enumerate_ptr = std::unique_ptr<::udev_enumerate, enumerate_deleter>;

    inline auto action_to_kind(char const * act) noexcept -> device_kind
    {
      if (!act)
        return device_kind::unknown;
      // udev_device_get_action returns one of: "add", "remove", "change",
      // "online", "offline", "bind", "unbind", "move". Fixed-string
      // comparison; not localized.
      if (std::strcmp(act, "add") == 0)
        return device_kind::add;
      if (std::strcmp(act, "remove") == 0)
        return device_kind::remove;
      if (std::strcmp(act, "change") == 0)
        return device_kind::change;
      if (std::strcmp(act, "online") == 0)
        return device_kind::online;
      if (std::strcmp(act, "offline") == 0)
        return device_kind::offline;
      if (std::strcmp(act, "bind") == 0)
        return device_kind::bind;
      if (std::strcmp(act, "unbind") == 0)
        return device_kind::unbind;
      if (std::strcmp(act, "move") == 0)
        return device_kind::move;
      return device_kind::unknown;
    }

    inline auto opt_string(char const * s) -> std::optional<std::string>
    {
      if (s)
        return std::string{s};
      return std::nullopt;
    }

    inline auto safe_string(char const * s) -> std::string
    {
      return s ? std::string{s} : std::string{};
    }

    inline auto build_event(::udev_device*        dev,
                              device_kind           synthesized_kind,
                              bool                  synthesized,
                              watch_options const & opts) -> device_event
    {
      device_event ev;
      ev.kind      = synthesized ? synthesized_kind
                                     : action_to_kind(::udev_device_get_action(dev));
      ev.subsystem = safe_string(::udev_device_get_subsystem(dev));
      ev.devtype   = safe_string(::udev_device_get_devtype(dev));
      ev.sysname   = safe_string(::udev_device_get_sysname(dev));
      ev.devnode   = opt_string(::udev_device_get_devnode(dev));
      ev.syspath   = opt_string(::udev_device_get_syspath(dev));

      if (opts.want_properties)
      {
        if (opts.property_keys.empty())
        {
          for (auto* e = ::udev_device_get_properties_list_entry(dev); e != nullptr;
               e       = ::udev_list_entry_get_next(e))
          {
            char const * k = ::udev_list_entry_get_name(e);
            char const * v = ::udev_list_entry_get_value(e);
            if (k)
              ev.properties.emplace_back(safe_string(k), safe_string(v));
          }
        }
        else
        {
          for (auto const & key: opts.property_keys)
          {
            char const * v = ::udev_device_get_property_value(dev, key.c_str());
            if (v)
              ev.properties.emplace_back(key, v);
          }
        }
      }
      return ev;
    }

    struct op_base
    {
      virtual ~op_base()                                               = default;
      virtual void on_poll_complete(::io_uring_cqe const &) noexcept   = 0;
      virtual void on_cancel_complete(::io_uring_cqe const &) noexcept = 0;
      virtual void on_finalize_complete() noexcept                     = 0;
    };

    template <class Rcvr>
    struct op;

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

    // Single-shot POLL_ADD on the udev_monitor netlink fd. The CQE's
    // res field carries the revents bitmask on success or a negative
    // errno on failure (-ECANCELED for explicit ASYNC_CANCEL, -EBADF
    // for fd close, etc.).
    struct poll_task
    {
      op_base*                                      outer_;
      experimental::execution::__io_uring::__context* ctx_;
      int                                             fd_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& sqe) noexcept
      {
        std::memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = IORING_OP_POLL_ADD;
        sqe.fd     = fd_;
        // poll32_events is the word-explicit member of the SQE union;
        // see linux/io_uring.h. POLLIN fits in 16 bits, so on
        // little-endian (x86_64, our only target) the byte layout is
        // identical to the legacy poll_events field.
        sqe.poll32_events = POLLIN | POLLERR | POLLHUP;
      }

      void complete(::io_uring_cqe const & cqe) noexcept
      {
        outer_->on_poll_complete(cqe);
      }
    };

    using poll_op_t = experimental::execution::__io_uring::__io_task_facade<poll_task>;

    // Cancel the in-flight POLL_ADD by user_data. Mirrors the inotify
    // wrapper's cancel_task; the only difference is the target type.
    struct cancel_task
    {
      op_base*                                      outer_;
      experimental::execution::__io_uring::__context* ctx_;
      void*                                           target_user_data_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& sqe) noexcept
      {
        std::memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = IORING_OP_ASYNC_CANCEL;
        sqe.addr   = reinterpret_cast<std::uint64_t>(target_user_data_);
      }

      void complete(::io_uring_cqe const & cqe) noexcept
      {
        outer_->on_cancel_complete(cqe);
      }
    };

    using cancel_op_t = experimental::execution::__io_uring::__io_task_facade<cancel_task>;

    // Deferred-finalize trampoline: same shape as inotify's finalize_task.
    // See its comment for why the indirection through a NOP CQE is the
    // unique safe site for tearing down the op.
    struct finalize_task
    {
      op_base*                                      outer_;
      experimental::execution::__io_uring::__context* ctx_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& sqe) noexcept
      {
        std::memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = IORING_OP_NOP;
      }

      void complete(::io_uring_cqe const &) noexcept
      {
        outer_->on_finalize_complete();
      }
    };

    using finalize_op_t = experimental::execution::__io_uring::__io_task_facade<finalize_task>;

    template <class Rcvr>
    struct op : op_base
    {
      using item_sender_t   = decltype(stdexec::just(std::declval<device_event>()));
      using next_sender_t   = exec::next_sender_of_t<Rcvr, item_sender_t>;
      using next_receiver_t = next_receiver<Rcvr>;
      using next_op_t       = stdexec::connect_result_t<next_sender_t, next_receiver_t>;

      enum class finish_kind
      {
        none,
        stopped,
        error
      };

      struct on_stop_fn
      {
        op* self_;
        void  operator()() noexcept
        {
          self_->stop_requested_.store(true, std::memory_order_release);

          if (self_->cancel_op_.has_value())
          {
            return;
          }

          // Same off-thread shadow-pointer dance as inotify: on_stop_fn
          // can fire from any thread, but only the reactor mutates
          // poll_op_. Read the in-flight POLL's user_data atomically
          // and submit a cancel for it.
          auto* tgt = self_->poll_user_data_.load(std::memory_order_acquire);
          if (tgt == nullptr)
          {
            return;
          }
          self_->pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
          self_->cancel_op_.emplace(std::in_place,
                                        cancel_task{static_cast<op_base*>(self_),
                                                      self_->ring_,
                                                      tgt});
          self_->cancel_op_->start();
        }
      };

      using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
      using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;

      udev_context*                                   ctx_;
      watch_options                                   opts_;
      Rcvr                                           rcvr_;
      experimental::execution::__io_uring::__context* ring_;
      udev_ptr                                      udev_;
      monitor_ptr                                   monitor_;
      int                                             mon_fd_{-1};

      // Single deque feeds both initial-enumerate-synthesized events and
      // events drained from POLL_ADD CQEs. The drainer pops one and
      // delivers via set_next; on set_value, drains again or arms POLL.
      std::deque<device_event> pending_;

      std::optional<poll_op_t>       poll_op_;
      std::optional<cancel_op_t>     cancel_op_;
      std::optional<finalize_op_t>   finalize_op_;
      std::unique_ptr<next_op_t>     next_op_;
      std::optional<stop_callback_t> stop_cb_;
      std::atomic<bool>                stop_requested_{false};
      std::atomic<bool>                finalize_scheduled_{false};
      // Shadow of in-flight POLL facade's task*. Published (release)
      // by arm_poll after emplace, read (acquire) by on_stop_fn
      // off-thread.
      std::atomic<experimental::execution::__io_uring::__task*> poll_user_data_{nullptr};
      std::atomic<int>                                          pending_cqes_{0};
      finish_kind      finish_kind_{finish_kind::none};
      std::exception_ptr error_;

      explicit op(udev_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{std::move(o)}
        , rcvr_{std::move(r)}
      {
        auto sched = stdexec::get_scheduler(stdexec::get_env(rcvr_));
        ring_      = sched.__context_;
      }

      void start() & noexcept
      {
        op_base* expected = nullptr;
        if (!ctx_->active_.compare_exchange_strong(expected, this))
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"udev_context already "
                                                                        "has an active watch"}));
          return;
        }

        try
        {
          setup_udev();
          seed_initial_replay();
        }
        catch (...)
        {
          // Setup failure: undo the CAS, complete with error before any
          // CQE has been submitted.
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_), std::current_exception());
          return;
        }

        drain_or_poll();

        // Stop callback last (matches inotify / fsevents / rdc / dax /
        // velx): if the token is already in stop state it fires
        // synchronously, which is now safe because we're either past
        // a set_next that's already in flight, or armed on POLL.
        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});
      }

      void setup_udev()
      {
        udev_.reset(::udev_new());
        if (!udev_)
        {
          throw std::runtime_error{"udev_new failed"};
        }
        monitor_.reset(::udev_monitor_new_from_netlink(udev_.get(), "udev"));
        if (!monitor_)
        {
          throw std::runtime_error{"udev_monitor_new_from_netlink failed"};
        }
        char const * devtype = opts_.devtype ? opts_.devtype->c_str() : nullptr;
        int          rc      = ::udev_monitor_filter_add_match_subsystem_devtype(monitor_.get(),
                                                                     opts_.subsystem.c_str(),
                                                                     devtype);
        if (rc < 0)
        {
          throw std::system_error{-rc,
                                  std::system_category(),
                                  "udev_monitor_filter_add_match_subsystem_devtype"};
        }
        rc = ::udev_monitor_enable_receiving(monitor_.get());
        if (rc < 0)
        {
          throw std::system_error{-rc, std::system_category(), "udev_monitor_enable_receiving"};
        }
        mon_fd_ = ::udev_monitor_get_fd(monitor_.get());
        if (mon_fd_ < 0)
        {
          throw std::runtime_error{"udev_monitor_get_fd returned invalid fd"};
        }
      }

      void seed_initial_replay()
      {
        if (!opts_.initial_replay)
        {
          return;
        }
        enumerate_ptr enumeration{::udev_enumerate_new(udev_.get())};
        if (!enumeration)
        {
          throw std::runtime_error{"udev_enumerate_new failed"};
        }
        int rc = ::udev_enumerate_add_match_subsystem(enumeration.get(), opts_.subsystem.c_str());
        if (rc < 0)
        {
          throw std::system_error{-rc,
                                  std::system_category(),
                                  "udev_enumerate_add_match_subsystem"};
        }
        rc = ::udev_enumerate_scan_devices(enumeration.get());
        if (rc < 0)
        {
          throw std::system_error{-rc, std::system_category(), "udev_enumerate_scan_devices"};
        }
        for (auto* e = ::udev_enumerate_get_list_entry(enumeration.get()); e != nullptr;
             e       = ::udev_list_entry_get_next(e))
        {
          char const * syspath = ::udev_list_entry_get_name(e);
          if (!syspath)
            continue;
          device_ptr dev{::udev_device_new_from_syspath(udev_.get(), syspath)};
          if (!dev)
            continue;
          // devtype filter (libudev's enumerate_add_match_subsystem
          // does not also filter on devtype). Skip non-matching devices
          // explicitly so initial replay agrees with the live filter.
          if (opts_.devtype)
          {
            char const * dt = ::udev_device_get_devtype(dev.get());
            if (!dt || *opts_.devtype != dt)
              continue;
          }
          pending_.push_back(build_event(dev.get(),
                                             device_kind::add,
                                             /*synthesized=*/true,
                                             opts_));
        }
      }

      // The drainer state machine: pending non-empty → deliver one
      // and let next_receiver::set_value re-invoke us; pending empty →
      // arm POLL_ADD and let the CQE handler push more then re-invoke.
      void drain_or_poll() noexcept
      {
        if (stop_requested_.load(std::memory_order_acquire))
        {
          request_finalize(finish_kind::stopped);
          return;
        }
        if (pending_.empty())
        {
          arm_poll();
          return;
        }
        deliver_front();
      }

      void deliver_front() noexcept
      {
        device_event ev = std::move(pending_.front());
        pending_.pop_front();
        try
        {
          next_op_.reset(new next_op_t(
            stdexec::connect(exec::set_next(rcvr_, stdexec::just(std::move(ev))),
                             next_receiver_t{this})));
          stdexec::start(*next_op_);
        }
        catch (...)
        {
          error_ = std::current_exception();
          request_finalize(finish_kind::error);
        }
      }

      void arm_poll() noexcept
      {
        pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
        poll_op_.emplace(std::in_place,
                           poll_task{static_cast<op_base*>(this), ring_, mon_fd_});
        auto* tgt = static_cast<experimental::execution::__io_uring::__task*>(&*poll_op_);
        poll_user_data_.store(tgt, std::memory_order_release);
        poll_op_->start();
      }

      void request_finalize(finish_kind k) noexcept
      {
        bool expected = false;
        if (!finalize_scheduled_.compare_exchange_strong(expected,
                                                           true,
                                                           std::memory_order_acq_rel))
        {
          return;
        }
        finish_kind_ = k;
        pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
        finalize_op_.emplace(std::in_place,
                               finalize_task{static_cast<op_base*>(this), ring_});
        finalize_op_->start();
      }

      void on_poll_complete(::io_uring_cqe const & cqe) noexcept override
      {
        poll_user_data_.store(nullptr, std::memory_order_release);

        if (cqe.res < 0)
        {
          if (cqe.res == -ECANCELED || stop_requested_.load(std::memory_order_acquire))
          {
            request_finalize(finish_kind::stopped);
          }
          else
          {
            error_ = std::make_exception_ptr(
              std::system_error{-cqe.res, std::system_category(), "udev_monitor poll"});
            request_finalize(finish_kind::error);
          }
          pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
          return;
        }

        // POLLERR / POLLHUP arrive in cqe.res as event-mask bits when
        // POLL_ADD fires. Treat these as fatal — the netlink socket
        // is gone or in error state, no point continuing.
        if ((cqe.res & (POLLERR | POLLHUP)) && !(cqe.res & POLLIN))
        {
          error_ = std::make_exception_ptr(std::runtime_error{"udev_monitor netlink socket "
                                                                "POLLERR/POLLHUP"});
          request_finalize(finish_kind::error);
          pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
          return;
        }

        // Drain everything currently readable. udev_monitor_receive_device
        // returns NULL on EAGAIN OR when libudev's internal filter rejects
        // a message — both are normal. Loop until NULL.
        try
        {
          while (true)
          {
            device_ptr dev{::udev_monitor_receive_device(monitor_.get())};
            if (!dev)
              break;
            pending_.push_back(build_event(dev.get(),
                                               device_kind::unknown,
                                               /*synthesized=*/false,
                                               opts_));
          }
        }
        catch (...)
        {
          error_ = std::current_exception();
          request_finalize(finish_kind::error);
          pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
          return;
        }

        pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
        drain_or_poll();
      }

      void on_cancel_complete(::io_uring_cqe const &) noexcept override
      {
        pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
      }

      void on_finalize_complete() noexcept override
      {
        if (pending_cqes_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
          finalize_and_complete();
        }
      }

      void on_next_value() noexcept
      {
        if (stop_requested_.load(std::memory_order_acquire))
        {
          request_finalize(finish_kind::stopped);
          return;
        }
        drain_or_poll();
      }

      void on_next_stopped() noexcept
      {
        request_finalize(finish_kind::stopped);
      }

      void on_next_error(std::exception_ptr ep) noexcept
      {
        error_ = std::move(ep);
        request_finalize(finish_kind::error);
      }

      void finalize_and_complete() noexcept
      {
        stop_cb_.reset();
        next_op_.reset();
        cancel_op_.reset();
        poll_op_.reset();
        finalize_op_.reset();
        // monitor_unref closes the netlink fd; udev_unref drops the
        // udev*. Order does not matter (each holds its own refcount).
        monitor_.reset();
        udev_.reset();
        ctx_->active_.store(nullptr, std::memory_order_release);

        if (finish_kind_ == finish_kind::error)
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_), std::move(error_));
        }
        else
        {
          stdexec::set_stopped(static_cast<Rcvr&&>(rcvr_));
        }
      }
    };

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

      using item_sender_t = decltype(stdexec::just(std::declval<device_event>()));
      using item_types      = exec::item_types<item_sender_t>;

      udev_context* ctx_;
      watch_options opts_;

      template <stdexec::receiver Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>, exec::io_uring_scheduler>
      auto subscribe(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ctx_, opts_, std::move(rcvr)};
      }
    };
  }  // namespace detail

  inline auto udev_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, std::move(opts)};
  }
}  // namespace udx
