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

// Windows-only: wraps Cfgmgr32 device-interface notifications for
// GUID_DEVINTERFACE_VOLUME as a stdexec sequence sender driven by
// exec::windows_thread_pool. Mirrors the DA wrapper (examples/da_wrapper.hpp)
// in shape; the only material divergence is that CM callbacks run on a
// Cfgmgr32-internal thread we do not own, so we cannot block them — events
// are handed off via an MPSC queue + a drainer pool work item that performs
// the DA-style binary_semaphore handshake on the user's pool.

#ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#  define NOMINMAX
#endif
#include <windows.h>
// windows.h must come first
#include <cfgmgr32.h>
#include <initguid.h>
#include <ioevent.h>  // GUID_DEVINTERFACE_VOLUME

#ifndef GUID_DEVINTERFACE_VOLUME
DEFINE_GUID(GUID_DEVINTERFACE_VOLUME,
            0x53f5630dL,
            0xb6bf,
            0x11d0,
            0x94,
            0xf2,
            0x00,
            0xa0,
            0xc9,
            0x1e,
            0xfb,
            0x8b);
#endif

#include "approval_policy.hpp"
#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/windows/windows_thread_pool.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace velx
{
  enum class volume_event_kind
  {
    // Device-interface filter (CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE).
    // The kernel-confirmed "volume class member appeared / disappeared"
    // signal — sufficient for consumers that only need add/remove and
    // don't want a per-volume HANDLE.
    interface_arrival,
    interface_removal,

    // Device-handle filter (CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE), opt-in
    // via watch_options.watch_handle_events. Each appeared volume gets
    // its own per-device HCMNOTIFICATION + open HANDLE; the wrapper
    // closes them on interface_removal or teardown.
    handle_remove_pending,       // CM_NOTIFY_ACTION_DEVICEREMOVEPENDING
    handle_remove_complete,      // CM_NOTIFY_ACTION_DEVICEREMOVECOMPLETE
    handle_query_remove_failed,  // CM_NOTIFY_ACTION_DEVICEQUERYREMOVEFAILED
    handle_custom_event,         // CM_NOTIFY_ACTION_DEVICECUSTOMEVENT
  };

  struct volume_event
  {
    volume_event_kind kind;
    std::string       device_path;  // UTF-8 lowercased "\\?\volume{guid}"

    // Populated only for handle_custom_event — the GUID identifying the
    // vendor-specific event (e.g. GUID_IO_VOLUME_MOUNT,
    // GUID_IO_VOLUME_DISMOUNT, GUID_IO_VOLUME_NAME_CHANGE).
    std::optional<GUID> custom_guid{};
  };

  // Per-approval-callback payload for query_remove. Same identity field
  // as volume_event so a predicate can match it against state built from
  // notification events.
  struct volume_info
  {
    std::string device_path;  // UTF-8 lowercased "\\?\volume{guid}"
  };

  // Tagged-union policy specialised for velx::volume_info — see
  // examples/approval_policy.hpp for the sync/bounded/monostate
  // semantics. Same shape as dax::approval_policy.
  using approval_policy = approval::policy<volume_info>;

  struct watch_options
  {
    // When true, every device that fires interface_arrival also gets a
    // per-device HCMNOTIFICATION (CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE)
    // registered against an open HANDLE to the volume. Required for
    // any of the handle_* event kinds (and for query_remove approval)
    // to fire. Defaults false to match v1 behavior — opt in only when
    // you actually need the per-device layer, since each device costs
    // one HCMNOTIFICATION + one open HANDLE.
    bool watch_handle_events{false};

    // Query-remove approval (CM_NOTIFY_ACTION_DEVICEQUERYREMOVE). Same
    // tagged-union shape as DA's approval fields — see
    // examples/approval_policy.hpp. Default monostate = wrapper returns
    // CR_SUCCESS (allow) without invoking any predicate, matching the
    // pre-v2 behavior of "no per-device callbacks at all".
    //
    // Only fires when watch_handle_events is true, since query-remove
    // is a handle-filter action.
    approval_policy query_remove{};
  };

  class volume_context;

  namespace detail
  {
    struct op_base;
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

  class volume_context
  {
   public:
    volume_context()  = default;
    ~volume_context() = default;

    volume_context(volume_context const &)                    = delete;
    auto operator=(volume_context const &) -> volume_context& = delete;

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
    struct op_base
    {
      virtual ~op_base() = default;
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

    inline auto wcs_to_utf8_lower(LPCWSTR w) -> std::optional<std::string>
    {
      if (!w)
        return std::nullopt;
      int const len = ::WideCharToMultiByte(CP_UTF8, 0, w, -1, nullptr, 0, nullptr, nullptr);
      if (len <= 0)
        return std::nullopt;
      std::string s(static_cast<std::size_t>(len - 1), '\0');
      if (::WideCharToMultiByte(CP_UTF8, 0, w, -1, s.data(), len, nullptr, nullptr) <= 0)
        return std::nullopt;
      // ASCII lowercase: \\?\Volume{guid} is pure ASCII.
      for (auto& c: s)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      return s;
    }

    // Inverse of wcs_to_utf8_lower for the per-device CreateFileW path.
    // The volume path is pure ASCII so the conversion is essentially a
    // widening, but we route through MultiByteToWideChar to keep the
    // boundary handling consistent with the wcs_to_utf8 side.
    inline auto utf8_to_wcs(std::string const & s) -> std::optional<std::wstring>
    {
      int const wlen = ::MultiByteToWideChar(CP_UTF8, 0, s.c_str(), -1, nullptr, 0);
      if (wlen <= 0)
        return std::nullopt;
      std::wstring w(static_cast<std::size_t>(wlen - 1), L'\0');
      if (::MultiByteToWideChar(CP_UTF8, 0, s.c_str(), -1, w.data(), wlen) <= 0)
        return std::nullopt;
      return w;
    }

    template <class Rcvr>
    struct op : op_base
    {
      using item_sender_t   = decltype(stdexec::just(std::declval<volume_event>()));
      using next_sender_t   = exec::next_sender_of_t<Rcvr, item_sender_t>;
      using next_receiver_t = next_receiver<Rcvr>;
      using next_op_t       = stdexec::connect_result_t<next_sender_t, next_receiver_t>;

      volume_context*     ctx_;
      watch_options       opts_;
      Rcvr               rcvr_;
      TP_CALLBACK_ENVIRON env_{};
      HCMNOTIFICATION     hnotify_{nullptr};
      PTP_WORK            drainer_work_{nullptr};

      // Per-device handle registration. Populated only when
      // watch_options::watch_handle_events is true. Each entry owns one
      // open HANDLE to the volume + one HCMNOTIFICATION on the
      // CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE filter. Lives under
      // queue_mu_ so the interface-filter CM callback (arrival /
      // removal) and the handle-filter CM callback (which looks up by
      // hNotify) cannot race on the map shape.
      struct device_reg
      {
        HANDLE          handle{INVALID_HANDLE_VALUE};
        HCMNOTIFICATION notify{nullptr};
      };

      // MPSC queue (CM thread → pool drainer).
      std::mutex                                    queue_mu_;
      std::deque<volume_event>                      queue_;
      std::unordered_set<std::string>               seen_arrivals_;  // shares queue_mu_
      std::unordered_map<std::string, device_reg> device_regs_;    // shares queue_mu_
      std::atomic<bool>                             drainer_running_{false};

      // Per-delivery handshake (drainer ↔ next_receiver). Same shape as DA.
      std::binary_semaphore delivery_done_{0};
      int                   delivery_state_{0};  // 1=value, 2=stopped, 3=error

      // Termination state (used in Task 3 for cleanup work item).
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
          // The drainer will also observe stop_requested_ on its next loop
          // iteration; if it is currently mid-delivery the in-flight set_next
          // chain shares the receiver's env (and thus its stop_token) and
          // will propagate stop, releasing the semaphore via
          // next_receiver::set_stopped (state==2).
        }
      };

      using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
      using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;

      PTP_WORK                         cleanup_work_{nullptr};
      std::atomic<bool>                cleanup_scheduled_{false};
      std::optional<stop_callback_t> stop_cb_;

      explicit op(volume_context* c, watch_options o, Rcvr r)
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
        // Resource cleanup is owned by this dtor (RDC pool pattern). Two
        // paths reach here:
        //   (1) the cleanup work item already ran teardown_and_complete,
        //       which CM-unregistered + waited for the drainer + cleared
        //       the active slot + completed the receiver. hnotify_ is
        //       null; this dtor only closes the work items and destroys
        //       the env.
        //   (2) start() returned set_error before the cleanup work item
        //       was wired up (CAS-fail, CreateThreadpoolWork-fail, CM
        //       register fail, enumeration fail). Whatever resources
        //       start() acquired before failing are still held; this
        //       dtor releases them.
        if (hnotify_)
        {
          CM_Unregister_Notification(hnotify_);
        }
        // Defensive: per-device handle registrations should already
        // have been torn down by the cleanup work item. Path (2) of
        // this dtor (start() failed before cleanup wired up) cannot
        // have populated device_regs_ because watch_handle_events
        // only takes effect inside the drainer, which never ran. So
        // this clear is a no-op in path (1) and a no-op in path (2);
        // it exists purely to make the lifecycle invariant explicit.
        unregister_all_device_handles();
        if (drainer_work_)
        {
          CloseThreadpoolWork(drainer_work_);
        }
        if (cleanup_work_)
        {
          CloseThreadpoolWork(cleanup_work_);
        }
        DestroyThreadpoolEnvironment(&env_);
      }

      static auto CALLBACK cm_callback(HCMNOTIFICATION,
                                         PVOID                 ctx_ptr,
                                         CM_NOTIFY_ACTION      action,
                                         PCM_NOTIFY_EVENT_DATA ev,
                                         DWORD) -> DWORD
      {
        // Filter check: we only want DEVINTERFACE arrivals/removals on
        // GUID_DEVINTERFACE_VOLUME. Anything else: ignore.
        if (ev->FilterType != CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE)
          return ERROR_SUCCESS;
        if (action != CM_NOTIFY_ACTION_DEVICEINTERFACEARRIVAL
            && action != CM_NOTIFY_ACTION_DEVICEINTERFACEREMOVAL)
          return ERROR_SUCCESS;
        if (!IsEqualGUID(ev->u.DeviceInterface.ClassGuid, GUID_DEVINTERFACE_VOLUME))
          return ERROR_SUCCESS;

        auto maybe_path = wcs_to_utf8_lower(ev->u.DeviceInterface.SymbolicLink);
        if (!maybe_path)
        {
          // Conversion failed for this event — drop it, do not fail the
          // whole stream (matches reference's WARN_MSG-and-continue policy).
          return ERROR_SUCCESS;
        }

        auto* self = static_cast<op*>(ctx_ptr);

        volume_event vev{
          .kind        = (action == CM_NOTIFY_ACTION_DEVICEINTERFACEARRIVAL)
                         ? volume_event_kind::interface_arrival
                         : volume_event_kind::interface_removal,
          .device_path = std::move(*maybe_path),
        };

        bool submit = false;
        {
          std::lock_guard lk{self->queue_mu_};
          if (vev.kind == volume_event_kind::interface_arrival)
          {
            if (!self->seen_arrivals_.insert(vev.device_path).second)
            {
              // Already known to us — drop. Closes the
              // register-vs-enumerate race; also harmless if CM ever
              // re-fires for the same device path.
              return ERROR_SUCCESS;
            }
          }
          else
          {
            self->seen_arrivals_.erase(vev.device_path);
          }
          self->queue_.push_back(std::move(vev));
          submit = !self->drainer_running_.exchange(true, std::memory_order_acq_rel);
        }
        if (submit)
        {
          SubmitThreadpoolWork(self->drainer_work_);
        }
        return ERROR_SUCCESS;
      }

      // Per-device CM callback. Fires for the
      // CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE registration the wrapper
      // creates on each volume when watch_options::watch_handle_events
      // is true.
      //
      // QUERYREMOVE returns the verdict synchronously (CR_SUCCESS =
      // allow, ERROR_CANCELLED = veto) by resolving the configured
      // approval policy. Everything else (REMOVEPENDING, REMOVECOMPLETE,
      // QUERYREMOVEFAILED, CUSTOMEVENT) gets translated into a
      // volume_event and pushed onto the same MPSC queue the
      // interface-filter callback uses, so the consumer sees one
      // ordered event stream.
      //
      // hNotify → device_path lookup is O(n) over device_regs_ — n is
      // the number of currently-tracked volumes (typically a handful),
      // so this is fine. Mirrors the OrangeDrive reference's pattern.
      static auto CALLBACK handle_callback(HCMNOTIFICATION       hnotify,
                                             PVOID                 ctx_ptr,
                                             CM_NOTIFY_ACTION      action,
                                             PCM_NOTIFY_EVENT_DATA ev,
                                             DWORD) -> DWORD
      {
        if (!ev || ev->FilterType != CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE)
          return ERROR_SUCCESS;

        auto* self = static_cast<op*>(ctx_ptr);

        // Look up the device path from the hNotify handle. Hold the
        // queue mutex only across the lookup; if we're dispatching to
        // the user's predicate or pushing to the queue, those happen
        // outside the lookup lock so an unrelated CM thread isn't
        // blocked on us.
        std::string path;
        {
          std::lock_guard lk{self->queue_mu_};
          for (auto const& [p, reg]: self->device_regs_)
          {
            if (reg.notify == hnotify)
            {
              path = p;
              break;
            }
          }
        }
        if (path.empty())
        {
          // The registration was already torn down (interface_removal
          // ran on the drainer just before this callback fired, or we
          // are in the middle of teardown). Allow the operation by
          // default for QUERYREMOVE — vetoing without context is worse
          // than allowing — and silently drop everything else.
          return ERROR_SUCCESS;
        }

        switch (action)
        {
        case CM_NOTIFY_ACTION_DEVICEQUERYREMOVE:
        {
          bool const allow = approval::resolve_verdict<volume_info>(self->opts_.query_remove,
                                                                      [&]
                                                                      {
                                                                        return volume_info{path};
                                                                      });
          return allow ? ERROR_SUCCESS : ERROR_CANCELLED;
        }
        case CM_NOTIFY_ACTION_DEVICEQUERYREMOVEFAILED:
          self->push_handle_event(
            {.kind = volume_event_kind::handle_query_remove_failed, .device_path = path});
          return ERROR_SUCCESS;
        case CM_NOTIFY_ACTION_DEVICEREMOVEPENDING:
          self->push_handle_event(
            {.kind = volume_event_kind::handle_remove_pending, .device_path = path});
          return ERROR_SUCCESS;
        case CM_NOTIFY_ACTION_DEVICEREMOVECOMPLETE:
          self->push_handle_event(
            {.kind = volume_event_kind::handle_remove_complete, .device_path = path});
          return ERROR_SUCCESS;
        case CM_NOTIFY_ACTION_DEVICECUSTOMEVENT:
          self->push_handle_event({.kind        = volume_event_kind::handle_custom_event,
                                       .device_path = path,
                                       .custom_guid = ev->u.DeviceHandle.EventGuid});
          return ERROR_SUCCESS;
        default:
          return ERROR_SUCCESS;
        }
      }

      static void CALLBACK drainer_callback(PTP_CALLBACK_INSTANCE,
                                              void* ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* self = static_cast<op*>(ctx_ptr);
        for (;;)
        {
          volume_event ev;
          {
            std::lock_guard lk{self->queue_mu_};
            if (self->stop_requested_.load(std::memory_order_acquire))
            {
              self->drainer_running_.store(false, std::memory_order_release);
              break;  // schedule_cleanup below
            }
            if (self->queue_.empty())
            {
              self->drainer_running_.store(false, std::memory_order_release);
              return;  // idle exit; CM callback re-arms us
            }
            ev = std::move(self->queue_.front());
            self->queue_.pop_front();
          }

          // Per-device handle registration is the drainer's job (not the
          // CM callback's): CreateFileW + CM_Register_Notification can
          // block, and the drainer runs on the user's pool, not on a
          // CM thread. Doing it before delivery means the consumer can
          // assume the handle layer is already armed when it sees the
          // arrival event. Best-effort — failures are silently dropped
          // (consumer can try again on next arrival), matching the
          // reference's WARN-and-continue posture.
          if (self->opts_.watch_handle_events)
          {
            if (ev.kind == volume_event_kind::interface_arrival)
              self->register_device_handle(ev.device_path);
            else if (ev.kind == volume_event_kind::interface_removal)
              self->unregister_device_handle(ev.device_path);
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
        // Reached only via the stop_requested branch above.
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

      // Push a non-arrival/removal event onto the queue (handle-filter
      // callbacks). Mirrors the queue-push half of cm_callback but
      // skips the interface-arrival dedup path. Drops on stop_requested
      // so a late handle event after teardown doesn't grow the queue.
      void push_handle_event(volume_event ev) noexcept
      {
        bool submit = false;
        {
          std::lock_guard lk{queue_mu_};
          if (stop_requested_.load(std::memory_order_acquire))
            return;
          queue_.push_back(std::move(ev));
          submit = !drainer_running_.exchange(true, std::memory_order_acq_rel);
        }
        if (submit)
          SubmitThreadpoolWork(drainer_work_);
      }

      // Open a per-volume HANDLE and register a DEVICEHANDLE-filter
      // CM notification against it. Returns true on success (the entry
      // is now in device_regs_), false on any failure (caller logs +
      // continues; mirrors the WARN-and-continue posture of the
      // OrangeDrive reference).
      //
      // Sharing flags are deliberately permissive (READ|WRITE|DELETE)
      // so the wrapper does not steal exclusive access from anything
      // else on the system. We only need a kernel handle to use as the
      // CM filter target, not actual data access.
      //
      // Must NOT be called while holding queue_mu_ —
      // CM_Register_Notification can block, and we don't want to
      // serialize that against the queue.
      auto register_device_handle(std::string const & path) noexcept -> bool
      {
        if (!opts_.watch_handle_events)
          return false;

        auto wpath = utf8_to_wcs(path);
        if (!wpath)
          return false;

        HANDLE h = ::CreateFileW(wpath->c_str(),
                                   GENERIC_READ,
                                   FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                   nullptr,
                                   OPEN_EXISTING,
                                   FILE_ATTRIBUTE_NORMAL,
                                   nullptr);
        if (h == INVALID_HANDLE_VALUE)
          return false;

        CM_NOTIFY_FILTER filter{};
        filter.cbSize                 = sizeof(filter);
        filter.FilterType             = CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE;
        filter.u.DeviceHandle.hTarget = h;

        HCMNOTIFICATION n = nullptr;
        if (CONFIGRET const cr =
              CM_Register_Notification(&filter, this, &handle_callback, &n);
            cr != CR_SUCCESS)
        {
          CloseHandle(h);
          return false;
        }

        // The fresh hNotify cannot have any in-flight callback yet, so
        // CM_Unregister_Notification on the duplicate-key path below is
        // safe to call under the lock.
        std::lock_guard lk{queue_mu_};
        auto [it, inserted] = device_regs_.try_emplace(path, device_reg{h, n});
        if (!inserted)
        {
          CM_Unregister_Notification(n);
          CloseHandle(h);
          return false;
        }
        return true;
      }

      // Drop the per-device registration for `path` (if any). Extracts
      // the entry under the lock then releases it before calling
      // CM_Unregister_Notification — that call blocks until any
      // in-flight handle callback returns, and an in-flight callback
      // tries to take queue_mu_, so we must NOT hold it across the
      // unregister.
      void unregister_device_handle(std::string const & path) noexcept
      {
        device_reg reg{};
        {
          std::lock_guard lk{queue_mu_};
          auto            it = device_regs_.find(path);
          if (it == device_regs_.end())
            return;
          reg = it->second;
          device_regs_.erase(it);
        }
        if (reg.notify)
          CM_Unregister_Notification(reg.notify);
        if (reg.handle != INVALID_HANDLE_VALUE)
          CloseHandle(reg.handle);
      }

      // Drop ALL per-device registrations. Same locking discipline as
      // unregister_device_handle: extract the snapshot under the
      // lock, release, then unregister + close. Used by the cleanup
      // work item.
      void unregister_all_device_handles() noexcept
      {
        std::vector<device_reg> regs;
        {
          std::lock_guard lk{queue_mu_};
          regs.reserve(device_regs_.size());
          for (auto& [p, r]: device_regs_)
            regs.push_back(r);
          device_regs_.clear();
        }
        for (auto& r: regs)
        {
          if (r.notify)
            CM_Unregister_Notification(r.notify);
          if (r.handle != INVALID_HANDLE_VALUE)
            CloseHandle(r.handle);
        }
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
        // (a) Drop the stop callback first so a late stop request cannot
        // re-enter teardown while we are mid-cleanup.
        stop_cb_.reset();

        // (b) Unregister the interface-filter CM notification first,
        // so no new arrivals/removals can land while we tear down the
        // per-device handle registrations. This is the unique safe
        // site: we are NOT in a CM callback frame (we are in a pool
        // work item), so "CM_Unregister_Notification cannot be called
        // from inside a CM callback" is satisfied. The OrangeDrive
        // reference needed a dedicated abandoned-thread for this; the
        // cleanup work item plays that role here.
        if (hnotify_)
        {
          CM_Unregister_Notification(hnotify_);
          hnotify_ = nullptr;
        }

        // (b2) Drop every per-device DEVICEHANDLE registration that
        // watch_handle_events accumulated. Each unregister blocks
        // until any in-flight handle callback for that hNotify
        // returns, so after this point no handle_* events can fire.
        // Closes the open volume handles too.
        unregister_all_device_handles();

        // (c) Wait for the drainer to drain. The drainer observes
        // stop_requested_ on its next loop iteration and returns; if it
        // was idle the wait is a no-op. Calling
        // WaitForThreadpoolWorkCallbacks on a *different* PTP_WORK from
        // inside another PTP_WORK callback is documented-safe.
        if (drainer_work_)
        {
          WaitForThreadpoolWorkCallbacks(drainer_work_, /*fCancelPendingCallbacks*/ FALSE);
        }

        // (d) Drainer should have reset next_op_ on every iteration; this
        // is defensive in case the drainer exited via the stop_requested
        // branch without delivering.
        next_op_.reset();

        // (e) Release the active slot.
        ctx_->active_.store(nullptr, std::memory_order_release);

        // (f) Move-out then complete. The receiver's set_stopped/set_error
        // may destroy *this* synchronously, so do not touch members afterwards.
        auto                local_rcvr = static_cast<Rcvr&&>(rcvr_);
        auto                ep         = std::move(error_);
        finish_kind const kind       = finish_kind_;

        if (kind == finish_error)
        {
          stdexec::set_error(std::move(local_rcvr), std::move(ep));
        }
        else
        {
          stdexec::set_stopped(std::move(local_rcvr));
        }
      }

      void start() & noexcept
      {
        // 1. CAS active slot.
        op_base* expected = nullptr;
        if (!ctx_->active_.compare_exchange_strong(expected, this))
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"volume_context already "
                                                                        "has an active watch"}));
          return;
        }

        // 2. Drainer work item.
        drainer_work_ = CreateThreadpoolWork(&drainer_callback, this, &env_);
        if (!drainer_work_)
        {
          DWORD const e = GetLastError();
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork"}));
          return;
        }

        // 2b. Cleanup work item.
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

        // 3. Register CM notification.
        CM_NOTIFY_FILTER filter{};
        filter.cbSize                      = sizeof(filter);
        filter.FilterType                  = CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE;
        filter.u.DeviceInterface.ClassGuid = GUID_DEVINTERFACE_VOLUME;
        if (CONFIGRET const cr =
              CM_Register_Notification(&filter, this, &cm_callback, &hnotify_);
            cr != CR_SUCCESS)
        {
          ctx_->active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"CM_Register_Notification "
                                                                        "failed (CR_"
                                                                        + std::to_string(cr)
                                                                        + ")"}));
          return;
        }

        // Initial replay: enumerate volumes that are already present and
        // synthesize arrival events for them. Dedupes against any CM
        // arrival that fired between CM register and this enumeration via
        // seen_arrivals_ — see design doc Section 5.
        for (;;)
        {
          ULONG size = 0;
          if (CONFIGRET const cr =
                CM_Get_Device_Interface_List_SizeA(&size,
                                                   const_cast<GUID*>(&GUID_DEVINTERFACE_VOLUME),
                                                   nullptr,
                                                   CM_GET_DEVICE_INTERFACE_LIST_PRESENT);
              cr != CR_SUCCESS)
          {
            // Treat enumeration failure as fatal in start(): roll back.
            // Resource cleanup (CM_Unregister, work items, env) is owned
            // by ~op; just clear active and propagate the error.
            ctx_->active_.store(nullptr, std::memory_order_release);
            stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                               std::make_exception_ptr(std::runtime_error{"CM_Get_Device_Interface_"
                                                                          "List_SizeA failed (CR_"
                                                                          + std::to_string(cr)
                                                                          + ")"}));
            return;
          }
          std::vector<char> buf(size);
          GUID              guid = GUID_DEVINTERFACE_VOLUME;
          if (CONFIGRET const cr =
                CM_Get_Device_Interface_ListA(&guid,
                                              nullptr,
                                              buf.data(),
                                              size,
                                              CM_GET_DEVICE_INTERFACE_LIST_PRESENT);
              cr == CR_BUFFER_SMALL)
          {
            // List grew between size and fetch — retry with the new size.
            continue;
          }
          else if (cr != CR_SUCCESS)
          {
            // Resource cleanup (CM_Unregister, work items, env) is owned
            // by ~op; just clear active and propagate the error.
            ctx_->active_.store(nullptr, std::memory_order_release);
            stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                               std::make_exception_ptr(std::runtime_error{"CM_Get_Device_Interface_"
                                                                          "ListA failed (CR_"
                                                                          + std::to_string(cr)
                                                                          + ")"}));
            return;
          }

          // Multi-string: NUL-separated, double-NUL terminated. Lock the
          // queue mutex once for the whole batch so the CM callback can't
          // interleave dedup decisions mid-enumeration.
          bool submit = false;
          {
            std::lock_guard lk{queue_mu_};
            char const *    p   = buf.data();
            char const *    end = buf.data() + size;
            while (p < end && *p)
            {
              std::size_t const n = std::strlen(p);
              std::string       path(p, n);
              for (auto& c: path)
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
              if (seen_arrivals_.insert(path).second)
              {
                queue_.push_back({volume_event_kind::interface_arrival, std::move(path)});
              }
              p += n + 1;
            }
            // Mark drainer_running_ true while still under the lock so a
            // CM callback firing concurrently does not double-submit.
            submit = !drainer_running_.exchange(true, std::memory_order_acq_rel);
          }
          if (submit)
          {
            SubmitThreadpoolWork(drainer_work_);
          }
          break;
        }

        // Register stop callback last. If the token is already in stop state
        // it fires synchronously here, but every resource it touches
        // (CM notification, work items, queue, drainer) is fully up. This
        // ordering is required for cleanup's WaitForThreadpoolWorkCallbacks
        // to be well-defined — see design doc Section 7.
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

      using item_sender_t = decltype(stdexec::just(std::declval<volume_event>()));
      using item_types      = exec::item_types<item_sender_t>;

      volume_context* ctx_;
      watch_options   opts_;

      template <stdexec::receiver Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>,
                                           exec::windows_thread_pool::scheduler>
      auto subscribe(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ctx_, opts_, std::move(rcvr)};
      }
    };
  }  // namespace detail

  inline auto volume_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, opts};
  }
}  // namespace velx
