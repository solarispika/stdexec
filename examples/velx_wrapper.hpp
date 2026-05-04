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

  namespace __detail
  {
    struct __op_base;
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

  class volume_context
  {
   public:
    volume_context()  = default;
    ~volume_context() = default;

    volume_context(volume_context const &)                    = delete;
    auto operator=(volume_context const &) -> volume_context& = delete;

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
    struct __op_base
    {
      virtual ~__op_base() = default;
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

    inline auto __wcs_to_utf8_lower(LPCWSTR __w) -> std::optional<std::string>
    {
      if (!__w)
        return std::nullopt;
      int const __len = ::WideCharToMultiByte(CP_UTF8, 0, __w, -1, nullptr, 0, nullptr, nullptr);
      if (__len <= 0)
        return std::nullopt;
      std::string __s(static_cast<std::size_t>(__len - 1), '\0');
      if (::WideCharToMultiByte(CP_UTF8, 0, __w, -1, __s.data(), __len, nullptr, nullptr) <= 0)
        return std::nullopt;
      // ASCII lowercase: \\?\Volume{guid} is pure ASCII.
      for (auto& __c: __s)
        __c = static_cast<char>(std::tolower(static_cast<unsigned char>(__c)));
      return __s;
    }

    // Inverse of __wcs_to_utf8_lower for the per-device CreateFileW path.
    // The volume path is pure ASCII so the conversion is essentially a
    // widening, but we route through MultiByteToWideChar to keep the
    // boundary handling consistent with the wcs_to_utf8 side.
    inline auto __utf8_to_wcs(std::string const & __s) -> std::optional<std::wstring>
    {
      int const __wlen = ::MultiByteToWideChar(CP_UTF8, 0, __s.c_str(), -1, nullptr, 0);
      if (__wlen <= 0)
        return std::nullopt;
      std::wstring __w(static_cast<std::size_t>(__wlen - 1), L'\0');
      if (::MultiByteToWideChar(CP_UTF8, 0, __s.c_str(), -1, __w.data(), __wlen) <= 0)
        return std::nullopt;
      return __w;
    }

    template <class _Rcvr>
    struct __op : __op_base
    {
      using __item_sender_t   = decltype(stdexec::just(std::declval<volume_event>()));
      using __next_sender_t   = exec::next_sender_of_t<_Rcvr, __item_sender_t>;
      using __next_receiver_t = __next_receiver<_Rcvr>;
      using __next_op_t       = stdexec::connect_result_t<__next_sender_t, __next_receiver_t>;

      volume_context*     __ctx_;
      watch_options       __opts_;
      _Rcvr               __rcvr_;
      TP_CALLBACK_ENVIRON __env_{};
      HCMNOTIFICATION     __hnotify_{nullptr};
      PTP_WORK            __drainer_work_{nullptr};

      // Per-device handle registration. Populated only when
      // watch_options::watch_handle_events is true. Each entry owns one
      // open HANDLE to the volume + one HCMNOTIFICATION on the
      // CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE filter. Lives under
      // __queue_mu_ so the interface-filter CM callback (arrival /
      // removal) and the handle-filter CM callback (which looks up by
      // hNotify) cannot race on the map shape.
      struct __device_reg
      {
        HANDLE          __handle{INVALID_HANDLE_VALUE};
        HCMNOTIFICATION __notify{nullptr};
      };

      // MPSC queue (CM thread → pool drainer).
      std::mutex                                    __queue_mu_;
      std::deque<volume_event>                      __queue_;
      std::unordered_set<std::string>               __seen_arrivals_;  // shares __queue_mu_
      std::unordered_map<std::string, __device_reg> __device_regs_;    // shares __queue_mu_
      std::atomic<bool>                             __drainer_running_{false};

      // Per-delivery handshake (drainer ↔ next_receiver). Same shape as DA.
      std::binary_semaphore __delivery_done_{0};
      int                   __delivery_state_{0};  // 1=value, 2=stopped, 3=error

      // Termination state (used in Task 3 for cleanup work item).
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
          // The drainer will also observe __stop_requested_ on its next loop
          // iteration; if it is currently mid-delivery the in-flight set_next
          // chain shares the receiver's env (and thus its stop_token) and
          // will propagate stop, releasing the semaphore via
          // next_receiver::set_stopped (state==2).
        }
      };

      using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
      using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;

      PTP_WORK                         __cleanup_work_{nullptr};
      std::atomic<bool>                __cleanup_scheduled_{false};
      std::optional<__stop_callback_t> __stop_cb_;

      explicit __op(volume_context* __c, watch_options __o, _Rcvr __r)
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
        // Resource cleanup is owned by this dtor (RDC pool pattern). Two
        // paths reach here:
        //   (1) the cleanup work item already ran __teardown_and_complete,
        //       which CM-unregistered + waited for the drainer + cleared
        //       the active slot + completed the receiver. __hnotify_ is
        //       null; this dtor only closes the work items and destroys
        //       the env.
        //   (2) start() returned set_error before the cleanup work item
        //       was wired up (CAS-fail, CreateThreadpoolWork-fail, CM
        //       register fail, enumeration fail). Whatever resources
        //       start() acquired before failing are still held; this
        //       dtor releases them.
        if (__hnotify_)
        {
          CM_Unregister_Notification(__hnotify_);
        }
        // Defensive: per-device handle registrations should already
        // have been torn down by the cleanup work item. Path (2) of
        // this dtor (start() failed before cleanup wired up) cannot
        // have populated __device_regs_ because watch_handle_events
        // only takes effect inside the drainer, which never ran. So
        // this clear is a no-op in path (1) and a no-op in path (2);
        // it exists purely to make the lifecycle invariant explicit.
        __unregister_all_device_handles();
        if (__drainer_work_)
        {
          CloseThreadpoolWork(__drainer_work_);
        }
        if (__cleanup_work_)
        {
          CloseThreadpoolWork(__cleanup_work_);
        }
        DestroyThreadpoolEnvironment(&__env_);
      }

      static auto CALLBACK __cm_callback(HCMNOTIFICATION,
                                         PVOID                 __ctx_ptr,
                                         CM_NOTIFY_ACTION      __action,
                                         PCM_NOTIFY_EVENT_DATA __ev,
                                         DWORD) -> DWORD
      {
        // Filter check: we only want DEVINTERFACE arrivals/removals on
        // GUID_DEVINTERFACE_VOLUME. Anything else: ignore.
        if (__ev->FilterType != CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE)
          return ERROR_SUCCESS;
        if (__action != CM_NOTIFY_ACTION_DEVICEINTERFACEARRIVAL
            && __action != CM_NOTIFY_ACTION_DEVICEINTERFACEREMOVAL)
          return ERROR_SUCCESS;
        if (!IsEqualGUID(__ev->u.DeviceInterface.ClassGuid, GUID_DEVINTERFACE_VOLUME))
          return ERROR_SUCCESS;

        auto __maybe_path = __wcs_to_utf8_lower(__ev->u.DeviceInterface.SymbolicLink);
        if (!__maybe_path)
        {
          // Conversion failed for this event — drop it, do not fail the
          // whole stream (matches reference's WARN_MSG-and-continue policy).
          return ERROR_SUCCESS;
        }

        auto* __self = static_cast<__op*>(__ctx_ptr);

        volume_event __vev{
          .kind        = (__action == CM_NOTIFY_ACTION_DEVICEINTERFACEARRIVAL)
                         ? volume_event_kind::interface_arrival
                         : volume_event_kind::interface_removal,
          .device_path = std::move(*__maybe_path),
        };

        bool __submit = false;
        {
          std::lock_guard __lk{__self->__queue_mu_};
          if (__vev.kind == volume_event_kind::interface_arrival)
          {
            if (!__self->__seen_arrivals_.insert(__vev.device_path).second)
            {
              // Already known to us — drop. Closes the
              // register-vs-enumerate race; also harmless if CM ever
              // re-fires for the same device path.
              return ERROR_SUCCESS;
            }
          }
          else
          {
            __self->__seen_arrivals_.erase(__vev.device_path);
          }
          __self->__queue_.push_back(std::move(__vev));
          __submit = !__self->__drainer_running_.exchange(true, std::memory_order_acq_rel);
        }
        if (__submit)
        {
          SubmitThreadpoolWork(__self->__drainer_work_);
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
      // hNotify → device_path lookup is O(n) over __device_regs_ — n is
      // the number of currently-tracked volumes (typically a handful),
      // so this is fine. Mirrors the OrangeDrive reference's pattern.
      static auto CALLBACK __handle_callback(HCMNOTIFICATION       __hnotify,
                                             PVOID                 __ctx_ptr,
                                             CM_NOTIFY_ACTION      __action,
                                             PCM_NOTIFY_EVENT_DATA __ev,
                                             DWORD) -> DWORD
      {
        if (!__ev || __ev->FilterType != CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE)
          return ERROR_SUCCESS;

        auto* __self = static_cast<__op*>(__ctx_ptr);

        // Look up the device path from the hNotify handle. Hold the
        // queue mutex only across the lookup; if we're dispatching to
        // the user's predicate or pushing to the queue, those happen
        // outside the lookup lock so an unrelated CM thread isn't
        // blocked on us.
        std::string __path;
        {
          std::lock_guard __lk{__self->__queue_mu_};
          for (auto const& [__p, __reg]: __self->__device_regs_)
          {
            if (__reg.__notify == __hnotify)
            {
              __path = __p;
              break;
            }
          }
        }
        if (__path.empty())
        {
          // The registration was already torn down (interface_removal
          // ran on the drainer just before this callback fired, or we
          // are in the middle of teardown). Allow the operation by
          // default for QUERYREMOVE — vetoing without context is worse
          // than allowing — and silently drop everything else.
          return ERROR_SUCCESS;
        }

        switch (__action)
        {
        case CM_NOTIFY_ACTION_DEVICEQUERYREMOVE:
        {
          bool const __allow = approval::resolve_verdict<volume_info>(__self->__opts_.query_remove,
                                                                      [&]
                                                                      {
                                                                        return volume_info{__path};
                                                                      });
          return __allow ? ERROR_SUCCESS : ERROR_CANCELLED;
        }
        case CM_NOTIFY_ACTION_DEVICEQUERYREMOVEFAILED:
          __self->__push_handle_event(
            {.kind = volume_event_kind::handle_query_remove_failed, .device_path = __path});
          return ERROR_SUCCESS;
        case CM_NOTIFY_ACTION_DEVICEREMOVEPENDING:
          __self->__push_handle_event(
            {.kind = volume_event_kind::handle_remove_pending, .device_path = __path});
          return ERROR_SUCCESS;
        case CM_NOTIFY_ACTION_DEVICEREMOVECOMPLETE:
          __self->__push_handle_event(
            {.kind = volume_event_kind::handle_remove_complete, .device_path = __path});
          return ERROR_SUCCESS;
        case CM_NOTIFY_ACTION_DEVICECUSTOMEVENT:
          __self->__push_handle_event({.kind        = volume_event_kind::handle_custom_event,
                                       .device_path = __path,
                                       .custom_guid = __ev->u.DeviceHandle.EventGuid});
          return ERROR_SUCCESS;
        default:
          return ERROR_SUCCESS;
        }
      }

      static void CALLBACK __drainer_callback(PTP_CALLBACK_INSTANCE,
                                              void* __ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* __self = static_cast<__op*>(__ctx_ptr);
        for (;;)
        {
          volume_event __ev;
          {
            std::lock_guard __lk{__self->__queue_mu_};
            if (__self->__stop_requested_.load(std::memory_order_acquire))
            {
              __self->__drainer_running_.store(false, std::memory_order_release);
              break;  // schedule_cleanup below
            }
            if (__self->__queue_.empty())
            {
              __self->__drainer_running_.store(false, std::memory_order_release);
              return;  // idle exit; CM callback re-arms us
            }
            __ev = std::move(__self->__queue_.front());
            __self->__queue_.pop_front();
          }

          // Per-device handle registration is the drainer's job (not the
          // CM callback's): CreateFileW + CM_Register_Notification can
          // block, and the drainer runs on the user's pool, not on a
          // CM thread. Doing it before delivery means the consumer can
          // assume the handle layer is already armed when it sees the
          // arrival event. Best-effort — failures are silently dropped
          // (consumer can try again on next arrival), matching the
          // reference's WARN-and-continue posture.
          if (__self->__opts_.watch_handle_events)
          {
            if (__ev.kind == volume_event_kind::interface_arrival)
              __self->__register_device_handle(__ev.device_path);
            else if (__ev.kind == volume_event_kind::interface_removal)
              __self->__unregister_device_handle(__ev.device_path);
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
        // Reached only via the stop_requested branch above.
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

      // Push a non-arrival/removal event onto the queue (handle-filter
      // callbacks). Mirrors the queue-push half of __cm_callback but
      // skips the interface-arrival dedup path. Drops on stop_requested
      // so a late handle event after teardown doesn't grow the queue.
      void __push_handle_event(volume_event __ev) noexcept
      {
        bool __submit = false;
        {
          std::lock_guard __lk{__queue_mu_};
          if (__stop_requested_.load(std::memory_order_acquire))
            return;
          __queue_.push_back(std::move(__ev));
          __submit = !__drainer_running_.exchange(true, std::memory_order_acq_rel);
        }
        if (__submit)
          SubmitThreadpoolWork(__drainer_work_);
      }

      // Open a per-volume HANDLE and register a DEVICEHANDLE-filter
      // CM notification against it. Returns true on success (the entry
      // is now in __device_regs_), false on any failure (caller logs +
      // continues; mirrors the WARN-and-continue posture of the
      // OrangeDrive reference).
      //
      // Sharing flags are deliberately permissive (READ|WRITE|DELETE)
      // so the wrapper does not steal exclusive access from anything
      // else on the system. We only need a kernel handle to use as the
      // CM filter target, not actual data access.
      //
      // Must NOT be called while holding __queue_mu_ —
      // CM_Register_Notification can block, and we don't want to
      // serialize that against the queue.
      auto __register_device_handle(std::string const & __path) noexcept -> bool
      {
        if (!__opts_.watch_handle_events)
          return false;

        auto __wpath = __utf8_to_wcs(__path);
        if (!__wpath)
          return false;

        HANDLE __h = ::CreateFileW(__wpath->c_str(),
                                   GENERIC_READ,
                                   FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                   nullptr,
                                   OPEN_EXISTING,
                                   FILE_ATTRIBUTE_NORMAL,
                                   nullptr);
        if (__h == INVALID_HANDLE_VALUE)
          return false;

        CM_NOTIFY_FILTER __filter{};
        __filter.cbSize                 = sizeof(__filter);
        __filter.FilterType             = CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE;
        __filter.u.DeviceHandle.hTarget = __h;

        HCMNOTIFICATION __n = nullptr;
        if (CONFIGRET const __cr =
              CM_Register_Notification(&__filter, this, &__handle_callback, &__n);
            __cr != CR_SUCCESS)
        {
          CloseHandle(__h);
          return false;
        }

        // The fresh hNotify cannot have any in-flight callback yet, so
        // CM_Unregister_Notification on the duplicate-key path below is
        // safe to call under the lock.
        std::lock_guard __lk{__queue_mu_};
        auto [__it, __inserted] = __device_regs_.try_emplace(__path, __device_reg{__h, __n});
        if (!__inserted)
        {
          CM_Unregister_Notification(__n);
          CloseHandle(__h);
          return false;
        }
        return true;
      }

      // Drop the per-device registration for `path` (if any). Extracts
      // the entry under the lock then releases it before calling
      // CM_Unregister_Notification — that call blocks until any
      // in-flight handle callback returns, and an in-flight callback
      // tries to take __queue_mu_, so we must NOT hold it across the
      // unregister.
      void __unregister_device_handle(std::string const & __path) noexcept
      {
        __device_reg __reg{};
        {
          std::lock_guard __lk{__queue_mu_};
          auto            __it = __device_regs_.find(__path);
          if (__it == __device_regs_.end())
            return;
          __reg = __it->second;
          __device_regs_.erase(__it);
        }
        if (__reg.__notify)
          CM_Unregister_Notification(__reg.__notify);
        if (__reg.__handle != INVALID_HANDLE_VALUE)
          CloseHandle(__reg.__handle);
      }

      // Drop ALL per-device registrations. Same locking discipline as
      // __unregister_device_handle: extract the snapshot under the
      // lock, release, then unregister + close. Used by the cleanup
      // work item.
      void __unregister_all_device_handles() noexcept
      {
        std::vector<__device_reg> __regs;
        {
          std::lock_guard __lk{__queue_mu_};
          __regs.reserve(__device_regs_.size());
          for (auto& [__p, __r]: __device_regs_)
            __regs.push_back(__r);
          __device_regs_.clear();
        }
        for (auto& __r: __regs)
        {
          if (__r.__notify)
            CM_Unregister_Notification(__r.__notify);
          if (__r.__handle != INVALID_HANDLE_VALUE)
            CloseHandle(__r.__handle);
        }
      }

      static void CALLBACK __cleanup_callback(PTP_CALLBACK_INSTANCE,
                                              void* __ctx_ptr,
                                              PTP_WORK) noexcept
      {
        auto* __self = static_cast<__op*>(__ctx_ptr);
        __self->__teardown_and_complete();
      }

      void __teardown_and_complete() noexcept
      {
        // (a) Drop the stop callback first so a late stop request cannot
        // re-enter teardown while we are mid-cleanup.
        __stop_cb_.reset();

        // (b) Unregister the interface-filter CM notification first,
        // so no new arrivals/removals can land while we tear down the
        // per-device handle registrations. This is the unique safe
        // site: we are NOT in a CM callback frame (we are in a pool
        // work item), so "CM_Unregister_Notification cannot be called
        // from inside a CM callback" is satisfied. The OrangeDrive
        // reference needed a dedicated abandoned-thread for this; the
        // cleanup work item plays that role here.
        if (__hnotify_)
        {
          CM_Unregister_Notification(__hnotify_);
          __hnotify_ = nullptr;
        }

        // (b2) Drop every per-device DEVICEHANDLE registration that
        // watch_handle_events accumulated. Each unregister blocks
        // until any in-flight handle callback for that hNotify
        // returns, so after this point no handle_* events can fire.
        // Closes the open volume handles too.
        __unregister_all_device_handles();

        // (c) Wait for the drainer to drain. The drainer observes
        // __stop_requested_ on its next loop iteration and returns; if it
        // was idle the wait is a no-op. Calling
        // WaitForThreadpoolWorkCallbacks on a *different* PTP_WORK from
        // inside another PTP_WORK callback is documented-safe.
        if (__drainer_work_)
        {
          WaitForThreadpoolWorkCallbacks(__drainer_work_, /*fCancelPendingCallbacks*/ FALSE);
        }

        // (d) Drainer should have reset __next_op_ on every iteration; this
        // is defensive in case the drainer exited via the stop_requested
        // branch without delivering.
        __next_op_.reset();

        // (e) Release the active slot.
        __ctx_->__active_.store(nullptr, std::memory_order_release);

        // (f) Move-out then complete. The receiver's set_stopped/set_error
        // may destroy *this* synchronously, so do not touch members afterwards.
        auto                __local_rcvr = static_cast<_Rcvr&&>(__rcvr_);
        auto                __ep         = std::move(__error_);
        __finish_kind const __kind       = __finish_kind_;

        if (__kind == __finish_error)
        {
          stdexec::set_error(std::move(__local_rcvr), std::move(__ep));
        }
        else
        {
          stdexec::set_stopped(std::move(__local_rcvr));
        }
      }

      void start() & noexcept
      {
        // 1. CAS active slot.
        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"volume_context already "
                                                                        "has an active watch"}));
          return;
        }

        // 2. Drainer work item.
        __drainer_work_ = CreateThreadpoolWork(&__drainer_callback, this, &__env_);
        if (!__drainer_work_)
        {
          DWORD const __e = GetLastError();
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{static_cast<int>(__e),
                                                                       std::system_category(),
                                                                       "CreateThreadpoolWork"}));
          return;
        }

        // 2b. Cleanup work item.
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

        // 3. Register CM notification.
        CM_NOTIFY_FILTER __filter{};
        __filter.cbSize                      = sizeof(__filter);
        __filter.FilterType                  = CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE;
        __filter.u.DeviceInterface.ClassGuid = GUID_DEVINTERFACE_VOLUME;
        if (CONFIGRET const __cr =
              CM_Register_Notification(&__filter, this, &__cm_callback, &__hnotify_);
            __cr != CR_SUCCESS)
        {
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{"CM_Register_Notification "
                                                                        "failed (CR_"
                                                                        + std::to_string(__cr)
                                                                        + ")"}));
          return;
        }

        // Initial replay: enumerate volumes that are already present and
        // synthesize arrival events for them. Dedupes against any CM
        // arrival that fired between CM register and this enumeration via
        // __seen_arrivals_ — see design doc Section 5.
        for (;;)
        {
          ULONG __size = 0;
          if (CONFIGRET const __cr =
                CM_Get_Device_Interface_List_SizeA(&__size,
                                                   const_cast<GUID*>(&GUID_DEVINTERFACE_VOLUME),
                                                   nullptr,
                                                   CM_GET_DEVICE_INTERFACE_LIST_PRESENT);
              __cr != CR_SUCCESS)
          {
            // Treat enumeration failure as fatal in start(): roll back.
            // Resource cleanup (CM_Unregister, work items, env) is owned
            // by ~__op; just clear active and propagate the error.
            __ctx_->__active_.store(nullptr, std::memory_order_release);
            stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                               std::make_exception_ptr(std::runtime_error{"CM_Get_Device_Interface_"
                                                                          "List_SizeA failed (CR_"
                                                                          + std::to_string(__cr)
                                                                          + ")"}));
            return;
          }
          std::vector<char> __buf(__size);
          GUID              __guid = GUID_DEVINTERFACE_VOLUME;
          if (CONFIGRET const __cr =
                CM_Get_Device_Interface_ListA(&__guid,
                                              nullptr,
                                              __buf.data(),
                                              __size,
                                              CM_GET_DEVICE_INTERFACE_LIST_PRESENT);
              __cr == CR_BUFFER_SMALL)
          {
            // List grew between size and fetch — retry with the new size.
            continue;
          }
          else if (__cr != CR_SUCCESS)
          {
            // Resource cleanup (CM_Unregister, work items, env) is owned
            // by ~__op; just clear active and propagate the error.
            __ctx_->__active_.store(nullptr, std::memory_order_release);
            stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                               std::make_exception_ptr(std::runtime_error{"CM_Get_Device_Interface_"
                                                                          "ListA failed (CR_"
                                                                          + std::to_string(__cr)
                                                                          + ")"}));
            return;
          }

          // Multi-string: NUL-separated, double-NUL terminated. Lock the
          // queue mutex once for the whole batch so the CM callback can't
          // interleave dedup decisions mid-enumeration.
          bool __submit = false;
          {
            std::lock_guard __lk{__queue_mu_};
            char const *    __p   = __buf.data();
            char const *    __end = __buf.data() + __size;
            while (__p < __end && *__p)
            {
              std::size_t const __n = std::strlen(__p);
              std::string       __path(__p, __n);
              for (auto& __c: __path)
                __c = static_cast<char>(std::tolower(static_cast<unsigned char>(__c)));
              if (__seen_arrivals_.insert(__path).second)
              {
                __queue_.push_back({volume_event_kind::interface_arrival, std::move(__path)});
              }
              __p += __n + 1;
            }
            // Mark drainer_running_ true while still under the lock so a
            // CM callback firing concurrently does not double-submit.
            __submit = !__drainer_running_.exchange(true, std::memory_order_acq_rel);
          }
          if (__submit)
          {
            SubmitThreadpoolWork(__drainer_work_);
          }
          break;
        }

        // Register stop callback last. If the token is already in stop state
        // it fires synchronously here, but every resource it touches
        // (CM notification, work items, queue, drainer) is fully up. This
        // ordering is required for cleanup's WaitForThreadpoolWorkCallbacks
        // to be well-defined — see design doc Section 7.
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

      using __item_sender_t = decltype(stdexec::just(std::declval<volume_event>()));
      using item_types      = exec::item_types<__item_sender_t>;

      volume_context* __ctx_;
      watch_options   __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                           exec::windows_thread_pool::scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };
  }  // namespace __detail

  inline auto volume_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace velx
