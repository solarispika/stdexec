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
#include <initguid.h>
#include <cfgmgr32.h>
#include <ioevent.h>          // GUID_DEVINTERFACE_VOLUME

#ifndef GUID_DEVINTERFACE_VOLUME
DEFINE_GUID(GUID_DEVINTERFACE_VOLUME, 0x53f5630dL, 0xb6bf, 0x11d0, 0x94, 0xf2,
            0x00, 0xa0, 0xc9, 0x1e, 0xfb, 0x8b);
#endif

#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/windows/windows_thread_pool.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cctype>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

namespace velx
{
  enum class volume_event_kind
  {
    interface_arrival,
    interface_removal
  };

  struct volume_event
  {
    volume_event_kind kind;
    std::string       device_path;  // UTF-8 lowercased "\\?\volume{guid}"
  };

  struct watch_options
  {};  // empty in v1, reserved for v2

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

  // env-injection adapter — mirrors fsx::on_queue / dax::on_queue /
  // inx::on_ring / rdcx::pool::on_pool. Same `exec::__on_scheduler_t`
  // instance type promoted in include/exec/on_scheduler.hpp.
  inline constexpr exec::__on_scheduler_t on_pool{};

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
      for (auto & __c: __s)
        __c = static_cast<char>(std::tolower(static_cast<unsigned char>(__c)));
      return __s;
    }

    template <class _Rcvr>
    struct __op : __op_base
    {
      using __item_sender_t   = decltype(stdexec::just(std::declval<volume_event>()));
      using __next_sender_t   = exec::next_sender_of_t<_Rcvr, __item_sender_t>;
      using __next_receiver_t = __next_receiver<_Rcvr>;
      using __next_op_t       = stdexec::connect_result_t<__next_sender_t, __next_receiver_t>;

      volume_context*    __ctx_;
      watch_options      __opts_;
      _Rcvr              __rcvr_;
      TP_CALLBACK_ENVIRON __env_{};
      HCMNOTIFICATION    __hnotify_{nullptr};
      PTP_WORK           __drainer_work_{nullptr};

      // MPSC queue (CM thread → pool drainer).
      std::mutex                __queue_mu_;
      std::vector<volume_event> __queue_;
      std::atomic<bool>         __drainer_running_{false};

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
        // No-op. Resource teardown is owned by __teardown_and_complete,
        // which runs in the cleanup work item. By the time this dtor runs,
        // __teardown_and_complete has already released __hnotify_ and the
        // pool environment is the only thing left — but the work items
        // themselves were closed inside __teardown_and_complete too (see
        // the additions in this step).
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
          (__action == CM_NOTIFY_ACTION_DEVICEINTERFACEARRIVAL)
            ? volume_event_kind::interface_arrival
            : volume_event_kind::interface_removal,
          std::move(*__maybe_path),
        };

        {
          std::lock_guard __lk{__self->__queue_mu_};
          __self->__queue_.push_back(std::move(__vev));
          if (!__self->__drainer_running_.exchange(true, std::memory_order_acq_rel))
          {
            // Drainer was idle; submit one. (Task 3 will keep this same
            // submission path; the cleanup-work-item indirection lands then.)
            SubmitThreadpoolWork(__self->__drainer_work_);
          }
        }
        return ERROR_SUCCESS;
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
            __self->__queue_.erase(__self->__queue_.begin());
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
        if (!__cleanup_scheduled_.compare_exchange_strong(__expected, true,
                                                          std::memory_order_acq_rel))
          return;
        __finish_kind_ = __k;
        SubmitThreadpoolWork(__cleanup_work_);
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

        // (b) Unregister the CM notification. This is the unique safe site:
        // we are NOT in a CM callback frame (we are in a pool work item),
        // so the API contract that "CM_Unregister_Notification cannot be
        // called from inside a CM callback" is satisfied. The OrangeDrive
        // reference needed a dedicated abandoned-thread for this; the
        // cleanup work item plays that role here.
        if (__hnotify_)
        {
          CM_Unregister_Notification(__hnotify_);
          __hnotify_ = nullptr;
        }

        // (c) Wait for the drainer to drain. The drainer observes
        // __stop_requested_ on its next loop iteration and returns; if it
        // was idle the wait is a no-op. Calling
        // WaitForThreadpoolWorkCallbacks on a *different* PTP_WORK from
        // inside another PTP_WORK callback is documented-safe.
        if (__drainer_work_)
        {
          WaitForThreadpoolWorkCallbacks(__drainer_work_, /*fCancelPendingCallbacks*/ FALSE);
        }

        // (c.5) Close pool work items. CloseThreadpoolWork is documented to
        // defer the actual free until any in-flight callbacks return; we have
        // already waited for the drainer in (c), so the cleanup work item is
        // the only callback that could be in-flight, and CloseThreadpoolWork
        // on the cleanup work itself is safe to call from inside that
        // callback (it merely marks for free; the runtime drops the handle
        // after we return).
        if (__drainer_work_)
        {
          CloseThreadpoolWork(__drainer_work_);
          __drainer_work_ = nullptr;
        }
        if (__cleanup_work_)
        {
          CloseThreadpoolWork(__cleanup_work_);
          __cleanup_work_ = nullptr;
        }
        DestroyThreadpoolEnvironment(&__env_);

        // (d) Drainer should have reset __next_op_ on every iteration; this
        // is defensive in case the drainer exited via the stop_requested
        // branch without delivering.
        __next_op_.reset();

        // (e) Release the active slot.
        __ctx_->__active_.store(nullptr, std::memory_order_release);

        // (f) Move-out then complete. The receiver's set_stopped/set_error
        // may destroy *this* synchronously, so do not touch members afterwards.
        auto             __local_rcvr = static_cast<_Rcvr&&>(__rcvr_);
        auto             __ep         = std::move(__error_);
        __finish_kind const __kind    = __finish_kind_;

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
                             std::make_exception_ptr(std::runtime_error{
                               "volume_context already has an active watch"}));
          return;
        }

        // 2. Drainer work item.
        __drainer_work_ = CreateThreadpoolWork(&__drainer_callback, this, &__env_);
        if (!__drainer_work_)
        {
          DWORD const __e = GetLastError();
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{
                               static_cast<int>(__e),
                               std::system_category(),
                               "CreateThreadpoolWork"}));
          return;
        }

        // 2b. Cleanup work item.
        __cleanup_work_ = CreateThreadpoolWork(&__cleanup_callback, this, &__env_);
        if (!__cleanup_work_)
        {
          DWORD const __e = GetLastError();
          CloseThreadpoolWork(__drainer_work_);
          __drainer_work_ = nullptr;
          DestroyThreadpoolEnvironment(&__env_);
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::system_error{
                               static_cast<int>(__e),
                               std::system_category(),
                               "CreateThreadpoolWork (cleanup)"}));
          return;
        }

        // 3. Register CM notification.
        CM_NOTIFY_FILTER __filter{};
        __filter.cbSize                          = sizeof(__filter);
        __filter.FilterType                      = CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE;
        __filter.u.DeviceInterface.ClassGuid     = GUID_DEVINTERFACE_VOLUME;
        if (CONFIGRET const __cr =
              CM_Register_Notification(&__filter, this, &__cm_callback, &__hnotify_);
            __cr != CR_SUCCESS)
        {
          CloseThreadpoolWork(__cleanup_work_);
          __cleanup_work_ = nullptr;
          CloseThreadpoolWork(__drainer_work_);
          __drainer_work_ = nullptr;
          DestroyThreadpoolEnvironment(&__env_);
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{
                               "CM_Register_Notification failed (CR_" + std::to_string(__cr) + ")"}));
          return;
        }

        // (Task 4 will insert the initial replay enumeration here, between
        //  CM register and stop-callback registration.)

        // Register stop callback last. If the token is already in stop state
        // it fires synchronously here, but every resource it touches
        // (CM notification, work items, queue, drainer) is fully up. This
        // ordering is required for cleanup's WaitForThreadpoolWorkCallbacks
        // to be well-defined — see design doc Section 7.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)),
                           __on_stop_fn{this});
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
