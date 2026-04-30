# Windows volume sender (velx) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Windows-only example wrapping `CM_Register_Notification` (Cfgmgr32) with `CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE` on `GUID_DEVINTERFACE_VOLUME` as an `exec::sequence_sender_t` driven by `exec::windows_thread_pool`. Companion to `examples/da_wrapper.hpp` (macOS / DiskArbitration) on the *device-level* event source axis, and to `examples/rdc_pool_wrapper.hpp` on the *Windows / windows_thread_pool* axis.

**Architecture:** Wrapper does not own a thread pool. User supplies `exec::windows_thread_pool::scheduler` via env using `velx::on_pool` (shared `exec::__on_scheduler_t` adapter). CM callbacks fire on a Cfgmgr32-internal worker thread; they enqueue the event into an MPSC `vector` under a mutex and `SubmitThreadpoolWork` a single drainer if it is not already running. The drainer runs on the user's pool, calls `set_next(rcvr, just(volume_event))` per item, and waits on a `binary_semaphore` until the next-receiver signals — same handshake as DA's `__delivery_done_`. Cancellation routes through a stop callback that flips an atomic flag and `SubmitThreadpoolWork`s a cleanup work item; cleanup `WaitForThreadpoolWorkCallbacks(__drainer_work_, FALSE)` to drain the drainer, calls `CM_Unregister_Notification` (safely outside any CM callback frame), and completes the user receiver.

**Tech Stack:** C++20, stdexec, `exec/windows/windows_thread_pool.hpp`, Win32 `<windows.h>`, Cfgmgr32 (`<cfgmgr32.h>`, `<initguid.h>`, `<ioevent.h>` for `GUID_DEVINTERFACE_VOLUME`), Win32 thread pool API (`SubmitThreadpoolWork`, `WaitForThreadpoolWorkCallbacks`), `WideCharToMultiByte` for WCHAR→UTF-8 conversion. Link `cfgmgr32.lib`.

**Spec:** `docs/plans/2026-04-30-velx-windows-cm-sender-design.md`

---

## File map

- **Create**: `examples/velx_wrapper.hpp` — header-only `velx` namespace
- **Create**: `examples/velx.cpp` — demo
- **Create**: `examples/velx_README.md`
- **Create**: `test/exec/test_velx_wrapper.cpp` — Windows-only structural tests
- **Modify**: `examples/CMakeLists.txt:93-103` — add `example.velx` to existing `if (WIN32)` block
- **Modify**: `test/exec/CMakeLists.txt` — add `test.velx_wrapper` Windows-only target

---

## Task 1: Scaffold — types, `volume_context`, CMakeLists, smoke demo

This task lands a buildable Windows-only target that defines the public types (`volume_event`, `watch_options`, `volume_context`), declares the `__detail` namespace's wrapper machinery as forward declarations, and exposes `velx::on_pool`. No CM registration, no sender body yet. The smoke demo just constructs a `volume_context` and prints to confirm the build hookup, includes, and namespace layout are correct.

**Files:**
- Create: `examples/velx_wrapper.hpp`
- Create: `examples/velx.cpp` (smoke version)
- Modify: `examples/CMakeLists.txt:93-103` (existing `if (WIN32)` block)

### Steps

- [ ] **Step 1.1 — Create header skeleton** (`examples/velx_wrapper.hpp`)

```cpp
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
    };
  }  // namespace __detail

  inline auto volume_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace velx
```

- [ ] **Step 1.2 — Create smoke demo** (`examples/velx.cpp`)

```cpp
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

#include "velx_wrapper.hpp"

#include <cstdio>

auto main() -> int
{
  velx::volume_context __ctx;
  std::printf("velx::volume_context constructed; sizeof(volume_event)=%zu\n",
              sizeof(velx::volume_event));
  return 0;
}
```

- [ ] **Step 1.3 — Wire into `examples/CMakeLists.txt`** (lines 93-103, the `if (WIN32)` block)

Replace:
```cmake
if (WIN32)
  add_executable(example.rdc rdc.cpp)
  target_link_libraries(example.rdc
      PRIVATE STDEXEC::stdexec
              stdexec_executable_flags)

  add_executable(example.rdc_pool rdc_pool.cpp)
  target_link_libraries(example.rdc_pool
      PRIVATE STDEXEC::stdexec
              stdexec_executable_flags)
endif ()
```

with:
```cmake
if (WIN32)
  add_executable(example.rdc rdc.cpp)
  target_link_libraries(example.rdc
      PRIVATE STDEXEC::stdexec
              stdexec_executable_flags)

  add_executable(example.rdc_pool rdc_pool.cpp)
  target_link_libraries(example.rdc_pool
      PRIVATE STDEXEC::stdexec
              stdexec_executable_flags)

  add_executable(example.velx velx.cpp)
  target_link_libraries(example.velx
      PRIVATE STDEXEC::stdexec
              stdexec_executable_flags
              cfgmgr32)
endif ()
```

- [ ] **Step 1.4 — Build**

Run: `cmake --build build --target example.velx`

Expected: target builds without errors. If `build/` doesn't exist, configure first with whatever CMake invocation `build/` was previously configured with — likely `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DSTDEXEC_BUILD_EXAMPLES=ON -DSTDEXEC_ENABLE_WINDOWS_THREAD_POOL=ON`.

Common gotcha: if `<ioevent.h>` is not found, the Windows SDK might place `GUID_DEVINTERFACE_VOLUME` in a different header. Alternative include order that works on all current Windows SDK versions:

```cpp
#include <windows.h>
#include <initguid.h>
#include <devguid.h>          // many GUID_DEVINTERFACE_* live here
#include <Mountmgr.h>         // GUID_DEVINTERFACE_MOUNTDEV (related)
#include <ioevent.h>          // GUID_DEVINTERFACE_VOLUME
#include <cfgmgr32.h>
```

If still unresolved, add a manual fallback right after the includes in `velx_wrapper.hpp`:

```cpp
#ifndef GUID_DEVINTERFACE_VOLUME
DEFINE_GUID(GUID_DEVINTERFACE_VOLUME, 0x53f5630dL, 0xb6bf, 0x11d0, 0x94, 0xf2,
            0x00, 0xa0, 0xc9, 0x1e, 0xfb, 0x8b);
#endif
```

(The literal GUID is the documented public value for `GUID_DEVINTERFACE_VOLUME`.)

- [ ] **Step 1.5 — Run smoke binary**

Run: `./build/examples/example.velx.exe` (or whatever path Windows places it at — typically `build/examples/Debug/example.velx.exe` for multi-config generators).

Expected: prints `velx::volume_context constructed; sizeof(volume_event)=...` and exits 0.

- [ ] **Step 1.6 — Commit**

```bash
git add examples/velx_wrapper.hpp examples/velx.cpp examples/CMakeLists.txt
git commit -m "$(cat <<'EOF'
examples: scaffold velx::volume_context (types + CMake)

Windows-only example skeleton for the Cfgmgr32 volume sender.
Defines volume_event / volume_event_kind / watch_options /
volume_context, the on_pool env-injection instance, and the
__watch_sender shell. Wired into examples/CMakeLists.txt under the
existing if (WIN32) block, links cfgmgr32. No CM registration or
sender body yet.
EOF
)"
```

---

## Task 2: Sender wiring — `__op`, CM register, MPSC queue, drainer work item

This task lands the working watch pipeline (no cancellation yet — that's Task 3). On `start()`, `__op` initializes a per-op `TP_CALLBACK_ENVIRON` bound to the user's pool, creates the drainer `PTP_WORK`, registers the CM interface notification, and submits the drainer (which is a no-op until events arrive). The CM callback enqueues each event under the queue mutex; the drainer drains, calls `set_next(rcvr, just(volume_event))`, and waits on a `binary_semaphore` for the next-receiver to signal.

This task does **not** include cancellation, cleanup work items, initial replay, or dedup. The demo is build-only — running it without cancellation will hang waiting for CM events; verification is by `cmake --build`.

**Files:**
- Modify: `examples/velx_wrapper.hpp` (add `__op` body, `__next_receiver`, `__cm_callback`, drainer)

### Steps

- [ ] **Step 2.1 — Replace `__op_base` and add `__next_receiver` + `__op` template** (`examples/velx_wrapper.hpp`)

In the `__detail` namespace, replace the empty `__op_base` and add the rest. Insert in `namespace __detail` BEFORE `__watch_sender`:

```cpp
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
        // Task 3 will move resource teardown into the cleanup work item.
        // For Task 2 we destroy here as a fallback so the build is clean and
        // a happy-path drainer-driven exit doesn't leak. start()'s rollback
        // paths also rely on these being safe to call when the resource is
        // null.
        if (__hnotify_)
          CM_Unregister_Notification(__hnotify_);
        if (__drainer_work_)
          CloseThreadpoolWork(__drainer_work_);
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
              // Task 3 will route this to schedule_cleanup; for Task 2 we
              // just exit (no cleanup work item exists yet). The dtor will
              // tidy up when __op is destroyed.
              return;
            }
            if (__self->__queue_.empty())
            {
              __self->__drainer_running_.store(false, std::memory_order_release);
              return;
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
            __self->__error_           = std::current_exception();
            __self->__delivery_state_  = 3;
            __self->__delivery_done_.release();
          }

          __self->__delivery_done_.acquire();
          int const __state         = __self->__delivery_state_;
          __self->__next_op_.reset();

          if (__state == 2 || __state == 3)
          {
            // Task 3 will route through schedule_cleanup. Task 2 exits the
            // drainer here; the user-receiver completion ALSO lands in Task 3
            // (cleanup work item is what calls set_stopped/set_error). For
            // now the demo will appear to hang because the receiver never
            // completes — this is intentional; Task 3 is the real-runnable
            // milestone.
            return;
          }
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

        // 3. Register CM notification.
        CM_NOTIFY_FILTER __filter{};
        __filter.cbSize                          = sizeof(__filter);
        __filter.FilterType                      = CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE;
        __filter.u.DeviceInterface.ClassGuid     = GUID_DEVINTERFACE_VOLUME;
        if (CONFIGRET const __cr =
              CM_Register_Notification(&__filter, this, &__cm_callback, &__hnotify_);
            __cr != CR_SUCCESS)
        {
          CloseThreadpoolWork(__drainer_work_);
          __drainer_work_ = nullptr;
          __ctx_->__active_.store(nullptr, std::memory_order_release);
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{
                               "CM_Register_Notification failed (CR_" + std::to_string(__cr) + ")"}));
          return;
        }

        // (Task 4 will insert the initial replay enumeration here, between
        //  CM register and stop-callback registration.)

        // (Task 3 will register __stop_cb_ here.)
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
```

- [ ] **Step 2.2 — Fill `__watch_sender::subscribe`** (`examples/velx_wrapper.hpp`)

Replace the existing `__watch_sender` definition body (declared in Task 1) with one that has `subscribe`:

```cpp
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
```

- [ ] **Step 2.3 — Build only**

Run: `cmake --build build --target example.velx`

Expected: builds clean. The smoke demo from Task 1 still works (it does not call `watch()`). We deliberately do NOT replace the demo until Task 3, because without cancellation a `watch() | ignore_all_values()` pipeline would hang.

If you get a compile error about `windows_thread_pool::scheduler::native_handle()` not returning `PTP_CALLBACK_ENVIRON` — it actually returns `PTP_POOL` (or `nullptr` for the default pool), and `SetThreadpoolCallbackPool` accepts that. Cross-check against `examples/rdc_pool_wrapper.hpp:194-196` for the canonical call.

- [ ] **Step 2.4 — Run smoke binary**

Run: `./build/examples/example.velx.exe`

Expected: same output as Step 1.5 — the smoke demo does not yet invoke the watch sender.

- [ ] **Step 2.5 — Commit**

```bash
git add examples/velx_wrapper.hpp
git commit -m "$(cat <<'EOF'
examples: velx watch sender — CM register + MPSC + drainer

__op registers a CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE notification on
GUID_DEVINTERFACE_VOLUME; the CM callback enqueues volume_event under
the queue mutex and submits a single PTP_WORK drainer if idle. The
drainer runs on the user's windows_thread_pool, calls set_next per
event, and waits on a binary_semaphore — same handshake as the DA
wrapper's __delivery_done_ pair, just with the producer indirection
that DA does not need. No cancellation / cleanup work item / initial
replay yet — those land in Tasks 3 and 4.
EOF
)"
```

---

## Task 3: Cancellation + cleanup work item — stop callback, schedule_cleanup, WaitForThreadpoolWorkCallbacks

This task makes the watch cleanly cancellable and replaces the dtor-based teardown with a proper cleanup work item that serializes against the drainer via `WaitForThreadpoolWorkCallbacks`. After this task, the demo can run end-to-end (timer wins → set_stopped propagates → cleanup runs → user receiver completes).

The cleanup work item is the single completion site. All "want to finish" triggers funnel through `__schedule_cleanup` (CAS-gated on `__cleanup_scheduled_`):
- Stop callback (upstream stop_token)
- Drainer observes `next_receiver::set_stopped` (state==2)
- Drainer observes `next_receiver::set_error` (state==3)
- Drainer captures exception during connect/start

**Files:**
- Modify: `examples/velx_wrapper.hpp` (add `__on_stop_fn`, `__cleanup_work_`, `__schedule_cleanup`, `__cleanup_callback`; route drainer's terminal states through schedule_cleanup; replace dtor-based teardown)
- Modify: `examples/velx.cpp` (replace smoke demo with the real watch demo)

### Steps

- [ ] **Step 3.1 — Add stop-callback machinery + cleanup work item to `__op`** (`examples/velx_wrapper.hpp`)

Add these member definitions and types inside `struct __op` (next to the existing members and before `start()`):

```cpp
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
```

- [ ] **Step 3.2 — Add `__schedule_cleanup`, `__cleanup_callback`, `__teardown_and_complete`** (`examples/velx_wrapper.hpp`)

Add these methods inside `struct __op`, after the existing methods:

```cpp
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
```

- [ ] **Step 3.3 — Replace dtor with empty-body, route terminal states through schedule_cleanup, register `__cleanup_work_` and `__stop_cb_` in `start()`** (`examples/velx_wrapper.hpp`)

Replace the existing dtor body:

```cpp
      ~__op() override
      {
        // No-op. Resource teardown is owned by __teardown_and_complete,
        // which runs in the cleanup work item. By the time this dtor runs,
        // __teardown_and_complete has already released __hnotify_ and the
        // pool environment is the only thing left — but the work items
        // themselves were closed inside __teardown_and_complete too (see
        // the additions in this step).
      }
```

In `__teardown_and_complete`, before `(d) __next_op_.reset()`, also close the work-item handles:

```cpp
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
```

In `start()`, after `__drainer_work_ = CreateThreadpoolWork(...)` succeeds, also create the cleanup work item:

```cpp
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
```

Also extend the rollback path in the existing `CM_Register_Notification` failure branch — close the cleanup work as well, in addition to the drainer:

```cpp
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
```

At the end of `start()`, register the stop callback last (after CM is up, after work items exist) — see Spec Section 7:

```cpp
        // Register stop callback last. If the token is already in stop state
        // it fires synchronously here, but every resource it touches
        // (CM notification, work items, queue, drainer) is fully up. This
        // ordering is required for cleanup's WaitForThreadpoolWorkCallbacks
        // to be well-defined — see design doc Section 7.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)),
                           __on_stop_fn{this});
```

In `__drainer_callback`, route the terminal states through `__schedule_cleanup` instead of plain `return`:

```cpp
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
```

Replace the entire `for(;;) { ... }` body of Task 2's `__drainer_callback` with this version (it folds in the dedup-aware termination paths and the `__schedule_cleanup` routing for state==2 / state==3):

```cpp
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
```

- [ ] **Step 3.4 — Replace smoke demo with real watch demo** (`examples/velx.cpp`)

```cpp
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

#include "velx_wrapper.hpp"

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"
#include "exec/windows/windows_thread_pool.hpp"

#include <chrono>
#include <cstdio>

using namespace std::chrono_literals;

auto main() -> int
{
  std::printf("velx demo: watching volume interface arrivals/removals for 30s.\n"
              "  - Plug/unplug a USB drive to trigger events, OR\n"
              "  - Run from another terminal:  Mount-VHD / Dismount-VHD foo.vhdx (admin)\n");

  exec::windows_thread_pool __wtp{2, 4};

  velx::volume_context __ctx;

  // Timer pool drives the cancellation deadline. Using an inline `just()`
  // would race with when_any's child startup (timer fires before the
  // watch is connected). Mirrors the DA / RDC pool demos.
  exec::static_thread_pool __timer_pool{1};
  auto                     __timer_sched = __timer_pool.get_scheduler();

  stdexec::sync_wait(exec::when_any(
    stdexec::starts_on(__timer_sched, stdexec::just())
      | stdexec::then([] { std::this_thread::sleep_for(30s); }),
    velx::on_pool(__wtp.get_scheduler(), __ctx.watch())
      | exec::transform_each(stdexec::then(
          [](velx::volume_event __ev) {
            std::printf("%s %s\n",
                        __ev.kind == velx::volume_event_kind::interface_arrival
                          ? "[arrival]" : "[removal]",
                        __ev.device_path.c_str());
          }))
      | exec::ignore_all_values()));

  return 0;
}
```

- [ ] **Step 3.5 — Build**

Run: `cmake --build build --target example.velx`

Expected: builds clean.

- [ ] **Step 3.6 — Run, verify clean cancellation (no event flow)**

Run: `./build/examples/example.velx.exe`

Expected: prints the demo header text, then waits 30 seconds with no event output (assuming you do not plug anything in), then exits 0 cleanly. The 30-second timer fires `set_stopped` on the watch via `when_any`, the stop callback runs, cleanup work item drains the drainer, `CM_Unregister_Notification` runs, the user receiver gets `set_stopped`. No hangs, no leaks.

Run it 5 times in a row to check for races: a PowerShell/`cmd` loop equivalent of `for /L %i in (1,1,5) do build\examples\example.velx.exe`. All runs should exit cleanly within 30 seconds.

- [ ] **Step 3.7 — Run, verify event delivery (with manual trigger)**

Run: `./build/examples/example.velx.exe`

While it is running (within the 30-second window):
- Plug in a USB drive → expect one or more `[arrival] \\?\volume{...}` lines
- Unplug → expect a matching `[removal] \\?\volume{...}` line

Then wait for the timer; expected: clean exit within 30 seconds of start.

Manual trigger via `Mount-VHD` (admin PowerShell) is also acceptable if no USB is available:

```powershell
New-VHD -Path C:\temp\velx.vhdx -SizeBytes 64MB -Fixed -Confirm:$false
Mount-VHD -Path C:\temp\velx.vhdx
Dismount-VHD -Path C:\temp\velx.vhdx
Remove-Item C:\temp\velx.vhdx
```

- [ ] **Step 3.8 — Commit**

```bash
git add examples/velx_wrapper.hpp examples/velx.cpp
git commit -m "$(cat <<'EOF'
examples: velx cancellation + cleanup work item

All "want to finish" triggers (upstream stop_token, drainer's
next_receiver::set_stopped/set_error, drainer connect/start
exception) funnel through __schedule_cleanup, CAS-gated on
__cleanup_scheduled_. The cleanup work item runs on the user's pool,
drops __stop_cb_, calls CM_Unregister_Notification (safely outside any
CM callback frame), WaitForThreadpoolWorkCallbacks(__drainer_work_,
FALSE) to wait for the drainer to drain, then closes work items and
completes the user receiver.

start() registers __stop_cb_ last, after CM register and both work
items exist — required for cleanup's WaitForThreadpoolWorkCallbacks
to have something to wait on (see design doc Section 7).

Real watch demo replaces the smoke demo: prints arrival/removal
device paths for 30 seconds, then exits cleanly via when_any.
EOF
)"
```

---

## Task 4: Initial replay + dedup — `CM_Get_Device_Interface_List` + `__seen_arrivals_`

This task adds the synthetic `interface_arrival` events for volumes that are already present at subscribe time, mirroring DA's daemon-side initial replay. The dedup mechanism (`std::unordered_set<std::string> __seen_arrivals_`) closes the register-vs-enumerate race documented in design doc Section 5.

**Files:**
- Modify: `examples/velx_wrapper.hpp` (add `__seen_arrivals_`, dedup in CM callback, initial replay loop in `start()`)

### Steps

- [ ] **Step 4.1 — Add `__seen_arrivals_` member and integrate dedup into the CM callback** (`examples/velx_wrapper.hpp`)

In `struct __op`, add a member next to `__queue_`:

```cpp
      std::unordered_set<std::string> __seen_arrivals_;  // shares __queue_mu_
```

In `__cm_callback`, replace the existing `lock_guard + push_back + exchange` block with the dedup-aware version:

```cpp
        {
          std::lock_guard __lk{__self->__queue_mu_};
          if (__vev.kind == volume_event_kind::interface_arrival)
          {
            if (!__self->__seen_arrivals_.insert(__vev.device_path).second)
            {
              // Already known to us — drop. Closes the
              // register-vs-enumerate race AND CM's own
              // "we already saw this device on a prior arrival" idempotence.
              return ERROR_SUCCESS;
            }
          }
          else
          {
            __self->__seen_arrivals_.erase(__vev.device_path);
          }
          __self->__queue_.push_back(std::move(__vev));
          if (!__self->__drainer_running_.exchange(true, std::memory_order_acq_rel))
          {
            SubmitThreadpoolWork(__self->__drainer_work_);
          }
        }
        return ERROR_SUCCESS;
```

- [ ] **Step 4.2 — Add initial replay enumeration in `start()`** (`examples/velx_wrapper.hpp`)

After `CM_Register_Notification` succeeds and BEFORE registering the stop callback, enumerate present volumes:

```cpp
        // Initial replay: enumerate volumes that are already present and
        // synthesize arrival events for them. Dedupes against any CM
        // arrival that fired between CM register and this enumeration via
        // __seen_arrivals_ — see design doc Section 5.
        for (;;)
        {
          ULONG __size = 0;
          if (CONFIGRET const __cr =
                CM_Get_Device_Interface_List_SizeA(
                  &__size,
                  const_cast<GUID*>(&GUID_DEVINTERFACE_VOLUME),
                  nullptr,
                  CM_GET_DEVICE_INTERFACE_LIST_PRESENT);
              __cr != CR_SUCCESS)
          {
            // Treat enumeration failure as fatal in start(): roll back.
            CM_Unregister_Notification(__hnotify_);
            __hnotify_ = nullptr;
            CloseThreadpoolWork(__cleanup_work_);
            __cleanup_work_ = nullptr;
            CloseThreadpoolWork(__drainer_work_);
            __drainer_work_ = nullptr;
            DestroyThreadpoolEnvironment(&__env_);
            __ctx_->__active_.store(nullptr, std::memory_order_release);
            stdexec::set_error(
              static_cast<_Rcvr&&>(__rcvr_),
              std::make_exception_ptr(std::runtime_error{
                "CM_Get_Device_Interface_List_SizeA failed (CR_" + std::to_string(__cr) + ")"}));
            return;
          }
          std::vector<char> __buf(__size);
          GUID              __guid = GUID_DEVINTERFACE_VOLUME;
          if (CONFIGRET const __cr =
                CM_Get_Device_Interface_ListA(
                  &__guid,
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
            CM_Unregister_Notification(__hnotify_);
            __hnotify_ = nullptr;
            CloseThreadpoolWork(__cleanup_work_);
            __cleanup_work_ = nullptr;
            CloseThreadpoolWork(__drainer_work_);
            __drainer_work_ = nullptr;
            DestroyThreadpoolEnvironment(&__env_);
            __ctx_->__active_.store(nullptr, std::memory_order_release);
            stdexec::set_error(
              static_cast<_Rcvr&&>(__rcvr_),
              std::make_exception_ptr(std::runtime_error{
                "CM_Get_Device_Interface_ListA failed (CR_" + std::to_string(__cr) + ")"}));
            return;
          }

          // Multi-string: NUL-separated, double-NUL terminated. Lock the
          // queue mutex once for the whole batch so the CM callback can't
          // interleave dedup decisions mid-enumeration.
          {
            std::lock_guard __lk{__queue_mu_};
            char const * __p   = __buf.data();
            char const * __end = __buf.data() + __size;
            while (__p < __end && *__p)
            {
              std::size_t const __n = std::strlen(__p);
              std::string       __path(__p, __n);
              for (auto & __c: __path)
                __c = static_cast<char>(std::tolower(static_cast<unsigned char>(__c)));
              if (__seen_arrivals_.insert(__path).second)
              {
                __queue_.push_back({volume_event_kind::interface_arrival, std::move(__path)});
              }
              __p += __n + 1;
            }
            // Mark drainer_running_ true while still under the lock so a
            // CM callback firing concurrently does not double-submit.
            if (!__drainer_running_.exchange(true, std::memory_order_acq_rel))
            {
              SubmitThreadpoolWork(__drainer_work_);
            }
          }
          break;
        }
```

Add a `<cstring>` include at the top of `velx_wrapper.hpp` for `std::strlen` (and for `std::memset` if not already pulled in):

```cpp
#include <cstring>
```

- [ ] **Step 4.3 — Build**

Run: `cmake --build build --target example.velx`

Expected: builds clean.

- [ ] **Step 4.4 — Run, verify initial replay**

Run: `./build/examples/example.velx.exe`

Expected: within the first ~100 ms of startup, the demo prints one `[arrival] \\?\volume{...}` line for each volume currently present on the system (typically 1-5: the system volume, recovery partition, any plugged USB sticks, etc.). After that, the program waits the full 30 seconds, then exits cleanly.

If you plug in a USB drive during the run, you should see exactly one new `[arrival]` line for it (not two — the dedup prevents the CM-callback path and any future enum-loop path from double-reporting).

- [ ] **Step 4.5 — Commit**

```bash
git add examples/velx_wrapper.hpp
git commit -m "$(cat <<'EOF'
examples: velx initial replay + dedup

start() enumerates currently-present volumes via
CM_Get_Device_Interface_List(GUID_DEVINTERFACE_VOLUME,
CM_GET_DEVICE_INTERFACE_LIST_PRESENT) and pushes a synthetic
interface_arrival for each, dedupe-guarded by __seen_arrivals_ so a
volume that races between CM register and enumeration is not reported
twice. interface_removal events also remove from the set so a volume
that disappears and reappears is reported correctly.

Mirrors DA daemon's behaviour where DARegisterDiskAppearedCallback
delivers the current snapshot on register. See design doc Section 5.
EOF
)"
```

---

## Task 5: Structural tests + README

Adds the structural unit tests (compile-time scheduler env constraint, cancel-before-event, sequential subscriptions) and the user-facing README.

**Files:**
- Create: `test/exec/test_velx_wrapper.cpp`
- Modify: `test/exec/CMakeLists.txt` (add Windows-only `test.velx_wrapper` target)
- Create: `examples/velx_README.md`

### Steps

- [ ] **Step 5.1 — Create structural test** (`test/exec/test_velx_wrapper.cpp`)

```cpp
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

// Structural tests for the velx (Windows volume sender) wrapper.
// Windows-only, gated by STDEXEC_ENABLE_WINDOWS_THREAD_POOL at the CMake
// level. These tests do not trigger real volume events; they exercise
// the wrapper's lifecycle / env constraints. Mirrors test_da_wrapper.cpp.

#include "catch2/catch_all.hpp"

#include "../../examples/velx_wrapper.hpp"

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"
#include "exec/windows/windows_thread_pool.hpp"
#include "stdexec/execution.hpp"

#include <chrono>
#include <thread>

namespace
{
  using namespace std::chrono_literals;

  // Compile-time invariant: the watch sender must reject receivers whose env
  // does not expose a windows_thread_pool::scheduler. Mirrors DA's
  // libdispatch_scheduler check; this is the wall against silently routing
  // CM callbacks onto an unrelated scheduler when a user composes via
  // starts_on.
  static_assert(!exec::__env_has_scheduler<stdexec::env<>,
                                           exec::windows_thread_pool::scheduler>);

  TEST_CASE("velx::volume_context watch can be cancelled before any volume event")
  {
    exec::windows_thread_pool __wtp{2, 4};
    exec::static_thread_pool  __tp{1};
    auto                      __timer_sched = __tp.get_scheduler();

    velx::volume_context __ctx;

    // Cancel via a short timer. Using an inline `just()` would race with
    // when_any's child startup (timer fires before the watch is connected).
    // 50 ms is enough for start() to bring up CM + drainer + initial replay,
    // but short enough that the test stays fast.
    stdexec::sync_wait(exec::when_any(
      stdexec::starts_on(__timer_sched, stdexec::just())
        | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
      velx::on_pool(__wtp.get_scheduler(), __ctx.watch())
        | exec::ignore_all_values()));

    // Reaching here means the watch's __on_stop_fn ran, the cleanup work
    // item ran, the drainer drained, CM_Unregister_Notification ran, and
    // __active_ was cleared.
    SUCCEED("cancellation round-trip completed");
  }

  TEST_CASE("velx::volume_context allows sequential subscriptions after each completes")
  {
    exec::windows_thread_pool __wtp{2, 4};
    exec::static_thread_pool  __tp{1};
    auto                      __timer_sched = __tp.get_scheduler();

    velx::volume_context __ctx;

    auto __run_once = [&]
    {
      stdexec::sync_wait(exec::when_any(
        stdexec::starts_on(__timer_sched, stdexec::just())
          | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
        velx::on_pool(__wtp.get_scheduler(), __ctx.watch())
          | exec::ignore_all_values()));
    };

    __run_once();
    __run_once();

    // Reaching here means __active_ was cleared by the cleanup work item
    // after the first run, allowing the second subscribe to CAS in.
    SUCCEED("two sequential subscriptions completed");
  }
}  // namespace
```

- [ ] **Step 5.2 — Wire test into `test/exec/CMakeLists.txt`**

After the existing `if(STDEXEC_ENABLE_LIBDISPATCH) ... endif()` block (around line 112), add the Windows-only block:

```cmake
if(STDEXEC_ENABLE_WINDOWS_THREAD_POOL AND WIN32)
    # examples/velx_wrapper.hpp uses the literal `stdexec::` namespace (same
    # as examples/da_wrapper.hpp), so this test deliberately does NOT link
    # `common_test_settings` — that target hard-codes STDEXEC_NAMESPACE to
    # std::execution, which would break user-facing example wrappers.
    add_executable(test.velx_wrapper test_velx_wrapper.cpp)
    set_target_properties(test.velx_wrapper PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF)
    target_link_libraries(test.velx_wrapper
        PRIVATE
        STDEXEC::stdexec
        stdexec_executable_flags
        Catch2::Catch2WithMain
        cfgmgr32)
    catch_discover_tests(test.velx_wrapper PROPERTIES TIMEOUT 30)
endif()
```

- [ ] **Step 5.3 — Build the test**

Run: `cmake --build build --target test.velx_wrapper`

Expected: builds clean. If `STDEXEC_ENABLE_WINDOWS_THREAD_POOL` is not on, the target does not exist; re-run CMake configure with `-DSTDEXEC_ENABLE_WINDOWS_THREAD_POOL=ON`.

- [ ] **Step 5.4 — Run the test**

Run: `./build/test/exec/test.velx_wrapper.exe`

Expected: both test cases pass within ~250 ms total (50 ms per cancel × 2, plus startup overhead). Output ends with `All tests passed`.

If the test hangs: re-check the drainer's stop_requested branch in `examples/velx_wrapper.hpp` (Task 3 Step 3.3). If the test reports `__active_ already taken` on the second case: the cleanup work item is not releasing `__active_` — re-check `__teardown_and_complete` step (e) in Task 3 Step 3.2.

- [ ] **Step 5.5 — Write the README** (`examples/velx_README.md`)

```markdown
# velx wrapper for stdexec

Windows-only example: wraps `CM_Register_Notification` (Cfgmgr32) on
`GUID_DEVINTERFACE_VOLUME` as an `exec::sequence_sender_t`, with
event handoff and cleanup driven by `exec::windows_thread_pool`.
Companion to `examples/da_wrapper.hpp` (macOS / DiskArbitration) on
the *device-level* event source axis, and to
`examples/rdc_pool_wrapper.hpp` on the *Windows / windows_thread_pool*
axis.

## Files

| File | Role |
|---|---|
| `velx_wrapper.hpp` | Header-only `velx::volume_context` |
| `velx.cpp`         | Demo: 30-second observer for arrival / removal |
| `velx_README.md`   | This file |

Build (only configured under Windows):

```sh
cmake --build build --target example.velx
build\examples\example.velx.exe
```

The demo prints `[arrival]` / `[removal]` lines for each currently-present
volume (initial replay), then for any volume that arrives or leaves
during the next 30 seconds, then exits.

## API

```cpp
exec::windows_thread_pool pool{2, 4};
velx::volume_context      ctx;

stdexec::sync_wait(
    velx::on_pool(pool.get_scheduler(), ctx.watch())
  | exec::transform_each(stdexec::then([](velx::volume_event ev) {
      // ev.kind: interface_arrival / interface_removal
      // ev.device_path: UTF-8 lowercased "\\?\volume{guid}"
    }))
  | exec::ignore_all_values());
```

`volume_event` carries:
- `kind` — `interface_arrival` or `interface_removal`
- `device_path` — `\\?\Volume{guid}` form, UTF-8, lowercased

`watch_options{}` is empty in v1 (reserved for v2 — see "Things
deliberately NOT done").

## Pool selection / scheduler

The wrapper does not own a thread pool. The pool is selected at the
pipeline level via `velx::on_pool`:

```cpp
exec::windows_thread_pool pool{2, 4};
sync_wait(velx::on_pool(pool.get_scheduler(), ctx.watch()) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require
`exec::windows_thread_pool::scheduler` in the receiver's env.
Composing with any other scheduler is a compile error — same wall as
DA / RDC pool against silent fallback when a user composes via
`starts_on`.

`velx::on_pool` is an instance of the shared `exec::__on_scheduler_t`
adapter (the same type used by `fsx::on_queue`, `dax::on_queue`,
`inx::on_ring`, and `rdcx::pool::on_pool`). See
[`sequence_sender_on_scheduler.md`](sequence_sender_on_scheduler.md)
for why this exists rather than `stdexec::starts_on`.

## Platform comparison

| | macOS (DA) | Windows (velx) | Linux | Windows (RDC pool) |
|---|---|---|---|---|
| Wrapper namespace | `dax` | **`velx`** | `inx` | `rdcx::pool` |
| Reactor scheduler | `exec::libdispatch_queue` | **`exec::windows_thread_pool`** | `exec::io_uring_context` | `exec::windows_thread_pool` |
| Env adapter | `dax::on_queue` | **`velx::on_pool`** | `inx::on_ring` | `rdcx::pool::on_pool` |
| Source primitive | `DARegisterDisk*Callback` | **`CM_Register_Notification` (DEVINTERFACE_VOLUME)** | `inotify_init1 + IORING_OP_READ` | `ReadDirectoryChangesW` |
| Scope | device-level | **device-level** | filesystem-level | filesystem-level |
| Item shape | per-event `disk_event` | **per-event `volume_event`** | batch `fs_batch` | batch `fs_batch` |
| Backpressure | `binary_semaphore` on dispatch queue callback | **`binary_semaphore` on drainer pool work item** | continuation-style (next read posted from set_value) | continuation-style |

`velx` is the device-level Windows analogue of `dax` (DiskArbitration).
The shared `binary_semaphore` handshake is identical; the divergence is
that CM callbacks fire on a Cfgmgr32-internal worker thread we do not
own, so `velx` interposes an MPSC queue + a drainer work item between
the OS thread and the user's pool. DA does not need this because
`DASessionSetDispatchQueue` lets DA fire callbacks directly on the
user's queue.

## What happens under the hood

```
                   ┌─────────────────────────────────────────────────────────┐
   CM thread       │  __cm_callback(action, EventData)                       │
                   │    1. filter check (DEVINTERFACE + GUID_DEVINTERFACE_VOLUME)│
                   │    2. SymbolicLink (WCHAR) → UTF-8 → lowercase          │
                   │    3. lock(__queue_mu_);                                │
                   │       if (arrival) dedup via __seen_arrivals_           │
                   │       else (removal) erase from __seen_arrivals_        │
                   │       __queue_.push_back(ev);                           │
                   │       if (!__drainer_running_.exchange(true))           │
                   │           SubmitThreadpoolWork(__drainer_work_);        │
                   │       unlock                                            │
                   │    4. return ERROR_SUCCESS (never blocks)               │
                   └─────────────────────────────────────────────────────────┘
                                       │ enqueue
                                       ▼
                   ┌─────────────────────────────────────────────────────────┐
   user pool       │  __drainer_callback(...)                                │
                   │    while (true) {                                       │
                   │      pop one event under __queue_mu_                    │
                   │      connect+start set_next(rcvr, just(ev))             │
                   │      __delivery_done_.acquire()  (binary_semaphore)     │
                   │      if state==stopped → schedule_cleanup(stopped)      │
                   │      if state==error   → schedule_cleanup(error)        │
                   │    }                                                    │
                   └─────────────────────────────────────────────────────────┘
```

`__op` owns:
- `HCMNOTIFICATION __hnotify_` — the registered CM notification
- `PTP_WORK __drainer_work_` — drainer pool work item
- `PTP_WORK __cleanup_work_` — cleanup pool work item
- `std::vector<volume_event> __queue_` (MPSC) under `__queue_mu_`
- `std::unordered_set<std::string> __seen_arrivals_` (dedup) under same mutex
- `std::binary_semaphore __delivery_done_` (drainer ↔ next_receiver handshake)

`volume_context` owns only `std::atomic<__op_base*> __active_` —
single-active CAS slot (matches DA / RDC pool / inotify / fsevents).

## Initial replay

On `subscribe`, the wrapper enumerates currently-present volumes via
`CM_Get_Device_Interface_List(GUID_DEVINTERFACE_VOLUME,
CM_GET_DEVICE_INTERFACE_LIST_PRESENT)` and synthesizes one
`interface_arrival` event per volume. Mirrors DA's daemon-side
behaviour where `DARegisterDiskAppearedCallback` delivers the current
snapshot on register.

If you only want *new* volumes (not the initial replay), dedupe
against an initial snapshot you take in the receiver — same posture
as the DA wrapper documents.

## Backpressure

DA-style: the drainer pool work item acquires `__delivery_done_`
(`std::binary_semaphore`) after each `set_next`, blocking until the
next-receiver fires `set_value` / `set_stopped` / `set_error`. While
the drainer is blocked, more CM events queue up under `__queue_mu_`;
the next drainer iteration drains them in arrival order.

CM events at the interface layer are rare (one per volume insertion
or removal), so the queue depth stays in the single digits even
under stress. v2 may add a bounded queue / drop policy if handle-level
events expand the rate; v1 does not need it.

If a downstream operation does **not** propagate `stop_token` while a
batch is in flight, the drainer can deadlock holding
`__delivery_done_`. Same gotcha as DA / RDC pool. `then`,
`transform_each`, and `bulk` all propagate stop, so this is rare in
practice.

## Cancellation

```
upstream stop_token ──► __on_stop_fn ──► __stop_requested_ = true
                                          ──► __schedule_cleanup(stopped)
                                                     │
                                                     ▼
                                       SubmitThreadpoolWork(__cleanup_work_)
                                                     │
                                                     ▼
                                      __cleanup_callback (on user pool)
                                                     │
                                                     ├─ drop __stop_cb_
                                                     ├─ CM_Unregister_Notification(__hnotify_)
                                                     ├─ WaitForThreadpoolWorkCallbacks(__drainer_work_, FALSE)
                                                     ├─ reset __next_op_, close work items, destroy env
                                                     ├─ release __active_
                                                     └─ set_stopped/set_error(rcvr)
```

All "want to finish" triggers funnel through one `__schedule_cleanup`,
CAS-gated on `__cleanup_scheduled_`:
- Upstream `stop_token` (via `__on_stop_fn`)
- Drainer observes `next_receiver::set_stopped` (state==2)
- Drainer observes `next_receiver::set_error` (state==3)
- Drainer captures exception during `connect` / `start`

`CM_Unregister_Notification` runs in the cleanup work item — **not**
in a CM callback frame — satisfying the API contract that this call
must not be made from inside a notification callback. The OrangeDrive
reference (`lib/platform/windows/volume-event-listener.cpp`) needed a
dedicated abandoned-notifications-drainer thread for the same reason;
the cleanup work item plays that role here.

## CM quirks worth knowing

| Quirk | Detail |
|---|---|
| **CM callback thread is shared process-wide** | Cfgmgr32 dispatches all device notifications from a small internal thread pool. **Never block in the callback** — blocking stalls device notifications for the whole process. The wrapper enforces this by enqueueing and returning. |
| **`SymbolicLink` is WCHAR** | The `EventData->u.DeviceInterface.SymbolicLink` field is `WCHAR[1]` — a flexible array. Convert via `WideCharToMultiByte(CP_UTF8, …)` before storing. The wrapper does this and ASCII-lowercases (`\\?\Volume{guid}` is pure ASCII). |
| **`CM_Unregister_Notification` cannot be called from inside the callback** | Documented in the Cfgmgr32 reference. The wrapper unregisters in the cleanup work item, which is a separate `PTP_WORK` callback. |
| **CM does not auto-replay on register** | Unlike DA, registering a CM notification does not deliver "currently present" devices. The wrapper enumerates explicitly via `CM_Get_Device_Interface_List_PRESENT` and synthesizes arrivals. Without this you would only see *transitions* after register, missing the boot-time volume set. |
| **Dedup is required, not optional** | The `register-then-enumerate` ordering is correct (enumerate-first would lose volumes that appeared in the gap), but it can produce duplicate arrivals if a volume appears in the gap between register and enum. The wrapper's `__seen_arrivals_` set dedupes, also handles `interface_removal` (erase) so re-arrivals are reported. |
| **Buffer sizing race** | `CM_Get_Device_Interface_List_SizeA` returns a size that may grow before `CM_Get_Device_Interface_List` runs (e.g. a USB plug between the two calls). The wrapper retries on `CR_BUFFER_SMALL` — same loop as the OrangeDrive reference. |
| **Multi-string format** | `CM_Get_Device_Interface_ListA` returns a NUL-separated, double-NUL-terminated multi-string. The wrapper parses it sequentially via `strlen` walks. |

## Things deliberately NOT done

- **Handle-level notifications** (per-volume `CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE`):
  remove-pending, remove-complete, custom-event GUID. The reference
  listener's primary motivation for this layer is "give the consumer
  time to clean up before ejection," which v2 should cover. v1's
  `interface_removal` already carries the kernel-confirmed "volume is
  gone" signal — sufficient for the vast majority of consumers.
- **Query-Remove veto**. Same request/response shape as DA approval
  callbacks. v2 should decide between (a) a separate `query_remove_sender`
  that returns `bool`, or (b) a per-event ack object on the main sender.
  Both have known footguns.
- **Custom-event GUID dispatch** (vendor events). Tied to the
  handle-level layer; ships together with v2.
- **Match dictionary in `watch_options`**. `CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE`
  has no kernel-side match analogue beyond the class GUID. A
  wrapper-side filter is just `transform_each | filter` in user code.
- **Multi-subscriber fan-out** on a single `volume_context`. The
  `__active_` slot is single-shot CAS-guarded; same posture as DA /
  RDC pool / inotify. Build a layer on top.
- **Automated VHD demo**. Would require admin privileges and
  PowerShell scripting; the demo prints instructions instead.
```

- [ ] **Step 5.6 — Build (final smoke)**

Run:
```sh
cmake --build build --target example.velx
cmake --build build --target test.velx_wrapper
```

Expected: both targets build clean.

- [ ] **Step 5.7 — Run final demo + tests**

Run:
```sh
./build/examples/example.velx.exe
./build/test/exec/test.velx_wrapper.exe
```

Expected:
- Demo prints initial replay arrivals, then waits 30 seconds, exits 0.
- Tests print `All tests passed` (2 cases) within ~250 ms.

- [ ] **Step 5.8 — Commit**

```bash
git add examples/velx_README.md test/exec/test_velx_wrapper.cpp test/exec/CMakeLists.txt
git commit -m "$(cat <<'EOF'
examples: README + structural tests for velx wrapper

README documents the platform comparison (dax / inx / rdcx::pool /
velx), CM-callback handoff via MPSC queue + drainer pool work item,
initial replay + dedup, cancellation through cleanup work item, and
the CM quirks (shared dispatcher thread, SymbolicLink WCHAR
conversion, CM_Unregister_Notification not-in-callback constraint,
buffer sizing race) the caller needs to know.

Structural tests mirror test_da_wrapper.cpp: compile-time
windows_thread_pool::scheduler env constraint, cancel-before-event
teardown, sequential subscriptions on one volume_context.
EOF
)"
```

---

## Self-Review

**1. Spec coverage**

| Spec section | Implementing task |
|---|---|
| `volume_context` (CAS-guarded `__active_`, no other shared state) | Task 1 (Step 1.1) |
| `volume_event{kind, device_path}` + `volume_event_kind` enum | Task 1 (Step 1.1) |
| `watch_options{}` empty-shell | Task 1 (Step 1.1) |
| `velx::on_pool` (shared `exec::__on_scheduler_t`) | Task 1 (Step 1.1) |
| `__watch_sender` with sequence_sender_tag, item_types | Task 1 (Step 1.1, scaffold) + Task 2 (Step 2.2, subscribe) |
| Compile-time `windows_thread_pool::scheduler` env constraint | Task 2 (Step 2.2) |
| CM_Register_Notification (DEVINTERFACE + GUID_DEVINTERFACE_VOLUME) | Task 2 (Step 2.1, `start()`) |
| CM callback: filter check, WCHAR → UTF-8 lowercase, enqueue, submit drainer | Task 2 (Step 2.1, `__cm_callback`) |
| MPSC queue + drainer work item + binary_semaphore handshake | Task 2 (Step 2.1, `__drainer_callback`) |
| Single-active subscription via CAS rejection | Task 2 (Step 2.1, `start()`) |
| Stop callback registered last in `start()` | Task 3 (Step 3.3) |
| `__schedule_cleanup` CAS-gated, all terminal triggers funnel through it | Task 3 (Steps 3.1, 3.2, 3.3) |
| Cleanup work item: drop stop_cb, CM_Unregister, Wait drainer, complete rcvr | Task 3 (Step 3.2, `__teardown_and_complete`) |
| Initial replay via `CM_Get_Device_Interface_List_PRESENT` | Task 4 (Step 4.2) |
| Dedup via `__seen_arrivals_` (closes register-vs-enumerate race) | Task 4 (Steps 4.1, 4.2) |
| Buffer-size retry on `CR_BUFFER_SMALL` | Task 4 (Step 4.2) |
| `CM_Register_Notification` failure → rollback `__active_`, set_error | Task 2 (Step 2.1, `start()`) |
| `CM_Get_Device_Interface_List` failure → rollback all resources, set_error | Task 4 (Step 4.2) |
| `WideCharToMultiByte` failure → log+drop event, do not fail stream | Task 2 (Step 2.1, `__cm_callback`) — silent drop matches the design's "log a warning" |
| Demo: `windows_thread_pool` + `when_any` timer + `transform_each` print | Task 3 (Step 3.4) |
| Structural tests: env constraint, cancel-before-event, sequential subscribes | Task 5 (Step 5.1) |
| README with platform comparison + quirks | Task 5 (Step 5.5) |
| CMake wiring under `if (WIN32)` linking `cfgmgr32` | Task 1 (Step 1.3) — example; Task 5 (Step 5.2) — test |

No spec section is left without a task.

**2. Placeholder scan**

No "TBD" / "TODO" / "implement later" / "add appropriate ..." in the plan. Every code block is the actual code; every command is exact; every file path is exact. The single soft note is in Task 2 Step 2.1's "log a warning" for the `WideCharToMultiByte` failure path — the actual implementation silently returns `ERROR_SUCCESS` without logging (the example wrapper does not pull in a logging dependency, matching DA's "deep-copy or skip" posture). The README documents this as "fails for an individual event drops that event."

**3. Type consistency**

- `velx::volume_event { kind, device_path }` — same in Task 1 declaration, Task 2 demo lambda, Task 3 demo, Task 5 README, Task 5 test. ✓
- `velx::volume_event_kind { interface_arrival, interface_removal }` — same throughout. ✓
- `velx::on_pool` — defined as `exec::__on_scheduler_t{}` instance in Task 1, used in Tasks 3 / 5. ✓
- `volume_context::__active_` field name + type used in `__op::start()` (Task 2), `__teardown_and_complete()` (Task 3) — consistent. ✓
- `__finish_kind` enum used in `__op::__finish_kind_` (Task 1 declaration in `__detail`), `__schedule_cleanup` (Task 3), `__teardown_and_complete` (Task 3). ✓
- `__queue_mu_`, `__queue_`, `__drainer_running_`, `__seen_arrivals_` — all under same mutex (Tasks 2, 4). ✓
- `__delivery_done_`, `__delivery_state_`, state values 1/2/3 — declared Task 2, used Task 2 / Task 3. ✓
- `__hnotify_`, `__drainer_work_`, `__cleanup_work_`, `__env_` — same names across Tasks 2 / 3. ✓
- `windows_thread_pool::scheduler::native_handle()` returns `PTP_POOL` — used in `SetThreadpoolCallbackPool` (Task 2 ctor). Cross-checked against `examples/rdc_pool_wrapper.hpp:194-196`. ✓

No type / signature drift across tasks.

**4. No dangling references**

All types referenced (`volume_event`, `volume_event_kind`, `watch_options`, `volume_context`, `__op`, `__op_base`, `__next_receiver`, `__watch_sender`, `__on_stop_fn`, `__stop_callback_t`, `__finish_kind`) are defined in the task that introduces them.

---

## Open questions / known limitations

- **`<ioevent.h>` availability across SDK versions.** The Windows SDK has historically defined `GUID_DEVINTERFACE_VOLUME` in `<ioevent.h>`, but some toolchains place it in `<Mountmgr.h>` or require `<Wdmguid.h>`. Task 1 Step 1.4 documents the fallback `DEFINE_GUID` if includes alone do not resolve.
- **`CM_Get_Device_Interface_ListA` vs `_ListW`**. The plan uses the `A` variant for natural string handling; volumes only ever return ASCII paths so this is safe. The reference uses `A` for the same reason.
- **`CM_Register_Notification` failure path is hard to unit-test.** Same posture as the RDC pool wrapper's `ReadDirectoryChangesW` failure — neither API has an injectable fault hook. The error path is exercised by `start()` rollback under runtime conditions; unit tests cover the happy path + cancellation only.
- **CM callback thread pool size.** Cfgmgr32 may serialize callbacks on a small internal pool. The wrapper does not assume parallelism; the queue mutex serializes producers regardless. If two CM callbacks fire concurrently for the same `__op` (different threads of the CM pool), `__queue_mu_` ensures correctness.

These are not blockers; they are the same constraints the existing Windows-side examples (RDC pool, RDC) already live with.
