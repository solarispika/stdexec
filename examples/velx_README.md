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
    exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch())
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
pipeline level via `exec::sequence_with_scheduler`:

```cpp
exec::windows_thread_pool pool{2, 4};
sync_wait(exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch()) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require
`exec::windows_thread_pool::scheduler` in the receiver's env.
Composing with any other scheduler is a compile error — same wall as
DA / RDC pool against silent fallback when a user composes via
`starts_on`.

`exec::sequence_with_scheduler` is the shared adapter from
`include/exec/on_scheduler.hpp`, used by all five example wrappers.
See [`sequence_sender_on_scheduler.md`](sequence_sender_on_scheduler.md)
for why this exists rather than `stdexec::starts_on`.

## Platform comparison

| | macOS (DA) | Windows (velx) | Linux | Windows (RDC pool) |
|---|---|---|---|---|
| Wrapper namespace | `dax` | **`velx`** | `inx` | `rdcx::pool` |
| Reactor scheduler | `exec::libdispatch_queue` | **`exec::windows_thread_pool`** | `exec::io_uring_context` | `exec::windows_thread_pool` |
| Env adapter | `exec::sequence_with_scheduler` | **`exec::sequence_with_scheduler`** | `exec::sequence_with_scheduler` | `exec::sequence_with_scheduler` |
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
