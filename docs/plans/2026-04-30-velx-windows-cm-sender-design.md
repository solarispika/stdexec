# Windows volume sender (velx) — design

Date: 2026-04-30
Branch: `examples/platform-event-senders` (velx wrapper added on top of the
inotify wrapper landing)
Scope: `examples/velx_wrapper.hpp`, `examples/velx.cpp`,
`examples/velx_README.md`, `test/exec/test_velx_wrapper.cpp`

## Motivation

Apply the DiskArbitration × `libdispatch_queue` pattern (see
`2026-04-29-da-libdispatch-sender-design.md`) to the Windows side, using
`CM_Register_Notification` (Cfgmgr32) as the device-event source. This is
the second `sequence_sender` over a Win32-callback-driven event source —
the first was `rdc_pool_wrapper.hpp` (RDC × `windows_thread_pool`) — and
the *device-level* counterpart on Windows of the macOS DA wrapper.

The reference for the Win32 mechanics is the Synology OrangeDrive
listener at `lib/platform/windows/volume-event-listener.cpp`: it uses
`CM_Register_Notification` with `CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE`
on `GUID_DEVINTERFACE_VOLUME` for arrival/removal, and a per-volume
`CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE` for the handle-level events
(query-remove veto, remove-pending, remove-complete, custom-event). The
v1 wrapper covers only the interface layer; the handle layer is a
follow-up (see "Out of scope").

## Decisions

### 1. Same four invariants as the DA / RDC-pool wrappers

1. **CAS-guarded single-active subscription.** `volume_context::__active_`
   is a single slot; a second concurrent `subscribe` on the same context
   fails with `set_error("already active")`. Same shape as `dax::da_context`,
   `inx::inotify_context`, `rdcx::pool::rdc_context`.
2. **Per-op operation state owns the OS resources.** `__op<_Rcvr>` owns
   the `HCMNOTIFICATION`, the per-op `TP_CALLBACK_ENVIRON`, and both
   `PTP_WORK` items. No global state in `volume_context` beyond the
   active slot.
3. **`__watch_sender::subscribe` is constrained at compile time** to
   require `exec::windows_thread_pool::scheduler` in the receiver env via
   `exec::__env_has_scheduler`. Composing with any other scheduler type
   is a compile error — same wall as DA / RDC pool against silent
   fallback when a user composes via `starts_on`.
4. **`velx::on_pool` reuses the shared `exec::__on_scheduler_t`**
   (`include/exec/on_scheduler.hpp`, the same instance type used by
   `fsx::on_queue` / `dax::on_queue` / `inx::on_ring` /
   `rdcx::pool::on_pool`). Sequence-sender semantics are preserved
   across env injection — see `examples/sequence_sender_on_scheduler.md`.

### 2. Scope: observation-only, interface-level only

Subscribed events:

- `CM_NOTIFY_ACTION_DEVICEINTERFACEARRIVAL` → `volume_event_kind::interface_arrival`
- `CM_NOTIFY_ACTION_DEVICEINTERFACEREMOVAL` → `volume_event_kind::interface_removal`

Filter:

```cpp
CM_NOTIFY_FILTER f{
  .cbSize     = sizeof f,
  .FilterType = CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE,
  .u          = {.DeviceInterface = {.ClassGuid = GUID_DEVINTERFACE_VOLUME}},
};
```

Excluded from v1:

- `CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE` notifications (per-volume handle
  events). The reference listener registers one per arrived volume to
  cover the user's "give me time to clean up before ejection" use case.
  In v1 the `interface_removal` event already carries the
  kernel-confirmed "volume is gone" signal, which is sufficient for the
  vast majority of consumers; the handle-level pre-removal phases are
  punted to v2.
- `CM_NOTIFY_ACTION_DEVICEQUERYREMOVE` veto. The CM callback for this
  action expects a `DWORD` return value (`ERROR_CANCELLED` to deny).
  This is request/response — the same shape DA's approval callbacks
  have, which the DA wrapper deliberately excluded for the same reason
  (sequence_sender items are fire-and-forget; modeling veto would
  require a per-event ack object or a second sender pair). v2 territory.
- `CM_NOTIFY_ACTION_DEVICECUSTOMEVENT` (carries a custom GUID per
  vendor). Tied to handle-level notifications — out by virtue of (1).

### 3. Item type: single `volume_event` per CM callback

Unlike the inotify and RDC wrappers (which read a *buffer of events*
per IO completion and naturally batch them), CM delivers one callback
per device transition. The sender's item type is therefore a single
`volume_event`:

```cpp
namespace velx {
  enum class volume_event_kind { interface_arrival, interface_removal };

  struct volume_event {
    volume_event_kind kind;
    std::string       device_path;   // "\\?\volume{guid}", UTF-8 lowercased
  };
}
```

`device_path` is owned (deep copy from `EventData->u.DeviceInterface.SymbolicLink`,
WCHAR → UTF-8 via `WideCharToMultiByte(CP_UTF8, …)`, then ASCII-lowercased
in place — `\\?\Volume{guid}` is pure ASCII so a per-byte
`std::tolower` is correct). Same *lowercase + UTF-8 + own the bytes*
convention as the OrangeDrive reference, so downstream consumers can
dedupe / map by `device_path` without normalization. A
`WideCharToMultiByte` failure for an individual event drops that event
with a logged warning (see "Error handling") rather than failing the
whole stream.

### 4. `watch_options{}` is empty in v1

Reserved for ABI extensibility in v2 (e.g. `watch_handle_remove_pending`,
`watch_custom_event`, a description-key narrowing analogue). Empty by
design — there is no kernel-side match dictionary for `CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE`
beyond the class GUID, so any wrapper-side filter belongs in user code
via `transform_each | filter`.

### 5. Initial replay on subscribe; no synthetic removal on stop

DA's daemon delivers `appeared` for every currently-visible disk on
register. CM does **not** auto-replay existing devices — we have to
enumerate. Decision: enumerate `CM_Get_Device_Interface_List(GUID_DEVINTERFACE_VOLUME,
CM_GET_DEVICE_INTERFACE_LIST_PRESENT)` in `start()` and push each path
into the queue as a synthetic `interface_arrival` event. Subscribers
see a snapshot of current state on subscribe, matching DA semantics.

**Race between register and enumerate.** CM only fires on transitions
(it does not auto-replay on register), so the only correct order is
*register first, enumerate second*: enumerate-first would lose any
volume that appeared in the gap between enum and register. But
register-first introduces a different race — a volume that appears in
the gap between register and enum will be both CM-reported (callback
fired) and enum-reported (present at enum time), surfacing as a
duplicate `arrival`. The reference handles this by inserting into a
`m_seen_devices` map at every arrival site (CM callback and enum loop
share the same `HandleVolumeDeviceArrival` path) and discarding the
second insert.

We adopt the same dedup, scoped to the wrapper's lifetime: an
`std::unordered_set<std::string> __seen_arrivals_` (under the same
mutex as the queue), updated by both the CM callback and the
`start()` enumeration loop. `interface_removal` events also erase
from the set so a volume that disappears and reappears is reported
correctly. The set lifetime equals the `__op` lifetime — bounded by
the cardinality of "volumes the user has seen during this watch."

The reference also synthesizes `interface_removal` for every known
volume on `Stop()`. We do **not** mirror this:

- DA wrapper does not do it (relies purely on `set_stopped`).
- It is misleading — the volumes are not actually gone, the watch is
  ending. A user that uses `removal` to drop file handles would
  spuriously close them on user-initiated cancel.
- `set_stopped` is the unambiguous "watch is ending" signal that the
  sender protocol provides; the user's receiver can clean up known
  volumes there.

### 6. CM-callback handoff: MPSC queue + drainer pool work item

This is the key Windows-specific decision and the only part that
materially diverges from DA. CM callbacks are dispatched from a
**Cfgmgr32-internal worker thread** that we do not own and cannot
choose. Two hard constraints:

- We cannot bind the user's `windows_thread_pool` to that thread (no
  hook exists).
- We must not block the CM callback. The Cfgmgr32 dispatcher is shared
  process-wide (and ultimately by the kernel); blocking it stalls all
  device notifications. CM also enforces a 5-second timeout for the
  Query-Remove family (not relevant to v1's interface-level scope but a
  signal of the dispatcher's posture).

Therefore the CM callback only **enqueues** and the user's pool drains:

```
                   ┌─────────────────────────────────────────────────────────┐
   CM thread       │  __cm_callback(action, EventData)                       │
                   │    1. filter check (DEVINTERFACE + GUID_DEVINTERFACE_VOLUME)│
                   │    2. SymbolicLink → UTF-8 → lowercase                  │
                   │    3. lock(__queue_mu_);                                │
                   │       if (action==arrival)                              │
                   │         if (!__seen_arrivals_.insert(path).second)      │
                   │           { unlock; return ERROR_SUCCESS; } // dedup    │
                   │       else if (action==removal)                         │
                   │         __seen_arrivals_.erase(path);                   │
                   │       __queue_.push_back(ev);                           │
                   │       if (!__drainer_running_.exchange(true))           │
                   │           SubmitThreadpoolWork(__drainer_work_);        │
                   │       unlock                                            │
                   │    4. return ERROR_SUCCESS                              │
                   └─────────────────────────────────────────────────────────┘
                                       │ enqueue
                                       ▼
                   ┌─────────────────────────────────────────────────────────┐
   user pool       │  __drainer_callback(...)                                │
                   │    while (true) {                                       │
                   │      lock(__queue_mu_);                                  │
                   │      if (__stop_requested_) { unlock; return; }          │
                   │      if (__queue_.empty()) {                             │
                   │          __drainer_running_.store(false); unlock; ret; }│
                   │      ev = move(__queue_.front()); __queue_.erase(begin);│
                   │      unlock;                                            │
                   │      __delivery_state_ = 0;                             │
                   │      try { connect+start set_next(rcvr, just(ev)); }    │
                   │      catch (...) { state=3; error_=…; release; }       │
                   │      __delivery_done_.acquire();                       │
                   │      __next_op_.reset();                                │
                   │      if (state==2) schedule_cleanup(stopped); ret;     │
                   │      if (state==3) schedule_cleanup(error);   ret;     │
                   │    }                                                    │
                   └─────────────────────────────────────────────────────────┘
```

Why this is exactly DA's pattern with one indirection added:
- The `binary_semaphore` / `__delivery_state_` handshake between the
  drainer and `next_receiver` is byte-for-byte the DA `__delivery_done_`
  / `__delivery_state_` pair. The single in-flight `set_next` invariant
  is preserved.
- DA's serial dispatch queue *is* the event-delivery thread; here, the
  drainer pool work item is the event-delivery thread. The CM callback
  is the (untouchable) producer that DA does not have an analogue for.
- The MPSC queue absorbs the bursts that DA's serial queue absorbs
  inside libdispatch. For v1's interface-level event rate (one per
  volume insertion / removal) the queue depth is in the single digits
  even under stress; bounding is unnecessary in v1.

### 7. `start()` ordering

To keep cleanup's `WaitForThreadpoolWorkCallbacks` correct when a stop
request fires synchronously at subscription time (e.g. `when_any` with
an already-stopped token), `start()` brings up resources in this exact
order:

1. CAS `__active_` to take the slot.
2. `InitializeThreadpoolEnvironment` + `SetThreadpoolCallbackPool` to
   the user's pool, then `CreateThreadpoolWork` for both the drainer
   and cleanup callbacks.
3. `CM_Register_Notification` for `DEVINTERFACE_VOLUME` arrival/removal.
   CM may begin firing callbacks immediately; they enqueue under the
   shared mutex but do not yet trigger a drainer submit because
   `__drainer_running_` is initially `false` and the lock contention
   with step 4 is fine.
4. Enumerate `CM_Get_Device_Interface_List`. Lock `__queue_mu_`, dedupe
   each enumerated path against `__seen_arrivals_` and push surviving
   `interface_arrival` events. Set `__drainer_running_ = true` while
   still holding the lock. Unlock.
5. `SubmitThreadpoolWork(__drainer_work_)` to drain the (possibly
   non-empty) queue.
6. `__stop_cb_.emplace(...)` last. If the token is already in stopped
   state the callback fires synchronously here, but every resource it
   touches (CM notification, work items, queue, drainer) is fully up,
   so the synchronous `__schedule_cleanup` and the eventual
   `WaitForThreadpoolWorkCallbacks(__drainer_work_, FALSE)` are
   well-defined. Reversing this with step 5 would let cleanup wait on
   a drainer that has not been submitted yet — `Wait` returns
   immediately, cleanup completes the receiver, then step 5 submits a
   drainer that runs after the receiver has been moved-from. Same
   ordering hazard as the DA wrapper's "register stop callback last"
   comment.

### 8. Cleanup serialization via `WaitForThreadpoolWorkCallbacks`

All "want to finish" triggers funnel through one `__schedule_cleanup`
call, CAS-gated on `__cleanup_scheduled_`:

```
upstream stop_token ──► __on_stop_fn ──► flag + schedule_cleanup(stopped)
drainer: next_receiver::set_stopped ──► state=2 ──► drainer schedules cleanup
drainer: next_receiver::set_error   ──► state=3 ──► drainer schedules cleanup
drainer: connect/start exception    ──► state=3 ──► drainer schedules cleanup
```

The cleanup work item runs on the user's pool and:

1. Drops `__stop_cb_` (so a late stop request cannot re-enter teardown).
2. Calls `CM_Unregister_Notification(__hnotify_)`. **This is the unique
   safe site for unregistering** — it is *not* a CM callback frame, so
   the API contract that "CM_Unregister_Notification cannot be called
   from inside a CM callback" is satisfied. The OrangeDrive reference
   needed a dedicated abandoned-notifications-drainer thread for the
   same reason; here the cleanup work item plays that role.
3. `WaitForThreadpoolWorkCallbacks(__drainer_work_, FALSE)` to wait for
   the drainer to drain. The drainer observes `__stop_requested_` next
   loop iteration and returns; if it was idle (queue empty,
   `__drainer_running_=false`) the wait is a no-op. Calling
   `WaitForThreadpoolWorkCallbacks` from inside a *different* work
   item is documented-safe (it would deadlock only if applied to the
   currently-running work item itself).
4. Resets `__next_op_` (drainer should have already; defensive).
5. Releases the `__active_` CAS slot.
6. Completes the user receiver: `set_error(rcvr, error_)` if
   `__finish_kind_ == __finish_error`, else `set_stopped(rcvr)`.

## API

```cpp
namespace velx {
  enum class volume_event_kind { interface_arrival, interface_removal };

  struct volume_event {
    volume_event_kind kind;
    std::string       device_path;   // UTF-8 lowercased "\\?\volume{guid}"
  };

  struct watch_options {};           // empty in v1, reserved for v2

  class volume_context {
   public:
    volume_context() = default;
    ~volume_context() = default;
    volume_context(const volume_context&)                    = delete;
    auto operator=(const volume_context&) -> volume_context& = delete;

    auto watch(watch_options = {}) -> __detail::__watch_sender;

   private:
    std::atomic<__detail::__op_base*> __active_{nullptr};
  };

  inline constexpr exec::__on_scheduler_t on_pool{};
}
```

Pipeline shape:

```cpp
exec::windows_thread_pool pool;
velx::volume_context      ctx;

stdexec::sync_wait(
    velx::on_pool(pool.get_scheduler(), ctx.watch())
  | exec::transform_each(stdexec::then([](velx::volume_event ev) {
      std::printf("%s %s\n",
                  ev.kind == velx::volume_event_kind::interface_arrival
                    ? "[arrival]" : "[removal]",
                  ev.device_path.c_str());
    }))
  | exec::ignore_all_values());
```

## Error handling

| Situation | Handling |
|---|---|
| `CM_Get_Device_Interface_List` (initial enumeration) fails | Rollback `__active_`, close work items, `set_error(rcvr, system_error{cr, …})` synchronously in `start()` |
| `CM_Register_Notification` fails | Same as above |
| `CreateThreadpoolWork` / `InitializeThreadpoolEnvironment` fails | Same as above |
| Per-event `SymbolicLink` → UTF-8 conversion fails inside CM callback | Log a warning and drop that event. Reasoning: a single malformed `SymbolicLink` should not fail the whole stream. Matches the reference's `WARN_MSG`-and-continue policy. |
| Drainer captures exception during `connect` / `start` of next-sender | Store `current_exception()`, set `__delivery_state_=3`, release semaphore. Drainer's outer loop sees state==3 and routes to `schedule_cleanup(error)`. |
| `next_receiver::set_error(_E&&)` | If `_E` is `exception_ptr`, store directly; otherwise `make_exception_ptr`. State=3 + release. |
| `__on_stop_fn` fires after cleanup is already scheduled | `__cleanup_scheduled_` CAS rejects the second submission. No-op. |

## Testing strategy

### Demo (`examples/velx.cpp`)

Mirrors the `inotify.cpp` / `da.cpp` "timer + observer" shape; does not
attempt to programmatically trigger volume events (VHD attach requires
admin; `subst` does not fire `DEVINTERFACE_VOLUME`):

1. Spin up a `windows_thread_pool` and a 1-thread `static_thread_pool`
   as the timer driver.
2. Subscribe `velx::on_pool(pool.get_scheduler(), ctx.watch())` and
   print each event.
3. Bound the run with `when_any(timer(30s), pipeline)`.
4. README documents how to actually fire events during the run:
   - **USB drive insert/remove** — most direct, no admin required
   - **PowerShell `Mount-VHD` / `Dismount-VHD`** on a `.vhdx`
     (admin required)

The 30-second window gives a human time to plug in a drive.

### Structural unit tests (`test/exec/test_velx_wrapper.cpp`)

Pure wrapper invariants, no real volume events triggered. Mirrors
`test_da_wrapper.cpp`:

- **Compile-time env constraint.** `static_assert(!exec::__env_has_scheduler<stdexec::env<>, exec::windows_thread_pool::scheduler>)`
  documents the same invariant that the `subscribe` `requires`-clause
  enforces.
- **Stop-before-event teardown.** Subscribe, immediately request stop
  via `when_any(timer(50ms), watch())`, expect `set_stopped` cleanly.
  Verifies the cleanup path when no CM callback ever fired (drainer was
  briefly active for initial replay; if no volumes are present the
  initial-replay set is empty, otherwise the drainer delivers and
  `transform_each` propagates stop).
- **Sequential subscriptions.** Run the cancel-before-event scenario
  twice on the same context to verify the `__active_` CAS releases on
  cleanup and a second `subscribe` succeeds.
- **(Not covered by unit tests.)** `CM_Register_Notification` failure
  and `CM_Get_Device_Interface_List` failure paths — neither API has
  an injectable fault hook, same posture as the RDC pool wrapper's
  IO-failure paths. Documented in the README's Quirks section.

Tests live in a new Windows-only test target gated by the same
Windows / `STDEXEC_BUILD_EXAMPLES` fence the existing `example.rdc_pool`
target uses (concrete CMake target name picked at implementation
time — the existing pattern is `test.<wrapper>` per `test_da_wrapper`).

## Out of scope (follow-ups)

- **Handle-level notifications** (per-volume `CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE`):
  remove-pending, remove-complete, custom-event GUID. The reference
  listener's primary motivation for this layer is "give the consumer
  time to clean up before ejection," which v2 should cover by adding:
  - `watch_options::watch_handle_remove_pending` (etc.) opt-in flags
  - Per-arrival registration of a second `HCMNOTIFICATION` against
    the volume handle (opened with `CreateFileW(device_path,
    GENERIC_READ, …, OPEN_EXISTING, …)`)
  - A `device_path → HCMNOTIFICATION` map on `__op` for unregistration
    routing on `interface_removal`
- **Query-Remove veto.** Same request/response shape as DA approval
  callbacks. v2 should decide between (a) a separate `query_remove_sender`
  that returns `bool`, or (b) a per-event ack object on the main
  sender. Both have known footguns.
- **Custom-event GUID dispatch** (vendor events). Tied to the
  handle-level layer; ships together with v2.
- **Multi-subscriber fan-out** on a single `volume_context`. The
  `__active_` slot is single-shot CAS-guarded; same posture as DA /
  RDC pool / inotify. Build a layer on top.
- **Match dictionary in `watch_options`.** CM's
  `CM_NOTIFY_FILTER_TYPE_DEVICEINTERFACE` does not have a kernel-side
  match analogue beyond the class GUID. A wrapper-side filter is just
  `transform_each | filter` in user code.
- **Automated VHD demo.** Would require admin and PowerShell
  scripting; the README documents the manual procedure instead.
