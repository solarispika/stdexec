# DiskArbitration wrapper for stdexec

macOS-only example showing how to wrap a callback-driven event source
(`DASession`) as an `exec::sequence_sender_t`. Mirrors the FSEvents
wrapper (`examples/fsevents_wrapper.hpp`) — both apply the same
"per-op private serial queue + binary_semaphore backpressure" pattern
documented in `docs/plans/2026-04-29-da-libdispatch-sender-design.md`.

## Files

| File | Role |
|---|---|
| `da_wrapper.hpp` | Header-only `dax::da_context` — the sequence sender |
| `da.cpp` | Demo: drive DA traffic via `hdiutil` + sparseimage |
| `da_README.md` | This file |

Build (only configured under `APPLE`):

```sh
cmake --build build --target example.da
./build/examples/example.da
```

Demo creates `/tmp/dax_demo.sparseimage`, attaches it without mount,
detaches it, prints each `disk_event`, and cleans up.

## API

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("my.pool");
dax::da_context ctx;

stdexec::sync_wait(
    exec::sequence_with_scheduler(
        pool.get_scheduler(),
        ctx.watch({.watch_appeared            = true,
                   .watch_disappeared         = true,
                   .watch_description_changed = false}))
  | exec::transform_each(stdexec::then([](dax::disk_event e){ ... }))
  | exec::ignore_all_values());
```

Each `disk_event` carries `kind` (one of `appeared` / `disappeared` /
`description_changed`), `bsd_name`, optional `volume_name` /
`volume_path`, and `changed_keys` (populated only for
`description_changed`). All strings are owned (deep-copied from the
CFString contents during the callback).

### Pre-removal hook (approval callbacks)

DA's approval callbacks (mount / unmount / eject) let a listener
veto an operation by returning a non-NULL `DADissenterRef`. The
wrapper exposes them via three `watch_options` fields, each a
`dax::approval_policy` (= `approval::policy<dax::disk_info>`):

```cpp
dax::watch_options opts{};
opts.unmount_approval = approval::sync<dax::disk_info>{
  .predicate = [](dax::disk_info const& info) {
    do_my_cleanup(info);   // synchronous; runs on DA's dispatch queue
    return true;           // allow; return false to veto
  },
};
```

For predicates that may legitimately block (service shutdown,
file-flush waits) use `approval::bounded` so the wrapper enforces a
timeout and falls back to `on_timeout_allow` if the worker overruns:

```cpp
opts.unmount_approval = approval::bounded<dax::disk_info>{
  .predicate = [](dax::disk_info const& info, stdexec::inplace_stop_token tok) {
    return stop_services_for(info, tok);   // bail out via tok.stop_requested()
  },
  .timeout          = std::chrono::seconds{4},
  .on_timeout_allow = true,
};
```

The default-constructed value is `std::monostate` — the wrapper
does not register the callback at all, so DA proceeds with no input
from this listener (matching v1 behavior).

## Queue selection / scheduler

The wrapper does not own a dispatch queue. The queue is selected at the
pipeline level via `exec::sequence_with_scheduler`:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("...");
sync_wait(exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch(opts)) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require a
`libdispatch_scheduler` in the receiver's env. Composing with any other
scheduler type is a compile error — same posture as the FSEvents
wrapper.

### Why `exec::sequence_with_scheduler` instead of `stdexec::starts_on`?

Same reason as FSEvents: `stdexec::starts_on` (and `stdexec::write_env`)
collapse sequence-sender attributes today, so downstream
`transform_each` loses the per-event type.
`exec::sequence_with_scheduler` is the shared adapter (defined in
`include/exec/on_scheduler.hpp`, used by all five example wrappers)
that performs the env injection while preserving sequence-sender
semantics. See `sequence_sender_on_scheduler.md` for the full
explanation.

### Internal serial queue

Each watch operation creates its own serial queue with the user's
queue as target (`dispatch_queue_create_with_target`). DA's own
serialization assumption (one in-flight callback at a time per
session) plus the wrapper's `dispatch_async_f`-based teardown require
a serial layer; the worker thread comes from the user's pool.

## What happens under the hood

```
DA daemon ──callback──► dispatch queue ──set_next──► next sender
                              │                          │
                              │                          ▼
                              ▼                      (downstream
                       semaphore.acquire ◄── set_value  pipeline)
                              │
                              ▼
                      next event processed
```

Single active subscription per `da_context` (CAS-guarded). Session
lifecycle (`DASessionCreate` / `DARegister*Callback` /
`DASessionSetDispatchQueue` / `DAUnregisterCallback` / `CFRelease`) is
owned by the operation state, with all teardown serialized through the
dispatch queue so it can never race with an in-flight callback.

## Backpressure

The callback synchronously blocks on `__delivery_done_` (a
`std::binary_semaphore`) until the next sender completes. Because DA
delivers callbacks serialized on the assigned dispatch queue, this
gives "natural" backpressure — slow consumer means events queue up
inside DA's internals rather than firing concurrently.

Limit: there is no documented per-session buffer limit on Apple's side.
Long-stall consumers will eventually consume memory inside the DA
daemon. Cancel the watch promptly if you can't keep up.

## Cancellation

```
upstream stop_token ──► __on_stop_fn ──► dispatch_async_f to queue
                              │                     │
                              ▼                     ▼
                  __stop_requested_ = true   DASessionSetDispatchQueue(NULL)
                              │             DAUnregisterCallback ×N
                              │             CFRelease(session)
                              │                     │
                              │                     ▼
                              │           set_stopped(outer rcvr)
                              ▼
                  in-flight next sender's stop_token (via env)
                  propagates → next_receiver::set_stopped → callback unblocks
```

If a downstream operation does **not** propagate `stop_token` (rare),
the callback can deadlock holding the semaphore. Same gotcha as
FSEvents.

## DA quirks worth knowing

| Quirk | Detail |
|---|---|
| **Initial replay** | On registration, DA fires `DiskAppearedCallback` for **every disk currently visible** to the daemon (every BSD device, every mounted volume, every network share that DA tracks). This is documented behavior. The demo prints ~30 `appeared` events before the sparseimage one. If you only want *new* disks, dedupe against an initial snapshot. |
| **`bsd_name` may be empty** | `DADiskGetBSDName` returns `NULL` for non-BSD disks (some network volumes). The wrapper stores an empty `bsd_name` in that case rather than throwing. |
| **`volume_path` may be missing** | A disk that exists but is not mounted has no `kDADiskDescriptionVolumePathKey`. `volume_path` is `std::nullopt`. The sparseimage in the demo is attached with `-nomount`, so `appeared`/`disappeared` for it carry no path. |
| **Approval callbacks** | `DARegisterDiskMountApprovalCallback` and friends require the consumer to *answer* each event with a `DADissenterRef` (NULL = allow). Modeled separately from the notification stream because the contract is request/response: configure `watch_options::{mount,unmount,eject}_approval` with an `approval::sync<dax::disk_info>` (predicate runs inline on the wrapper's dispatch queue) or `approval::bounded<dax::disk_info>` (predicate runs on a worker, wrapper enforces a timeout, falls back to `on_timeout_allow`). The default `monostate` skips the registration entirely — DA proceeds with no input from this listener. See `examples/approval_policy.hpp`. |
| **`description_changed` is opt-in** | Off by default — it can be very noisy (filesystem state changes, mount/unmount transitions all fire it). Set `watch_options::watch_description_changed = true` if you actually want it. To narrow which keys trigger the callback, populate `watch_options::description_keys` with the raw key strings (e.g. `"DAVolumeName"`, `"DAVolumePath"` — same shape as `disk_event::changed_keys`); an empty vector (default) keeps DA's "watch all keys" behavior. |
| **Match dictionary** | Populate `watch_options::match` to narrow which disks fire callbacks (forwarded as the `match` `CFDictionaryRef` to all three `DARegister*Callback` calls). Keys are the raw DA description key strings — same shape as `description_keys` — and values are `bool` (CFBoolean keys, e.g. `{"DAMediaWhole", true}`) or `std::string` (CFString keys, e.g. `{"DAVolumeKind", "apfs"}`). Empty (default) = match every disk. |

## Things deliberately NOT done

- **Peek approval** (`DARegisterDiskPeekCallback`). Same shape as the
  three approval verbs we do support, but the use cases are narrow
  enough that nobody has asked. Add as a fourth `watch_options` field
  if needed.
- **Dual-session HOL isolation.** Approval callbacks share the
  notification dispatch queue today, so a slow notification consumer
  can delay an approval callback for an unrelated disk. Workaround:
  open a second `da_context` purely for approval — they run on
  independent DA sessions, independent queues. A future flag could
  bake this in (`watch_options::isolate_approval_queue`), but the
  workaround is cheap and the default keeps the wrapper simpler.
- **Multi-subscriber fan-out** on a single `da_context`. The
  `__active_` slot is single-shot CAS-guarded; a second concurrent
  `subscribe` fails with `set_error`. For fan-out, build a layer on top.
