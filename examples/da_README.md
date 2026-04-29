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
    dax::on_queue(pool.get_scheduler(),
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

## Queue selection / scheduler

The wrapper does not own a dispatch queue. The queue is selected at the
pipeline level via `dax::on_queue`:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("...");
sync_wait(dax::on_queue(pool.get_scheduler(), ctx.watch(opts)) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require a
`libdispatch_scheduler` in the receiver's env. Composing with any other
scheduler type is a compile error — same posture as the FSEvents
wrapper.

### Why `dax::on_queue` instead of `stdexec::starts_on`?

Same reason as FSEvents: `stdexec::starts_on` (and `stdexec::write_env`)
collapse sequence-sender attributes today, so downstream
`transform_each` loses the per-event type. `dax::on_queue` is a thin
adapter around the shared `exec::__on_scheduler_t` (defined in
`include/exec/on_scheduler.hpp` and also used by `fsx::on_queue` and
`rdcx::pool::on_pool`) that performs the env injection while
preserving sequence-sender semantics. See
`sequence_sender_on_scheduler.md` for the full explanation.

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
| **Approval callbacks not wired** | `DARegisterDiskMountApprovalCallback` and friends require the consumer to *answer* each event by returning a `DADissenterRef`. Not modeled in v1; see "Things deliberately NOT done". |
| **`description_changed` is opt-in** | Off by default — it can be very noisy (filesystem state changes, mount/unmount transitions all fire it). Set `watch_options::watch_description_changed = true` if you actually want it. |
| **Match dictionary fixed at NULL** | `watch()` does not yet expose a `CFDictionaryRef` filter, so all callbacks see every disk. Filtering is left to the consumer (e.g. `transform_each` over `bsd_name`). |

## Things deliberately NOT done

- **Approval callbacks** (mount/unmount/eject/peek). They are
  request/response — the C callback returns a `DADissenterRef` (or
  `NULL` to allow). A `sequence_sender` whose item is fire-and-forget
  does not naturally express this. A future PR can decide between a
  request/response sender pair and a per-event ack object.
- **Match-dictionary filtering** in `watch_options`. Easy to add when
  needed; not in v1.
- **Description-change key narrowing.** `DARegisterDiskDescriptionChangedCallback`
  takes a `CFArrayRef` of keys to watch — currently `NULL` (all keys).
  A future `watch_options::description_keys` can narrow it.
- **Multi-subscriber fan-out** on a single `da_context`. The
  `__active_` slot is single-shot CAS-guarded; a second concurrent
  `subscribe` fails with `set_error`. For fan-out, build a layer on top.
