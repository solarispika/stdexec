# FSEvents wrapper for stdexec

macOS-only example showing how to wrap a callback-driven event source
(`FSEventStream`) as an `exec::sequence_sender_t`, plus a coroutine
consumer built on top of the same wrapper.

## Files

| File | Role |
|---|---|
| `fsevents_wrapper.hpp` | Header-only `fsx::fsevents_context` — the sequence sender |
| `fsevents.cpp` | Demo: consume via `transform_each` + `ignore_all_values` |
| `fsevents_coro.cpp` | Demo: consume via `exec::task` coroutine over an awaitable channel |
| `fsevents_README.md` | This file |

Build (only configured under `APPLE`):

```sh
cmake --build build --target example.fsevents example.fsevents_coro
./build/examples/example.fsevents
./build/examples/example.fsevents_coro
```

Both demos write to / watch a temp directory (`fsx_demo`, `fsx_demo_coro`).

## API

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("my.pool");
fsx::fsevents_context ctx{{"/path/to/dir"}};

stdexec::sync_wait(
    exec::sequence_with_scheduler(
        pool.get_scheduler(),
        ctx.watch({.since = kFSEventStreamEventIdSinceNow,
                   .latency = 0.2,
                   .create_flags = kFSEventStreamCreateFlagFileEvents
                                 | kFSEventStreamCreateFlagNoDefer
                                 | kFSEventStreamCreateFlagWatchRoot}))
  | exec::transform_each(stdexec::then([](fsx::fs_batch b){ ... }))
  | exec::ignore_all_values());
```

Each `fs_batch` carries `events` (span of `fs_event{path, flags, id}`),
`last_id`, `had_drops`, `must_rescan`. `ctx.last_completed_id()` advances
**only after** the next sender for a batch completes — safe to persist as
a resume point.

## Queue selection / scheduler

The wrapper does not own a dispatch queue. The queue is selected at the
pipeline level via `exec::sequence_with_scheduler`:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("...");
sync_wait(exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch(opts)) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require a
`libdispatch_scheduler` in the receiver's env. Composing with any other
scheduler type is a compile error — this prevents silently falling back
to a default queue when the caller intended e.g. a `static_thread_pool`.

### Why `exec::sequence_with_scheduler` instead of `stdexec::starts_on`?

Short version: `stdexec::starts_on` (and `stdexec::write_env`) collapse
sequence-sender attributes today, so downstream `transform_each` loses
the per-batch type. `exec::sequence_with_scheduler` is a small shared
adapter (in `include/exec/on_scheduler.hpp`) that does just the env
injection while preserving sequence-sender semantics. See
[`sequence_sender_on_scheduler.md`](sequence_sender_on_scheduler.md)
for the full explanation; the same adapter is used by all the example
wrappers (DA, inotify, RDC pool, velx).

### Internal serial queue

Each watch operation creates its own serial queue with the user's queue
as target (`dispatch_queue_create_with_target`). The serial attribute is
required by the wrapper's callback ↔ teardown serialization idiom; the
worker thread comes from the user's pool.

## What happens under the hood

```
fseventsd ──callback──► dispatch queue ──set_next──► next sender
                              │                           │
                              │                           ▼
                              ▼                       (downstream
                       semaphore.acquire ◄── set_value  pipeline)
                              │
                              ▼
                      next batch processed
```

Single active subscription per context (CAS-guarded). Stream lifecycle
(`Create` / `Start` / `Stop` / `Invalidate` / `Release`) is owned by the
operation state, with all teardown serialized through the dispatch queue
so it can never race with an in-flight callback.

## Backpressure

The callback synchronously blocks on `__delivery_done_` (a
`std::binary_semaphore`) until the next sender completes. This gives you
"natural" backpressure for free: the kernel keeps producing events,
`fseventsd` keeps coalescing them per `latency`, and the next callback
fires with a **larger** batch the slower the consumer is. No explicit
queue.

Limits: the kernel's per-stream buffer is finite. When it overflows,
`fseventsd` sets `kFSEventStreamEventFlagUserDropped` /
`kFSEventStreamEventFlagKernelDropped` /
`kFSEventStreamEventFlagMustScanSubDirs` on a synthesizing event. The
wrapper surfaces these as `fs_batch::had_drops` / `must_rescan` and via
`fsx::is_drop_notice(event)` — **always handle them**, otherwise slow
consumers silently lose events.

## Cancellation

```
upstream stop_token ──► __on_stop_fn ──► dispatch_async_f to queue
                              │                     │
                              ▼                     ▼
                  __stop_requested_ = true   FSEventStreamStop/Invalidate/Release
                              │                     │
                              │                     ▼
                              │           set_stopped(outer rcvr)
                              ▼
                  in-flight next sender's stop_token (via env)
                  propagates → next_receiver::set_stopped → callback unblocks
```

If a downstream operation does **not** propagate `stop_token` (rare —
`then`, `transform_each`, `bulk` etc. all do), the callback can deadlock
holding the semaphore. Document this in user-facing wrappers.

## FSEvents quirks worth knowing

| Quirk | Detail |
|---|---|
| **Resolved paths** | `FSEventStreamCreate` matches against canonical paths. `/var/folders/…` will silently produce zero events; you need `/private/var/folders/…`. The demos call `fs::canonical(dir)` first. |
| **`NoDefer` flag** | First event in a quiet stream delivers immediately; later events coalesce within `latency`. Without it, every batch is delayed by `latency`. |
| **`latency = 0`** | Pretty much always wrong — you lose the daemon-side coalescing that makes batches large enough to amortize callback overhead. Use ≥ 100 ms. |
| **`since` too old** | If the event id you resume from is older than what `fseventsd` retained on disk, the **first batch** will carry `kFSEventStreamEventFlagMustScanSubDirs`. Caller must rescan the tree manually. |
| **`RootChanged` flag** | Watched root was deleted/moved. The wrapper currently surfaces it via `must_rescan`; you might want to escalate to `set_error` instead. |
| **Callback paths buffer** | Without `kFSEventStreamCreateFlagUseCFTypes`, `eventPaths` is `char**`. The strings are valid only during the callback — copy out before suspending. The wrapper deep-copies into a `vector<fs_event>` per batch. |

## Sender wrapper vs coroutine wrapper

Short version: **wrapper = sender, consumer = either**. The wrapper is
the more general abstraction because:

- A sequence-sender wrapper composes with all of stdexec's algorithms
  (`transform_each`, `merge_each`, `ignore_all_values`, …) and can also
  feed a coroutine consumer via a small channel bridge (see
  `fsevents_coro.cpp`).
- An `async_generator`-style coroutine wrapper would lock callers into
  `co_await`, and re-entering the sender world requires a bridge again.
- Code volume is roughly the same either way: the boilerplate just
  shifts between "sender concept tax" and "channel + generator
  primitive".

The exception: if your codebase is already coroutine-native and has a
stop-token-aware async channel + async generator primitive, a direct
coroutine wrapper can be ~50 lines.

## Known gotcha: coroutine consumer under nested `when_any`

The original `fsevents_coro.cpp` had:

```cpp
sync_wait(when_any(
    starts_on(sched, just()) | then([]{ sleep_for(3s); }),
    when_all(producer_pipeline,
             starts_on(sched, consume(ch)) | then(...))));
```

Symptoms: first batch consumed correctly. Second `co_await ch.pop()`
calls `set_value` on the receiver chain (verifiable via tracing) but the
coroutine never resumes — push #2 hand-offs deliver, but the body after
`co_await` does not run. Subsequent pushes pile up in the channel slot
and eventually block.

The trigger appears to be the interaction between:

1. `exec::task`'s sticky-scheduler behavior (each `co_await` wraps the
   awaited sender in `continues_on(sender, captured_scheduler)`), and
2. `when_any`'s cancellation propagation through the nested `when_all`
   plus
3. the scheduler being a `static_thread_pool` whose threads are partly
   committed to the timer.

I did not isolate the exact cause. The working layout (current
`fsevents_coro.cpp`) avoids it by:

- Running the producer pipeline on a dedicated `std::thread` with its
  own `sync_wait(when_any(timer, pipeline))`, so push-side cancellation
  is local.
- Driving the coroutine via a flat `sync_wait(consume(ch) | then(...))`
  on the main thread — no `starts_on`, no `when_all` nesting.
- The producer thread calls `ch.close()` after its `sync_wait` returns,
  which wakes any blocked `pop()`.

If you re-introduce a sticky scheduler around `consume()`, retest end-to-end:
the silent-drop behavior is the bug to watch for.

## Things deliberately NOT done

- **Multi-subscriber**: `__active_` is single-slot, CAS-guarded. Multiple
  concurrent watches on the same context fail with `set_error`. For
  fan-out, build a layer on top.
- **`kFSEventStreamCreateFlagUseExtendedData`**: useful for inode info
  but adds CFDictionary parsing per event; not wired up.
- **Drop-event demo**: producing genuine `UserDropped` requires forcing
  `fseventsd` to fall behind (e.g., a slow consumer with thousands of
  events/sec). Out of scope for these demos.
- **Stale-`since` recovery helper**: caller is expected to detect
  `must_rescan` on the first batch and walk the tree themselves.
- **Resume-from-disk demo**: the wrapper supports it (`watch_options::since`
  + `last_completed_id()`), but neither demo persists across runs.
