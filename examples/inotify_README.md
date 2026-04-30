# inotify wrapper for stdexec

Linux-only example showing how to wrap `inotify` as an `exec::sequence_sender_t`
driven by `exec::io_uring_context` (`IORING_OP_READ` on the inotify fd).

## Files

| File | Role |
|---|---|
| `inotify_wrapper.hpp` | Header-only `inx::inotify_context` — io_uring-driven inotify sequence sender |
| `inotify.cpp`         | Demo: `inotify_wrapper.hpp` + `transform_each` + `ignore_all_values` + `when_any` |
| `inotify_README.md`   | This file |

Build (only configured under Linux):

```sh
cmake --build build --target example.inotify
./build/examples/example.inotify
```

The demo watches a temp directory (`/tmp/inx_demo`), spawns a mutator thread
that creates five files 400 ms apart, and stops the pipeline after 3 s via
`when_any` against a timer.

## API

```cpp
exec::io_uring_context ring;
std::thread            driver{[&] { ring.run_until_stopped(); }};

// Single path:
inx::inotify_context ctx{{"path/to/dir"}};

// Multiple paths:
inx::inotify_context ctx{{"path/to/a", "path/to/b"}};

// Optional per-context default mask override:
inx::inotify_context ctx{{"path/to/dir"}, IN_CREATE | IN_DELETE};

stdexec::sync_wait(
    inx::on_ring(ring.get_scheduler(), ctx.watch())
  | exec::transform_each(stdexec::then([&](inx::fs_batch b) {
        if (b.overflow) { /* rescan */ }
        for (const auto& e : b.events) {
            auto path = ctx.path_for(e.wd); // optional<string>
            std::printf("wd=%d mask=%#x name=%s\n",
                        e.wd, e.mask, e.name.c_str());
        }
    }))
  | exec::ignore_all_values());

ring.request_stop();
driver.join();
```

`ctx.watch()` accepts an optional `watch_options`:

```cpp
inx::watch_options opts{
    .mask        = IN_CREATE | IN_DELETE | IN_MODIFY,  // per-watch mask
    .buffer_size = 128 * 1024,                         // read buffer in bytes
};
ctx.watch(opts);
```

The default mask covers `IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MODIFY |
IN_ATTRIB | IN_MOVED_FROM | IN_MOVED_TO | IN_MOVE_SELF | IN_CLOSE_WRITE`.

Dynamic path management is thread-safe and can happen while the pipeline is
running:

```cpp
// Add a new path (uses context default mask unless overridden):
int wd = ctx.add_watch("path/to/new");
int wd2 = ctx.add_watch("path/to/other", IN_CREATE | IN_ISDIR);

// Remove a path:
ctx.remove_watch(wd);

// Resolve wd → path (mutex-protected):
auto p = ctx.path_for(e.wd); // std::optional<std::string>
```

Each `fs_batch` carries:
- `events` — a `std::span<const fs_event>` over the parsed events for this
  read completion. `IN_Q_OVERFLOW` synthetic events (`wd=-1`) are filtered out
  of the span and reflected as `overflow = true` instead.
- `overflow` — `true` if the kernel inotify queue overflowed; events may have
  been lost, requiring a rescan.

Each `fs_event` carries:
- `wd` — watch descriptor returned by `add_watch`
- `mask` — event flags (`IN_CREATE`, `IN_DELETE`, …)
- `cookie` — rename-pair correlation token (`IN_MOVED_FROM` / `IN_MOVED_TO`)
- `name` — filename relative to the watched directory; empty for self-events
  (`IN_DELETE_SELF`, `IN_MOVE_SELF`)

## Platform comparison

| | macOS (`fsevents_wrapper.hpp`) | Windows (`rdc_pool_wrapper.hpp`) | Linux (`inotify_wrapper.hpp`) |
|---|---|---|---|
| **Namespace** | `fsx` | `rdcx::pool` | `inx` |
| **Reactor scheduler** | `exec::libdispatch_queue` | `exec::windows_thread_pool` | `exec::io_uring_context` |
| **Env adapter** | `fsx::on_queue` | `rdcx::pool::on_pool` | `inx::on_ring` |
| **Source primitive** | `FSEventStreamCreate` | `ReadDirectoryChangesW` | `inotify_init1` + `IORING_OP_READ` |
| **Recursive** | Kernel-side | Kernel-side | Caller-side (walk + `add_watch`) |
| **Resume id** | `last_completed_id()` | None | None |

All three share: `sequence_sender_t`, one `fs_batch` per IO completion,
single-active subscription per context (CAS-guarded), continuation-style
backpressure, env-injected scheduler enforced at compile time, and native
cancellation routed through a stop callback.

## Pool selection / scheduler

The inotify wrapper does not own an io_uring ring. The ring is selected at the
pipeline level via `inx::on_ring`:

```cpp
exec::io_uring_context ring;
stdexec::sync_wait(inx::on_ring(ring.get_scheduler(), ctx.watch()) | ...);
```

`inx::on_ring` is an instance of the shared `exec::__on_scheduler_t` adapter
(the same type used by `fsx::on_queue` on macOS and `rdcx::pool::on_pool` on
Windows). It injects the scheduler into the receiver's env and preserves
sequence-sender semantics that `stdexec::starts_on` / `stdexec::write_env`
would currently collapse.

`__watch_sender::subscribe` is constrained at compile time to require an
`exec::io_uring_scheduler` in the receiver's env:

```cpp
template <stdexec::receiver _Rcvr>
  requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                     exec::io_uring_scheduler>
auto subscribe(_Rcvr rcvr) const -> __op<_Rcvr>;
```

Composing with any other scheduler type is a compile error — this prevents
silently routing inotify completions through an unintended executor. See
[`sequence_sender_on_scheduler.md`](sequence_sender_on_scheduler.md) for the
full explanation; the same pattern is used by `fsx::on_queue` on macOS and
`rdcx::pool::on_pool` on Windows.

### How `__op` binds to the user's ring

In the constructor, `__op` calls `stdexec::get_scheduler(get_env(rcvr))` and
stores the scheduler's internal `__context_` pointer. Every `__io_task_facade`
instance (`__read_op_`, `__cancel_op_`, `__finalize_op_`) is constructed with
that context pointer, so all SQE submissions and CQE deliveries happen on the
caller's ring. There is no fallback — if the env contains no
`io_uring_scheduler` the `subscribe` constraint rejects the composition at
compile time.

## What happens under the hood

`__op` owns three `optional` io_uring task facades:

| Facade | SQE type | Role |
|---|---|---|
| `__read_op_` | `IORING_OP_READ` | in-flight read from the inotify fd |
| `__cancel_op_` | `IORING_OP_ASYNC_CANCEL` | issued when stop is requested |
| `__finalize_op_` | `IORING_OP_NOP` | deferred-finalize trampoline (see below) |

### Batch lifecycle

```
inotify_init1(IN_CLOEXEC | IN_NONBLOCK) + initial inotify_add_watch (ctor)
        │
        ▼
submit IORING_OP_READ(inotify_fd, buf) ◄────────────────┐
        │                                               │
        ▼                                               │
CQE on io_uring reactor thread                          │
  · res < 0:  -ECANCELED → request_finalize(stopped)    │
               other     → request_finalize(error)      │
  · res ≥ 0:  parse buffer into vector<fs_event>         │
        │                                               │
        ▼                                               │
  · IN_Q_OVERFLOW (wd=-1) → batch.overflow = true       │
  · IN_IGNORED → erase wd from wd→path map (mutex)      │
        │                                               │
        ▼                                               │
set_next(rcvr, just(fs_batch{...}))                     │
        │                                               │
        ▼                                               │
downstream pipeline (sync OR async)                     │
        │                                               │
        ▼                                               │
next_receiver::set_value ───────────────────────────────┘
        │
        ▼ (set_stopped / set_error / stop_token fired)
request_finalize(...) — CAS-gated, submits IORING_OP_NOP
        │
        ▼
NOP CQE arrives in a fresh reactor frame
        │
        ▼
__finalize_and_complete: drop stop_cb, reset all facades, set_stopped/set_error(rcvr)
```

All "want to finish" triggers — a negative read result, `next_receiver::set_stopped`,
`next_receiver::set_error`, and the stop callback — funnel through a single
`__request_finalize` call. A CAS on `__finalize_scheduled_` ensures only one
`IORING_OP_NOP` is ever submitted. The NOP CQE arrives on the reactor in a
fresh stack frame, which is the unique safe site for `__finalize_and_complete`:
it avoids self-destruction of `__next_op_` from inside its own
`set_value`/`set_stopped`/`set_error` chain and correctly handles both
synchronous and asynchronous downstream pipelines. This mirrors
`rdc_pool_wrapper.hpp`'s `__schedule_cleanup` + `SubmitThreadpoolWork` pattern,
with the `IORING_OP_NOP` SQE serving as the Linux analogue.

`__pending_cqes_` (atomic counter) tracks every in-flight CQE (read +
optional cancel + NOP). The NOP handler calls `__finalize_and_complete` only
when the counter reaches zero, guaranteeing the cancel facade outlives its CQE
delivery.

## Backpressure

Backpressure is continuation-style. The next `IORING_OP_READ` is submitted
from `next_receiver::set_value`, not after `set_next` returns. While the
downstream pipeline is processing a batch, no SQE is in flight; the kernel
inotify queue accumulates events in the meantime.

The kernel queue depth is bounded by
`/proc/sys/fs/inotify/max_queued_events` (typical default: 16384 events).
When the queue overflows, the kernel emits a synthetic event with `wd=-1` and
`mask=IN_Q_OVERFLOW`. The wrapper detects this, sets `fs_batch::overflow =
true`, and filters the synthetic entry out of the `events` span — the caller
must perform a full rescan when `overflow` is set. Events queued before the
overflow are delivered normally; any events that did not fit are permanently
lost.

This is the Linux analogue of FSEvents' `MustScanSubDirs` flag and
`ReadDirectoryChangesW`'s `bytes_returned == 0`.

Increase `watch_options::buffer_size` (default 64 KiB) to absorb larger
event bursts between reads. The kernel queue depth is the actual bottleneck;
a larger read buffer only helps if multiple kernel events can be coalesced
into one `IORING_OP_READ` completion.

## Cancellation

```
upstream stop_token ──► __on_stop_fn
                              │
                              ▼
                    __stop_requested_ = true
                              │
                              ▼
                    Load __read_user_data_ (atomic acquire)
                              │
                              ▼ (if non-null)
                    Submit IORING_OP_ASYNC_CANCEL targeting that user_data
                              │
                              ▼
                    Read CQE arrives with res = -ECANCELED
                              │
                              ▼
                    __on_read_complete → __request_finalize(stopped)
                              │
                              ▼
                    Cancel CQE arrives → __on_cancel_complete (dec counter)
                              │
                              ▼
                    NOP CQE arrives → __on_finalize_complete → set_stopped(rcvr)
```

`__on_stop_fn` reads `__read_user_data_` with acquire ordering to get the
in-flight `IORING_OP_READ` facade's `__task*` without racing
`__post_read`'s concurrent emplace. A stale `nullptr` (read before
`__post_read` publishes) is safe — the kernel returns `-ENOENT` for a missed
cancel target, and the read will complete normally on the next CQE.

If downstream does not propagate `stop_token` while a batch is in flight, the
pipeline stalls until the downstream completes. `then`, `transform_each`, and
`bulk` all propagate stop, so this is rare in practice.

## Recursive watching

inotify is not recursive. The kernel watches a single directory inode; events
in subdirectories do not appear unless those subdirectories are also watched
individually. The caller is responsible for walking the tree and calling
`add_watch` on each subdirectory.

```cpp
namespace fs = std::filesystem;

// Walk the tree and watch every subdirectory.
void watch_tree(inx::inotify_context& ctx,
                const fs::path&       root,
                std::uint32_t         mask = IN_CREATE | IN_DELETE
                                           | IN_ISDIR  | IN_MODIFY)
{
    ctx.add_watch(root.string(), mask);
    for (const auto& entry : fs::recursive_directory_iterator{root}) {
        if (entry.is_directory()) {
            ctx.add_watch(entry.path().string(), mask);
        }
    }
}
```

Then, when the pipeline receives an `IN_CREATE | IN_ISDIR` event for a new
subdirectory, add it dynamically:

```cpp
for (const auto& e : batch.events) {
    if ((e.mask & IN_CREATE) && (e.mask & IN_ISDIR)) {
        auto root = ctx.path_for(e.wd);
        if (root) {
            ctx.add_watch(*root + "/" + e.name,
                          IN_CREATE | IN_DELETE | IN_ISDIR | IN_MODIFY);
        }
    }
}
```

**Unavoidable race**: there is a window between the initial directory walk and
the first `add_watch` calls during which events in those subdirectories may be
missed. A newly-created subdirectory that is populated before the walk reaches
it will not have a watch yet when those inner events fire. This is why FSEvents
and `ReadDirectoryChangesW` implement recursion in the kernel (`watch_subtree =
true`) and inotify cannot match that guarantee from user space.

The practical mitigation is to schedule a rescan after the initial
`watch_tree` call, and to treat the watch as "best effort" for the first few
milliseconds.

You do **not** need to call `remove_watch` when a watched directory is
deleted by the filesystem (or unmounted, or otherwise auto-removed by
the kernel). The kernel emits `IN_DELETE_SELF` followed by
`IN_IGNORED` for the affected `wd`; the wrapper's `IN_IGNORED` handler
already erases the entry from the wd→path map under the mutex. Only
call `remove_watch` when you have decided you no longer want a watch
that is still alive.

## inotify quirks worth knowing

| Quirk | Detail |
|---|---|
| **Not recursive** | Each `inotify_add_watch` watches exactly one directory; caller must walk and recurse (see above). |
| **`name` is NUL-padded, not NUL-terminated** | The `len` field is the padded record length; the wrapper uses `strnlen(ev->name, ev->len)` to extract the real string. |
| **`name` is empty for self-events** | `IN_DELETE_SELF` and `IN_MOVE_SELF` report an event on the watched path itself; `fs_event::name` is empty. |
| **`IN_IGNORED` auto-cleans the map** | When the kernel drops a watch (file deleted, `inotify_rm_watch` called), it delivers `IN_IGNORED`. The wrapper erases the `wd→path` map entry under the mutex; `path_for(wd)` returns `nullopt` from that point on. |
| **`IN_Q_OVERFLOW` becomes `fs_batch::overflow`** | The synthetic overflow event (`wd=-1`) is filtered out of `events` and reflected as `overflow = true`. Events queued before the overflow are still delivered; everything after is gone — rescan required. |
| **Repeated `add_watch` on the same path replaces the mask** | Without `IN_MASK_ADD`, a second `inotify_add_watch` call on the same path is equivalent to a mask update; the wrapper does not OR masks automatically. |
| **Inode-level, not path-level** | The watch is on the inode, not the path. Hard links to the same inode generate events on whichever watch descriptor was registered first. Renaming the watched directory does not invalidate the watch, but the stored path in `wd→path` becomes stale. |
| **Buffer alignment** | The wrapper uses `std::vector<std::uint64_t>` (alignment ≥ 8) as the read buffer. `::inotify_event` requires at least 4-byte alignment for its `int wd` / `uint32_t` fields; a plain `vector<char>` is only guaranteed `alignof(char) == 1`. Same trick as `rdc_pool_wrapper.hpp`'s `vector<DWORD>`. |
| **`O_NONBLOCK` on the inotify fd** | The fd is opened with `inotify_init1(IN_CLOEXEC | IN_NONBLOCK)`. The wrapper never reads it synchronously — all reads go through `IORING_OP_READ`, which does not require `O_NONBLOCK`, but the flag prevents accidental blocking if the fd is ever used outside the wrapper. |

## Things deliberately NOT done

- **Recursive subtree watching**: caller-side only (see above). No built-in
  `watch_subtree` flag; the kernel does not support it for inotify.
- **fanotify backend**: `fanotify` requires `CAP_SYS_ADMIN` (or
  `CAP_AUDIT_CONTROL` in some configurations), has an asymmetric feature
  surface (permission events, mount-wide scopes), and adds significant
  complexity for the common file-watching use case. Out of scope here.
- **`IORING_OP_READ_MULTISHOT`**: would avoid re-submitting the SQE after
  each read completion, but requires provided-buffers rings (`IORING_OP_PROVIDE_BUFFERS`)
  and breaks the per-batch backpressure alignment (multiple CQEs per SQE).
  Future optimization only.
- **Multi-subscriber fan-out**: `__active_` is single-slot, CAS-guarded.
  Multiple concurrent watches on the same context fail with `set_error`.
  For fan-out, build a layer on top.
- **`IN_MASK_ADD` semantics**: callers control mask composition; the wrapper
  passes the mask directly to `inotify_add_watch`.
- **Resume / replay**: inotify has no event-id analogue. There is no
  `last_completed_id()`. After a process restart, rescan the watched paths
  and diff file metadata to reconstruct what changed while down.
- **Unit tests**: structural parity with the FSEvents and RDC examples; the
  same test-infrastructure decisions apply.
