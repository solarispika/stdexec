# ReadDirectoryChangesW wrapper for stdexec

Windows-only example showing how to wrap a callback-driven event source
(`ReadDirectoryChangesW`) as an `exec::sequence_sender_t`. Two variants
share the same public API and differ only in how IO completions are
scheduled.

## Files

| File | Role |
|---|---|
| `rdc_wrapper.hpp`      | Header-only `rdcx::rdc_context` — dedicated worker thread per watch |
| `rdc_pool_wrapper.hpp` | Header-only `rdcx::pool::rdc_context` — Win32 thread-pool driven |
| `rdc.cpp`              | Demo: `rdc_wrapper.hpp` + `transform_each` + `ignore_all_values` |
| `rdc_pool.cpp`         | Demo: `rdc_pool_wrapper.hpp` + `windows_thread_pool` + `on_pool` |
| `rdc_README.md`        | This file |

Build (only configured under `WIN32`):

```sh
cmake --build build --target example.rdc example.rdc_pool
./build/examples/example.rdc
./build/examples/example.rdc_pool
```

Both demos write to / watch a temp directory (`rdcx_demo`,
`rdcx_pool_demo`).

## API

Dedicated-thread variant:

```cpp
rdcx::rdc_context ctx{L"C:\\path\\to\\dir"};

stdexec::sync_wait(
    ctx.watch({.watch_subtree = true,
               .filter        = FILE_NOTIFY_CHANGE_FILE_NAME
                              | FILE_NOTIFY_CHANGE_DIR_NAME
                              | FILE_NOTIFY_CHANGE_LAST_WRITE
                              | FILE_NOTIFY_CHANGE_SIZE,
               .buffer_size   = 64 * 1024})
  | exec::transform_each(stdexec::then([](rdcx::fs_batch b){ ... }))
  | exec::ignore_all_values());
```

Pool variant:

```cpp
exec::windows_thread_pool        pool{2, 4};
rdcx::pool::rdc_context          ctx{L"C:\\path\\to\\dir"};

stdexec::sync_wait(
    rdcx::pool::on_pool(pool.get_scheduler(), ctx.watch(opts))
  | exec::transform_each(stdexec::then([](rdcx::pool::fs_batch b){ ... }))
  | exec::ignore_all_values());
```

Each `fs_batch` carries `events` (span of `fs_event{path, action}`) and
an `overflow` flag. `path` is wide-char and **relative** to the watched
root. `action` is one of `FILE_ACTION_ADDED` / `REMOVED` / `MODIFIED` /
`RENAMED_OLD_NAME` / `RENAMED_NEW_NAME`. There is no resume-id analogue
to FSEvents' `last_completed_id()` — Win32 does not expose one.

## Pool selection / scheduler (pool variant)

The pool wrapper does not own a thread pool. The pool is selected at the
pipeline level via `rdcx::pool::on_pool`:

```cpp
exec::windows_thread_pool pool{2, 4};
sync_wait(rdcx::pool::on_pool(pool.get_scheduler(), ctx.watch(opts)) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require a
`windows_thread_pool::scheduler` in the receiver's env. Composing with
any other scheduler type is a compile error — this prevents silently
falling back to the process default pool when the caller intended e.g.
a `static_thread_pool`.

### Why `rdcx::pool::on_pool` instead of `stdexec::starts_on`?

Same reason as `fsx::on_queue` on the libdispatch side:
`stdexec::starts_on(sched, child)` rewrites the child via the
regular-sender path (`__sequence(continues_on(just(), sched), child)`)
which strips `item_types` and other sequence-sender attributes. The
adapter (~80 LoC, in `rdc_pool_wrapper.hpp`) wraps the receiver to expose
`get_scheduler -> windows_thread_pool::scheduler` in its env without
losing sequence-sender semantics. Tracked upstream in
`docs/plans/2026-04-29-stdexec-write_env-sequence-sender-issue.md`; once
fixed, `on_pool` collapses to plain `stdexec::write_env`.

### How `__op` binds to the user's pool

In ctor, `__op` initializes its own `TP_CALLBACK_ENVIRON` and calls
`SetThreadpoolCallbackPool(&env, sched.native_handle())`. All
`CreateThreadpoolIo` / `CreateThreadpoolWork` calls use that env, so
both the IO callback and the deferred-cleanup work item run on the
user's pool. `nullptr` from `native_handle()` is the documented sentinel
for "process default pool" and is what the default-constructed
`windows_thread_pool` returns — same behaviour as before the env-driven
refactor.

## What happens under the hood

### Dedicated-thread variant

```
ReadDirectoryChangesW ──► GetOverlappedResult (worker thread) ──► set_next ──► next sender
                                                                       │           │
                                                                       │           ▼
                                                                       ▼       (downstream
                                                              semaphore.acquire ◄── set_value
                                                                       │
                                                                       ▼
                                                              repost ReadDirectoryChangesW
```

The worker thread is owned by the operation state. Only one IO is ever
pending, so no IOCP — `GetOverlappedResult(__dir_, ..., bWait=TRUE)` is
fine.

### Pool variant

```
ReadDirectoryChangesW ──► PTP_IO completion (pool worker) ──► set_next ──► next sender
                                                                       │           │
                                                                       │           ▼
                                                                       ▼       (downstream
                                                              return to pool      pipeline)
                                                                       │
                                                                       ▼
                                                              next_receiver::set_value
                                                                       │
                                                                       ▼
                                                              StartThreadpoolIo + repost
                                                                       │
                                                                       ▼
                                                              future PTP_IO completion
```

No worker thread is owned. The IO callback never blocks — backpressure
is continuation-style: the next `ReadDirectoryChangesW` is posted from
`next_receiver::set_value`, not after a semaphore acquire. Cleanup is
deferred to a separate `PTP_WORK` because
`WaitForThreadpoolIoCallbacks` cannot be called from inside an IO
callback (deadlock).

Single active subscription per context, CAS-guarded, in both variants.
HANDLE / `PTP_IO` / `PTP_WORK` are owned by the operation state.

## Backpressure

| Variant | Mechanism |
|---|---|
| Dedicated thread | Worker thread acquires `std::binary_semaphore` after `set_next`, blocks until next-receiver fires. No new Read posted while busy. |
| Pool | Continuation-style: next Read posted from `next_receiver::set_value`. While downstream is processing, no Read is pending. |

Both give "natural" backpressure: between an in-flight batch and its
completion, no new IO is posted, so the kernel buffer fills. If it
overflows, `bytes_returned == 0` on the next completion and we deliver a
batch with `overflow = true` (rescan required) — the RDC analogue of
FSEvents' `MustScanSubDirs`.

The kernel's per-handle buffer is finite (and not directly configurable
from user space). For high-event-rate workloads, increase
`watch_options::buffer_size` to give the kernel more room to queue
events between Reads.

## Cancellation

```
upstream stop_token ──► __on_stop_fn
                              │
                              ▼
                   __stop_requested_ = true
                              │
                              ▼
                   CancelIoEx(__dir_, &__ovl_)
                              │
                              ▼
              pending Read completes with ERROR_OPERATION_ABORTED
                              │
                              ▼
              completion path schedules cleanup → set_stopped(rcvr)
```

If a downstream operation does not propagate `stop_token` while a batch
is in flight, the wrapper stalls (same hazard as the FSEvents wrapper):
in the dedicated-thread variant the worker holds the semaphore; in the
pool variant the continuation chain stops being scheduled. `then`,
`transform_each`, `bulk` all propagate stop, so this is rare in practice.

## ReadDirectoryChangesW quirks worth knowing

| Quirk | Detail |
|---|---|
| **`FILE_FLAG_OVERLAPPED` required** | RDC will not return `ERROR_IO_PENDING` without it; everything else in the wrapper assumes async IO. |
| **`FILE_FLAG_BACKUP_SEMANTICS` required** | Needed to open a directory handle. Not optional. |
| **`FILE_LIST_DIRECTORY` access** | The desired-access mask. `GENERIC_READ` is wrong here. |
| **`FILE_SHARE_DELETE`** | Without it, the directory cannot be renamed or deleted while the watch is active. |
| **Buffer must be `DWORD`-aligned** | The wrapper holds a `std::vector<DWORD>` and reinterprets to bytes for that reason. Plain `vector<char>` is not guaranteed `alignof(DWORD)`. |
| **`FILE_NOTIFY_INFORMATION::FileName` is not null-terminated** | `FileNameLength` is in bytes; `/sizeof(WCHAR)` for character count. The wrapper constructs `wstring` from `(FileName, length)`. |
| **`bytes_returned == 0`** | The kernel buffer overflowed; *all* events for this completion are gone. The wrapper surfaces this as `fs_batch::overflow` — handle it (rescan), otherwise events are silently lost. |
| **Renames** | Surface as a pair of events: `RENAMED_OLD_NAME` then `RENAMED_NEW_NAME`. Atomic only relative to the same notification batch — across batches you can see one without the other on overflow. |
| **`watch_subtree`** | Recursive watching is "free" — RDC does it kernel-side. But in deep trees, the kernel buffer fills faster (every event in any subdir lands here). Tune `buffer_size` accordingly. |
| **Synchronous failure** | `ReadDirectoryChangesW` returning `FALSE` with `GetLastError() != ERROR_IO_PENDING` means no callback will fire. The wrapper detects this and delivers `set_error`. |

## Variant comparison

| | Dedicated thread (`rdc_wrapper.hpp`) | Pool (`rdc_pool_wrapper.hpp`) |
|---|---|---|
| Threads owned | 1 per active watch | 0 (shared pool) |
| Backpressure mechanism | Semaphore on worker thread | Continuation chain |
| User-supplied scheduler | None — owns its own thread | Required via env (`on_pool`) |
| Complexity | Lower — synchronous loop | Higher — `PTP_IO` lifecycle, deferred cleanup, env propagation |
| Scales to many watches | Poorly — one thread each | Well — pool workers shared |
| Cancellation latency | Immediate (`CancelIoEx` interrupts `GetOverlappedResult`) | Immediate, but cleanup work item adds one pool-dispatch hop |

For one or two watches the dedicated-thread variant is simpler and the
pool overhead doesn't pay off. For many concurrent watches (a
file-indexer scanning hundreds of directories), the pool variant is the
right choice.

## Things deliberately NOT done

- **Multi-subscriber**: `__active_` is single-slot, CAS-guarded.
  Multiple concurrent watches on the same context fail with `set_error`.
  For fan-out, build a layer on top.
- **`ReadDirectoryChangesExW`**: returns `FILE_NOTIFY_EXTENDED_INFORMATION`
  with file size / timestamps / FileID, available since Windows 10 1709.
  Not wired up; would just be a swap of struct + filter constants.
- **Resume / replay**: RDC has no event-id analogue, so neither wrapper
  exposes a `last_completed_id()`. After process restart you must
  rescan; you can hash file metadata to detect what changed while you
  were down.
- **`FILE_NOTIFY_CHANGE_SECURITY`**: not in the default `filter`;
  caller can opt in via `watch_options::filter`.
- **Network shares**: `ReadDirectoryChangesW` works on local volumes;
  behaviour over SMB depends on the server. Not tested; treat as
  out-of-scope.
- **Long-path support**: `\\?\` prefix should work but is not exercised
  by the demos. Caller's responsibility to compose the path correctly.
