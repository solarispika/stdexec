# DiskArbitration sender — design

Date: 2026-04-29
Branch: `fsevents-sender-wrapper` (DA wrapper added on top of the FSEvents redesign)
Scope: `examples/da_wrapper.hpp`, `examples/da.cpp`, `examples/da_README.md`,
`test/exec/test_da_wrapper.cpp`

## Motivation

Apply the FSEvents × `libdispatch_queue` pattern (see
`2026-04-29-fsevents-libdispatch-redesign-design.md` and the
`libdispatch_sequence_sender_pattern` memory) to DiskArbitration as the
second concrete `sequence_sender` over a libdispatch-callback-driven
event source. This validates the pattern is reusable and gives users a
ready-to-use disk event stream.

## Decisions

### 1. Same four invariants as FSEvents wrapper

1. Per-op private serial dispatch queue, target = user's
   `libdispatch_scheduler` queue. Wrapper never hands the user's queue
   to `DASessionSetDispatchQueue` directly.
2. `__watch_sender::subscribe` requires `libdispatch_scheduler` in the
   receiver env via `exec::__env_has_scheduler`. Compile-time error
   otherwise — no silent fallback.
3. `dax::on_queue` reuses `exec::__on_scheduler_t` (the shared
   env-injection adapter in `include/exec/on_scheduler.hpp` that
   preserves sequence-sender semantics).
4. Single in-flight callback + serial queue + `binary_semaphore`
   backpressure. DA delivers callbacks serialized on the assigned
   dispatch queue, so the same FSEvents idiom works.

### 2. Scope: observation-only, no approval callbacks

Subscribed events:

- `DARegisterDiskAppearedCallback`
- `DARegisterDiskDisappearedCallback`
- `DARegisterDiskDescriptionChangedCallback` (opt-in via
  `watch_options::watch_description_changed`)

Excluded from v1:

- `DARegisterDiskMountApprovalCallback`,
  `DARegisterDiskUnmountApprovalCallback`,
  `DARegisterDiskEjectApprovalCallback`,
  `DARegisterDiskPeekCallback`.

The approval family is request/response — the callback synchronously
returns a `DADissenterRef` (or `NULL` to allow). That requires the
consumer to *answer* each event, not just *observe* it. A
`sequence_sender` whose item is a fire-and-forget batch does not
naturally express this; it would force a side-channel for the answer
and break the existing single-in-flight backpressure invariant. Punted
to a follow-up PR that can decide whether to model it as a
request/response sender pair or expose the dissenter via a per-batch
acknowledgement object.

### 3. Item type: `disk_event` (single), not a batch

FSEvents naturally batches because `fseventsd` coalesces events on a
`latency` window. DA fires one callback per disk transition. The
sender's item type is therefore a single `disk_event`:

```cpp
struct disk_event {
  disk_event_kind            kind;            // appeared / disappeared / description_changed
  std::string                bsd_name;        // e.g. "disk7s1"
  std::optional<std::string> volume_name;
  std::optional<std::string> volume_path;
  std::vector<std::string>   changed_keys;    // populated only for description_changed
};
```

`disk_event` is owned (deep-copies of CFString contents) — the
`DADiskRef` and `CFArrayRef` from the C callback are valid only inside
the callback frame.

### 4. Per-op session ownership

Each `__op<_Rcvr>` creates its own `DASessionRef` and registers its own
callbacks. Session create / register / `DASessionSetDispatchQueue` runs
in `start()`. Teardown runs from inside the dispatch queue (via
`dispatch_async_f`), serialized after any in-flight callback:

```
DASessionSetDispatchQueue(session, NULL)   // detach queue first
DAUnregisterCallback(session, &__on_appeared, this)
DAUnregisterCallback(session, &__on_disappeared, this)
DAUnregisterCallback(session, &__on_description_changed, this)  // if registered
CFRelease(session)
```

Match dictionary is `nullptr` (matches all disks) for v1. A future
`watch_options::match` can pass a `CFDictionaryRef` filter.

## API

```cpp
namespace dax {
  enum class disk_event_kind { appeared, disappeared, description_changed };

  struct disk_event { ... };

  struct watch_options {
    bool watch_appeared{true};
    bool watch_disappeared{true};
    bool watch_description_changed{false};
  };

  class da_context {
   public:
    da_context() = default;
    ~da_context() = default;
    da_context(const da_context&) = delete;
    auto operator=(const da_context&) -> da_context& = delete;

    auto watch(watch_options = {}) -> __detail::__watch_sender;

   private:
    std::atomic<__detail::__op_base*> __active_{nullptr};  // single-active CAS
  };

  inline constexpr exec::__on_scheduler_t on_queue{};
}
```

Pipeline shape (mirrors fsevents):

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("dax.demo");
dax::da_context ctx;

stdexec::sync_wait(
    dax::on_queue(pool.get_scheduler(), ctx.watch())
  | exec::transform_each(stdexec::then([](dax::disk_event e){ ... }))
  | exec::ignore_all_values());
```

## Testing strategy

### T2 — `hdiutil`-driven demo (`examples/da.cpp`)

Mirrors `examples/fsevents.cpp`'s "mutator thread + observer pipeline"
shape:

1. `hdiutil create -size 1m -fs HFS+ -volname dax_demo /tmp/dax_demo.sparseimage`
2. Subscribe the sender on a `make_concurrent` pool.
3. From a worker thread: short delay → `hdiutil attach -nomount …`
   (parses stdout for `/dev/diskN`) → delay → `hdiutil detach /dev/diskN`.
4. `when_any` with a timer (e.g. 5 s) bounds the run.
5. Cleanup the sparseimage on exit.

No root, no real device. Reproduces appeared/disappeared end-to-end.

### T3 — structural unit tests (`test/exec/test_da_wrapper.cpp`)

Pure wrapper invariants, no DA events triggered:

- **Compile-time env constraint.** `requires`-based concept asserts
  that `subscribe` is callable only with a receiver whose env exposes
  `libdispatch_scheduler`. A `static_assert` rejects `static_thread_pool`
  receivers.
- **Stop-before-event teardown.** Subscribe, immediately request stop,
  expect `set_stopped` within a short timeout. Verifies the
  session/registration cleanup path when no callback ever fired.
- **Double-subscribe rejection.** Hold one active subscription, attempt
  a second on the same context, expect `set_error` (the `__active_`
  CAS slot rejects).

Tests live in the existing `test.libdispatch_ext` target (Apple-only,
already gated by `STDEXEC_ENABLE_LIBDISPATCH`).

## Out of scope (follow-ups)

- Approval callbacks (mount/unmount/eject/peek).
- Match-dictionary filtering exposed in `watch_options`.
- Multi-subscriber fan-out on a single context. Same posture as
  FSEvents: build a layer on top.

(Done after this design landed: `watch_options::description_keys`
narrowing — a `std::vector<CFStringRef>` forwarded as the `watch`
array to `DARegisterDiskDescriptionChangedCallback`, empty = current
all-keys behavior.)
