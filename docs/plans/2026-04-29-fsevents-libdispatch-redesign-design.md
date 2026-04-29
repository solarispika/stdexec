# FSEvents wrapper × libdispatch_queue redesign

Date: 2026-04-29
Branch: `fsevents-sender-wrapper`
Scope: `examples/fsevents_wrapper.hpp`, `examples/fsevents*.cpp`,
`include/exec/libdispatch_queue.hpp`

## Motivation

Current `fsevents_wrapper.hpp` hard-codes its dispatch queue:

```cpp
__queue_{dispatch_queue_create("fsx.fsevents", DISPATCH_QUEUE_SERIAL)}
```

Caller has no way to choose the QoS, share a queue with other components,
or place the FSEvents callback worker on an existing pool. This redesign
makes the queue choice a first-class part of the stdexec pipeline,
expressed via `stdexec::starts_on`, and brings `libdispatch_queue` up to
parity with `windows_thread_pool` as a configurable execution context.

## Decisions

### 1. Dispatch the FSEvents callback queue via `starts_on`, not ctor

```cpp
fsx::fsevents_context ctx{{"/path"}};
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("my.pool");

sync_wait(
    stdexec::starts_on(pool.get_scheduler(), ctx.watch(opts))
  | exec::transform_each(...)
  | exec::ignore_all_values());
```

`starts_on(sched, child)` wraps `child`'s receiver env so that
`get_scheduler(env)` returns `sched` (verified in
`include/stdexec/__detail/__starts_on.hpp` + `__schedulers.hpp::__sched_env`).
The watch op queries the env at construction time and pulls the underlying
`dispatch_queue_t` from the `libdispatch_scheduler`.

Rationale: the queue is a "where work runs" concern. stdexec already has
algorithms for that. Baking it into ctor makes the wrapper less composable
and forces a separate concept ("queue-owning context") that does not exist
in `windows_thread_pool` or `static_thread_pool`.

### 2. Strict env constraint, not silent fallback

`__watch_sender::subscribe` requires the receiver's env to expose a
`libdispatch_scheduler` via `get_scheduler`. Any other scheduler type is a
compile-time error.

Rationale: silent fallback to a default global queue would compile when
the user wrote `starts_on(static_thread_pool_sched, ctx.watch())`,
producing a runtime mismatch where the FSEvents callback runs on a queue
unrelated to the scheduler the user picked. Catching this at compile time
costs one `requires` clause and prevents an entire footgun class.

### 3. Context owns no queue; per-op internal serial queue

`fsevents_context` becomes a pure data holder (paths +
`__last_completed_id_` + `__active_` CAS slot). Each `__op` creates its
own serial queue at ctor time:

```cpp
dispatch_queue_create_with_target("fsx.fsevents",
                                  DISPATCH_QUEUE_SERIAL,
                                  sched.queue_->native_handle());
```

The serial wrapping is a wrapper-internal implementation detail required
by the callback ↔ teardown serialization assumption (see "Why serial"
below). Callers can supply any queue — global, concurrent, or serial —
the wrapper makes its own internal serial layer with the user's queue as
target. Worker threads come from the user's queue's pool; ordering inside
the wrapper stays serial.

### 4. Why FSEvents internally needs serial

`fsevents_wrapper.hpp` lines 308 / 325 / 356 use `dispatch_async_f` to
the same queue FSEvents callbacks fire on, relying on serial ordering to
guarantee teardown runs *after* the in-flight callback returns. On a
concurrent queue this idiom races. The wrapper also blocks the callback
on a `binary_semaphore` for backpressure; on a concurrent queue, multiple
callbacks would park on the semaphore in parallel, defeating the
"single in-flight callback" assumption and breaking batch ordering.

Redesigning to be lock-based instead of queue-based costs ~30–50 lines of
synchronization with no user benefit (FSEvents is a single-producer
source; concurrency at the callback level is not useful). Target-queue
wrapping captures all the user's intent (pool / QoS / shared workers)
without exposing the serial constraint.

## API design

### `include/exec/libdispatch_queue.hpp`

```cpp
struct libdispatch_queue {
    libdispatch_queue() = default;                            // global queue, default priority
    explicit libdispatch_queue(int priority);                 // global queue, specific priority

    static libdispatch_queue make_serial    (char const* label,
                                             dispatch_qos_class_t qos = QOS_CLASS_DEFAULT);
    static libdispatch_queue make_concurrent(char const* label,
                                             dispatch_qos_class_t qos = QOS_CLASS_DEFAULT);

    static libdispatch_queue make_serial    (char const* label,
                                             libdispatch_queue& target,
                                             dispatch_qos_class_t qos = QOS_CLASS_UNSPECIFIED);
    static libdispatch_queue make_concurrent(char const* label,
                                             libdispatch_queue& target,
                                             dispatch_qos_class_t qos = QOS_CLASS_UNSPECIFIED);

    static libdispatch_queue wrap(dispatch_queue_t q);        // retains q

    libdispatch_queue(libdispatch_queue&&) noexcept;
    libdispatch_queue(libdispatch_queue const&) = delete;
    ~libdispatch_queue();

    auto get_scheduler() -> libdispatch_scheduler;
    auto native_handle() const noexcept -> dispatch_queue_t;

  private:
    dispatch_queue_t __q_{nullptr};   // nullptr = use global queue + priority
    int              priority{DISPATCH_QUEUE_PRIORITY_DEFAULT};
    bool             __owns_{false};  // factory-built or wrap-retained → release in dtor
};
```

Key behaviors:

- Default ctor / priority ctor: `__q_ = nullptr`, `submit()` looks up
  `dispatch_get_global_queue(priority, 0)` per submission. Backward
  compatible with the original implementation.
- Factory variants: `__q_` set, `__owns_ = true`, dtor releases.
- `wrap`: retains caller's queue, dtor releases its own retain.
- No `is_serial()` — see Decision 3 above.
- `native_handle()` exposed for raw GCD interop (FSEvents,
  `dispatch_io`, etc.). Treated like `std::thread::native_handle()`.

The existing `bulk` machinery in `__libdispatch::bulk_*` keeps working —
it submits via `libdispatch_queue::submit()` which now picks the right
queue based on the configured fields.

### `examples/fsevents_wrapper.hpp`

```cpp
class fsevents_context {
  public:
    explicit fsevents_context(std::vector<std::string> paths);

    auto watch(watch_options opts = {}) -> __detail::__watch_sender;

    [[nodiscard]] auto last_completed_id() const noexcept -> FSEventStreamEventId;

    fsevents_context(fsevents_context const&)                    = delete;
    auto operator=(fsevents_context const&) -> fsevents_context& = delete;

  private:
    std::vector<std::string>              __paths_;
    std::atomic<__detail::__op_base*>     __active_{nullptr};
    std::atomic<FSEventStreamEventId>     __last_completed_id_{0};
};
```

`__watch_sender::subscribe` gains:

```cpp
template <class _Rcvr>
    requires stdexec::__callable<stdexec::get_scheduler_t,
                                 stdexec::env_of_t<_Rcvr> const&>
          && std::same_as<
               stdexec::__call_result_t<stdexec::get_scheduler_t,
                                        stdexec::env_of_t<_Rcvr> const&>,
               exec::libdispatch_scheduler>
auto subscribe(_Rcvr rcvr) const -> __op<_Rcvr>;
```

`__op<_Rcvr>` gains a `dispatch_queue_t __queue_` member, builds it from
`get_scheduler(get_env(rcvr))` in ctor, releases in dtor. Every existing
use of `__ctx_->__queue_` becomes `this->__queue_`.

### Demo updates

Both `examples/fsevents.cpp` and `examples/fsevents_coro.cpp` switch to:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("fsx.demo");
fsx::fsevents_context  ctx{{canonical_dir.string()}};

sync_wait(
    stdexec::starts_on(pool.get_scheduler(), ctx.watch(opts))
  | ...);
```

## Data flow

```
caller's libdispatch_queue (any kind)
        │
        ▼ get_scheduler()
libdispatch_scheduler
        │
        ▼ starts_on(sched, ctx.watch())  → injects sched into receiver env
__watch_sender::subscribe(rcvr)
        │
        ▼ requires libdispatch_scheduler in env (compile-time)
__op ctor
        │
        ▼ get_scheduler(get_env(rcvr)).queue_->native_handle()
dispatch_queue_create_with_target("fsx.fsevents", SERIAL, target=user_queue)
        │
        ▼ FSEventStreamSetDispatchQueue + FSEventStreamStart
fseventsd ──► callback (on internal serial q, worker from user pool)
                │
                ▼ set_next(rcvr, just(batch))
              next op runs (still on internal serial q)
                │
                ▼ set_value() → semaphore.release → callback returns
              next batch can fire
```

## PR scope

In:
1. `include/exec/libdispatch_queue.hpp` — factory functions, target
   queue, `wrap`, `native_handle`, lifetime.
2. `examples/fsevents_wrapper.hpp` — context-as-data, env-constrained
   subscribe, per-op queue.
3. `examples/fsevents.cpp` + `fsevents_coro.cpp` — switch to starts_on.
4. `examples/fsevents_README.md` — API section + "why starts_on" note.

Out (separate follow-up):
- `schedule_at` / `schedule_after` with `dispatch_source` TIMER for real
  cancellation of pending timer fires.
- Periodic timer as a multi-shot `sequence_sender`.
- `dispatch_source` READ / WRITE / SIGNAL wrappers.
- `dispatch_io` wrapper.
- `runloop_scheduler` (for FSEvents' deprecated runloop API path).

## Risks

- `libdispatch_scheduler` lives in `experimental::execution::__libdispatch`
  namespace internals. Exposing `queue_` field or `native_handle()` on it
  is required for the wrapper to extract the underlying queue. Need to
  decide: make `queue_` public, add a public accessor, or use a friend
  declaration. Adding a public `native_handle()` is the cleanest.
- `starts_on` semantics for `sequence_sender` are not heavily exercised
  in tests. Need to verify env propagation works on the sequence-sender
  path the same way as plain sender. If not, may need a custom adapter
  algorithm.
- The `make_*` factories with target may need to track that the
  `libdispatch_queue` argument is a `libdispatch_queue&` (so we get
  `native_handle`), but conceptually a raw `dispatch_queue_t` would also
  work. Decision: only accept `libdispatch_queue&` to keep the type
  surface small; users with raw queues call `wrap()` first.
