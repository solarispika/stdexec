# FSEvents × libdispatch_queue Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the FSEvents wrapper dispatch its callback queue via stdexec's `starts_on`, and bring `libdispatch_queue` up to a configurable execution context (factories, target queues, raw-queue wrap, native handle accessor).

**Architecture:** `fsevents_context` becomes pure data. Each `__op` queries `get_scheduler` from the receiver env (compile-time-constrained to `libdispatch_scheduler`) and creates an internal serial queue with the user's queue as target. `libdispatch_queue` grows factory functions (serial / concurrent / wrap), an optional target queue parameter, ownership tracking, and a `native_handle()` accessor.

**Tech Stack:** C++20, CMake, libdispatch (Apple GCD), stdexec, Catch2.

**Design reference:** `docs/plans/2026-04-29-fsevents-libdispatch-redesign-design.md`

**Worktree:** `/Users/bernies/Programming/stdexec/.worktrees/fsevents-libdispatch`
**Branch:** `fsevents-libdispatch-redesign`

---

## Pre-flight notes for the implementer

- This is **macOS-only** code. Build/test on a macOS host (the worktree was set up on Darwin 24.6.0, Apple clang).
- Pre-existing bug: `test/exec/sequence/test_merge_each_threaded.cpp` fails to build (`std::runtime_error` default-construct in `__variant.hpp:280`). **Do not try to fix this** — it is unrelated to this PR. Task 0 sets up an isolated test binary so we can run libdispatch tests without building the broken file.
- `libdispatch_scheduler` already stores `libdispatch_queue *queue_;` as a public member (`include/exec/libdispatch_queue.hpp:184`). We will add a `native_handle()` accessor for cleanliness; existing pointer access also works.
- All new files copyright header should match existing style (see top of `libdispatch_queue.hpp` for the Apache-2.0 LLVM exception block).
- Build command throughout: `cmake --build build --target <target> -j 8`
- Test discovery via Catch2: `./build/test/exec/test.libdispatch_ext` (we create this target in Task 0).

---

## Task 0: Set up isolated test binary

**Why:** `test.exec` cannot link due to the pre-existing test_merge_each_threaded.cpp error. We need to run our libdispatch tests in isolation.

**Files:**
- Modify: `test/exec/CMakeLists.txt` (add new target after the existing `test.exec`)

**Step 1: Add a new test executable target**

Append to `test/exec/CMakeLists.txt` after the `catch_discover_tests(test.exec)` line (around line 76):

```cmake
if(STDEXEC_ENABLE_LIBDISPATCH)
    add_executable(test.libdispatch_ext test_libdispatch.cpp)
    target_link_libraries(test.libdispatch_ext
        PUBLIC
        STDEXEC::stdexec
        stdexec_executable_flags
        Catch2::Catch2WithMain
        PRIVATE
        common_test_settings)
endif()
```

(Note: uses `Catch2WithMain` because we don't share a `main()` source with `test.exec`.)

**Step 2: Reconfigure CMake**

Run: `cmake -S . -B build`
Expected: success, "Build files have been written"

**Step 3: Build the new target**

Run: `cmake --build build --target test.libdispatch_ext -j 8`
Expected: builds successfully (the existing 3 test cases compile against current `libdispatch_queue.hpp`).

**Step 4: Run baseline**

Run: `./build/test/exec/test.libdispatch_ext`
Expected: `All tests passed (3 assertions in 3 test cases)` or similar — the existing 3 tests pass.

**Step 5: Commit**

```bash
git add test/exec/CMakeLists.txt
git commit -m "test: add isolated test.libdispatch_ext target

Avoids the unrelated test_merge_each_threaded.cpp build failure that
currently blocks linking test.exec on this branch. Mirrors the same
sources but builds independently with Catch2WithMain."
```

---

## Task 1: Add `native_handle()` to `libdispatch_queue`

**Files:**
- Modify: `include/exec/libdispatch_queue.hpp`
- Test: `test/exec/test_libdispatch.cpp`

**Step 1: Write failing test**

Append to `test/exec/test_libdispatch.cpp` (inside the anonymous namespace, before the closing `}  // namespace`):

```cpp
TEST_CASE("libdispatch_queue::native_handle returns a valid dispatch_queue_t")
{
    exec::libdispatch_queue queue;
    dispatch_queue_t        handle = queue.native_handle();
    CHECK(handle != nullptr);
    // For default ctor (global queue), native_handle returns
    // dispatch_get_global_queue with the configured priority.
    CHECK(handle == dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0));
}
```

**Step 2: Verify failure**

Run: `cmake --build build --target test.libdispatch_ext -j 8 2>&1 | tail -10`
Expected: compile error — no `native_handle` member.

**Step 3: Implement**

In `include/exec/libdispatch_queue.hpp`, inside `struct libdispatch_queue` (around line 187-203), add public member:

```cpp
auto native_handle() const noexcept -> dispatch_queue_t
{
    return __q_ ? __q_ : dispatch_get_global_queue(priority, 0);
}
```

Also add the storage field (will become useful for later tasks):

```cpp
dispatch_queue_t __q_{nullptr};
```

(Later tasks will populate `__q_` from factories. For now, default ctor keeps it `nullptr`.)

**Step 4: Verify pass**

Run: `cmake --build build --target test.libdispatch_ext -j 8 && ./build/test/exec/test.libdispatch_ext`
Expected: all 4 test cases pass.

**Step 5: Commit**

```bash
git add include/exec/libdispatch_queue.hpp test/exec/test_libdispatch.cpp
git commit -m "feat: add native_handle() accessor to libdispatch_queue"
```

---

## Task 2: Add `make_serial` / `make_concurrent` factories (no target)

**Files:**
- Modify: `include/exec/libdispatch_queue.hpp`
- Test: `test/exec/test_libdispatch.cpp`

**Step 1: Write failing tests**

Append to `test/exec/test_libdispatch.cpp`:

```cpp
TEST_CASE("libdispatch_queue::make_serial creates a labelled serial queue")
{
    auto             q   = exec::libdispatch_queue::make_serial("test.serial");
    dispatch_queue_t raw = q.native_handle();
    REQUIRE(raw != nullptr);
    CHECK(std::string{dispatch_queue_get_label(raw)} == "test.serial");
    // Schedule something on it to verify it works.
    auto sch = q.get_scheduler();
    auto [v] = STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([]{ return 42; })).value();
    CHECK(v == 42);
}

TEST_CASE("libdispatch_queue::make_concurrent creates a labelled concurrent queue")
{
    auto             q   = exec::libdispatch_queue::make_concurrent("test.concurrent");
    dispatch_queue_t raw = q.native_handle();
    REQUIRE(raw != nullptr);
    CHECK(std::string{dispatch_queue_get_label(raw)} == "test.concurrent");
    auto sch = q.get_scheduler();
    auto [v] = STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([]{ return 7; })).value();
    CHECK(v == 7);
}
```

**Step 2: Verify failure**

Run: `cmake --build build --target test.libdispatch_ext -j 8 2>&1 | tail -10`
Expected: compile error — `make_serial` / `make_concurrent` not found.

**Step 3: Implement**

In `include/exec/libdispatch_queue.hpp`, inside `struct libdispatch_queue`:

```cpp
static auto make_serial(char const* label,
                        dispatch_qos_class_t qos = QOS_CLASS_DEFAULT) -> libdispatch_queue
{
    auto attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL, qos, 0);
    auto raw  = dispatch_queue_create(label, attr);
    libdispatch_queue q;
    q.__q_     = raw;
    q.__owns_  = true;
    return q;
}

static auto make_concurrent(char const* label,
                            dispatch_qos_class_t qos = QOS_CLASS_DEFAULT) -> libdispatch_queue
{
    auto attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_CONCURRENT, qos, 0);
    auto raw  = dispatch_queue_create(label, attr);
    libdispatch_queue q;
    q.__q_     = raw;
    q.__owns_  = true;
    return q;
}
```

Also add the ownership flag and dtor (will be filled out properly in Task 5; for now make the dtor release if owning):

```cpp
private:
    bool __owns_{false};

public:
    ~libdispatch_queue()
    {
        if (__owns_ && __q_)
            dispatch_release(__q_);
    }
```

**Critical: forbid copy now to prevent double-release**

```cpp
libdispatch_queue(libdispatch_queue const&)                    = delete;
auto operator=(libdispatch_queue const&) -> libdispatch_queue& = delete;
```

(Move support comes in Task 5.)

The existing `operator==` and `submit()` need to keep working. Update `submit()` (around line 191-195) to dispatch onto the right queue:

```cpp
void submit(__libdispatch::task_base *f)
{
    auto queue = __q_ ? __q_ : dispatch_get_global_queue(priority, 0);
    dispatch_async_f(queue, f, reinterpret_cast<void (*)(void *) noexcept>(f->execute));
}
```

`operator==` defaulted comparison may break because `dispatch_queue_t` is a pointer. Replace with explicit comparison:

```cpp
friend auto operator==(libdispatch_queue const& a, libdispatch_queue const& b) noexcept -> bool
{
    return a.__q_ == b.__q_ && a.priority == b.priority;
}
```

**Step 4: Verify pass**

Run: `cmake --build build --target test.libdispatch_ext -j 8 && ./build/test/exec/test.libdispatch_ext`
Expected: 6 test cases pass.

Also build the existing FSEvents demos to ensure we didn't break them:

Run: `cmake --build build --target example.fsevents example.fsevents_coro -j 8`
Expected: success.

**Step 5: Commit**

```bash
git add include/exec/libdispatch_queue.hpp test/exec/test_libdispatch.cpp
git commit -m "feat: libdispatch_queue::make_serial / make_concurrent factories"
```

---

## Task 3: Add target-queue overloads of factories

**Files:**
- Modify: `include/exec/libdispatch_queue.hpp`
- Test: `test/exec/test_libdispatch.cpp`

**Step 1: Write failing test**

Append to `test/exec/test_libdispatch.cpp`:

```cpp
TEST_CASE("libdispatch_queue::make_serial(label, target) targets the parent queue")
{
    auto parent = exec::libdispatch_queue::make_concurrent("test.parent");
    auto child  = exec::libdispatch_queue::make_serial("test.child", parent);
    REQUIRE(child.native_handle() != nullptr);
    REQUIRE(child.native_handle() != parent.native_handle());
    // Schedule and verify it runs.
    auto sch = child.get_scheduler();
    auto [v] = STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([]{ return 99; })).value();
    CHECK(v == 99);
}
```

**Step 2: Verify failure**

Run: `cmake --build build --target test.libdispatch_ext -j 8 2>&1 | tail -10`
Expected: compile error — no overload taking `libdispatch_queue&`.

**Step 3: Implement**

In `include/exec/libdispatch_queue.hpp`, add inside `struct libdispatch_queue`:

```cpp
static auto make_serial(char const* label,
                        libdispatch_queue& target,
                        dispatch_qos_class_t qos = QOS_CLASS_UNSPECIFIED) -> libdispatch_queue
{
    auto attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL, qos, 0);
    auto raw  = dispatch_queue_create_with_target(label, attr, target.native_handle());
    libdispatch_queue q;
    q.__q_    = raw;
    q.__owns_ = true;
    return q;
}

static auto make_concurrent(char const* label,
                            libdispatch_queue& target,
                            dispatch_qos_class_t qos = QOS_CLASS_UNSPECIFIED) -> libdispatch_queue
{
    auto attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_CONCURRENT, qos, 0);
    auto raw  = dispatch_queue_create_with_target(label, attr, target.native_handle());
    libdispatch_queue q;
    q.__q_    = raw;
    q.__owns_ = true;
    return q;
}
```

Note `QOS_CLASS_UNSPECIFIED` causes the new queue to inherit QoS from the target — matching Apple's documented attribute behavior when QoS is unspecified.

**Step 4: Verify pass**

Run: `cmake --build build --target test.libdispatch_ext -j 8 && ./build/test/exec/test.libdispatch_ext`
Expected: 7 test cases pass.

**Step 5: Commit**

```bash
git add include/exec/libdispatch_queue.hpp test/exec/test_libdispatch.cpp
git commit -m "feat: libdispatch_queue factories with target queue"
```

---

## Task 4: Add `wrap()` for raw queues

**Files:**
- Modify: `include/exec/libdispatch_queue.hpp`
- Test: `test/exec/test_libdispatch.cpp`

**Step 1: Write failing test**

Append to `test/exec/test_libdispatch.cpp`:

```cpp
TEST_CASE("libdispatch_queue::wrap retains and releases the raw queue")
{
    dispatch_queue_t raw = dispatch_queue_create("test.raw", DISPATCH_QUEUE_SERIAL);
    {
        auto wrapped = exec::libdispatch_queue::wrap(raw);
        CHECK(wrapped.native_handle() == raw);
        auto sch = wrapped.get_scheduler();
        auto [v] = STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([]{ return 5; })).value();
        CHECK(v == 5);
    }
    // After wrapped goes out of scope, our retain we did manually keeps raw alive.
    dispatch_release(raw);
}
```

**Step 2: Verify failure**

Run: `cmake --build build --target test.libdispatch_ext -j 8 2>&1 | tail -10`
Expected: compile error — no `wrap`.

**Step 3: Implement**

In `include/exec/libdispatch_queue.hpp`:

```cpp
static auto wrap(dispatch_queue_t q) -> libdispatch_queue
{
    dispatch_retain(q);
    libdispatch_queue lq;
    lq.__q_    = q;
    lq.__owns_ = true;   // we hold a retain, dtor must release
    return lq;
}
```

**Step 4: Verify pass**

Run: `cmake --build build --target test.libdispatch_ext -j 8 && ./build/test/exec/test.libdispatch_ext`
Expected: 8 test cases pass. No leaks (run with `MallocStackLogging=1` if you want to verify, but the test already exercises retain/release symmetry).

**Step 5: Commit**

```bash
git add include/exec/libdispatch_queue.hpp test/exec/test_libdispatch.cpp
git commit -m "feat: libdispatch_queue::wrap for raw dispatch_queue_t"
```

---

## Task 5: Move semantics

**Files:**
- Modify: `include/exec/libdispatch_queue.hpp`
- Test: `test/exec/test_libdispatch.cpp`

**Step 1: Write failing test**

Append to `test/exec/test_libdispatch.cpp`:

```cpp
TEST_CASE("libdispatch_queue is movable")
{
    auto             q   = exec::libdispatch_queue::make_serial("test.move");
    dispatch_queue_t raw = q.native_handle();
    auto             q2  = std::move(q);
    CHECK(q2.native_handle() == raw);
    // q is moved-from; its dtor must not double-release raw.
    auto sch = q2.get_scheduler();
    auto [v] = STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([]{ return 1; })).value();
    CHECK(v == 1);
}
```

**Step 2: Verify failure**

Run: `cmake --build build --target test.libdispatch_ext -j 8 2>&1 | tail -10`
Expected: compile error — copy is deleted, no move ctor/assignment defined.

**Step 3: Implement**

In `include/exec/libdispatch_queue.hpp`:

```cpp
libdispatch_queue(libdispatch_queue&& other) noexcept
    : __q_(other.__q_)
    , priority(other.priority)
    , __owns_(other.__owns_)
{
    other.__q_    = nullptr;
    other.__owns_ = false;
}

auto operator=(libdispatch_queue&& other) noexcept -> libdispatch_queue&
{
    if (this != &other)
    {
        if (__owns_ && __q_)
            dispatch_release(__q_);
        __q_          = other.__q_;
        priority      = other.priority;
        __owns_       = other.__owns_;
        other.__q_    = nullptr;
        other.__owns_ = false;
    }
    return *this;
}
```

**Step 4: Verify pass**

Run: `cmake --build build --target test.libdispatch_ext -j 8 && ./build/test/exec/test.libdispatch_ext`
Expected: 9 test cases pass.

**Step 5: Commit**

```bash
git add include/exec/libdispatch_queue.hpp test/exec/test_libdispatch.cpp
git commit -m "feat: move semantics for libdispatch_queue"
```

---

## Task 6: Add `native_handle()` to `libdispatch_scheduler`

**Files:**
- Modify: `include/exec/libdispatch_queue.hpp`
- Test: `test/exec/test_libdispatch.cpp`

**Step 1: Write failing test**

Append to `test/exec/test_libdispatch.cpp`:

```cpp
TEST_CASE("libdispatch_scheduler exposes native_handle of underlying queue")
{
    auto q   = exec::libdispatch_queue::make_serial("test.sch.handle");
    auto sch = q.get_scheduler();
    CHECK(sch.native_handle() == q.native_handle());
}
```

**Step 2: Verify failure**

Run: `cmake --build build --target test.libdispatch_ext -j 8 2>&1 | tail -10`
Expected: compile error — no `native_handle` on scheduler.

**Step 3: Implement**

In `include/exec/libdispatch_queue.hpp`, inside `struct libdispatch_scheduler` (after the `query` methods, before `libdispatch_queue *queue_;`):

```cpp
auto native_handle() const noexcept -> dispatch_queue_t
{
    return queue_->native_handle();
}
```

**Step 4: Verify pass**

Run: `cmake --build build --target test.libdispatch_ext -j 8 && ./build/test/exec/test.libdispatch_ext`
Expected: 10 test cases pass.

**Step 5: Commit**

```bash
git add include/exec/libdispatch_queue.hpp test/exec/test_libdispatch.cpp
git commit -m "feat: libdispatch_scheduler::native_handle accessor"
```

---

## Task 7: Refactor `fsevents_context` to be queue-less

**Files:**
- Modify: `examples/fsevents_wrapper.hpp`

**Note:** This task breaks the demos temporarily. They'll be fixed in Tasks 10–11. After Task 7 the wrapper must still **compile** as a header (since other code includes it), but the demos won't link until Tasks 10–11.

**Step 1: Update fsevents_context (lines 91–134)**

Edit `examples/fsevents_wrapper.hpp`:

- **Remove**: `#include "exec/sequence_senders.hpp"` (already there) but **add** `#include "exec/libdispatch_queue.hpp"` near the other includes (top of file).
- **Replace** `fsevents_context`'s ctor body — drop the queue creation:

```cpp
explicit fsevents_context(std::vector<std::string> __paths)
    : __paths_{std::move(__paths)}
{}
```

- **Remove the dtor** (no queue to release):

```cpp
~fsevents_context() = default;
```

- **Remove** the `dispatch_queue_t __queue_;` member.

**Step 2: Update __next_receiver and __op to own a per-op queue**

In the `__detail` namespace, inside `__op<_Rcvr>` (search for `struct __op` in the file):

- Add a member `dispatch_queue_t __queue_{nullptr};` next to the existing members.
- In the `__op` ctor, after the existing field initialization, build the queue from the receiver's env:

```cpp
__op(fsevents_context* __ctx, watch_options __opts, _Rcvr __rcvr)
    : __ctx_{__ctx}
    , __opts_{__opts}
    , __rcvr_{std::move(__rcvr)}
    , __queue_{__make_internal_queue(__rcvr_)}
{}
```

- Add a private static helper inside `__op`:

```cpp
static auto __make_internal_queue(_Rcvr const& __r) -> dispatch_queue_t
{
    auto __sch = stdexec::get_scheduler(stdexec::get_env(__r));
    auto __attr = dispatch_queue_attr_make_with_qos_class(
        DISPATCH_QUEUE_SERIAL, QOS_CLASS_UNSPECIFIED, 0);
    return dispatch_queue_create_with_target("fsx.fsevents", __attr, __sch.native_handle());
}
```

- Add op dtor that releases the queue (or extend existing one):

```cpp
~__op()
{
    if (__queue_)
        dispatch_release(__queue_);
}
```

**Step 3: Update all `__ctx_->__queue_` usages**

Search `examples/fsevents_wrapper.hpp` for `__ctx_->__queue_` and `__self_->__ctx_->__queue_` — replace with `this->__queue_` (in `__op` methods) or `__op_ptr->__queue_` (in `__next_receiver`, which holds `__op*` via `__self_`).

Specifically:
- Line 247: `FSEventStreamSetDispatchQueue(__stream_, __ctx_->__queue_)` → `... this->__queue_)`
- Line 308: `dispatch_async_f(__ctx_->__queue_, ...)` → `dispatch_async_f(this->__queue_, ...)`
- Line 325: same pattern
- Line 356: `dispatch_async_f(__self_->__ctx_->__queue_, ...)` → `dispatch_async_f(__self_->__queue_, ...)` (where `__self_` is `__op*`)

**Step 4: Verify the header still compiles**

Run: `cmake --build build --target test.libdispatch_ext -j 8`
Expected: builds (this target doesn't include fsevents_wrapper.hpp, so it just verifies we didn't break libdispatch_queue.hpp).

The fsevents headers themselves only get included by `examples/fsevents.cpp` and `examples/fsevents_coro.cpp`. Those will fail to link until Tasks 10–11 because they call `ctx.watch()` without a `starts_on`. We'll deliberately leave the demos broken until then.

Optional: confirm header parses by attempting to build:
Run: `cmake --build build --target example.fsevents -j 8 2>&1 | head -20`
Expected: Errors about `get_scheduler` failing inside `__op<_Rcvr>::__make_internal_queue` because the demo's receiver env doesn't have a libdispatch_scheduler. **This is expected** and will be fixed when the demo is updated.

**Step 5: Commit**

```bash
git add examples/fsevents_wrapper.hpp
git commit -m "refactor(fsevents): per-op queue derived from receiver env

fsevents_context no longer owns a dispatch_queue; each __op pulls the
scheduler out of the receiver env and builds an internal serial queue
with the user's queue as target. Demos broken pending Tasks 10-11."
```

---

## Task 8: Add subscribe-time env constraint

**Files:**
- Modify: `examples/fsevents_wrapper.hpp` (the `__watch_sender::subscribe` template)

**Step 1: Find `__watch_sender`**

In `examples/fsevents_wrapper.hpp`, locate `struct __watch_sender` (around line 404). Find its `subscribe` member (around line 419):

```cpp
template <stdexec::receiver _Rcvr>
auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
```

**Step 2: Add the constraint**

Replace with:

```cpp
template <stdexec::receiver _Rcvr>
    requires stdexec::__callable<stdexec::get_scheduler_t,
                                 stdexec::env_of_t<_Rcvr> const&>
          && std::same_as<
               stdexec::__call_result_t<stdexec::get_scheduler_t,
                                        stdexec::env_of_t<_Rcvr> const&>,
               exec::libdispatch_scheduler>
auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
```

**Step 3: Verify header builds**

Run: `cmake --build build --target test.libdispatch_ext -j 8`
Expected: success.

Building `example.fsevents` should now produce a clearer constraint-failed error when the demo doesn't pass through `starts_on`. Don't fix it yet.

**Step 4: Commit**

```bash
git add examples/fsevents_wrapper.hpp
git commit -m "fsevents: constrain subscribe to libdispatch_scheduler in env

Compile-time check that the receiver env exposes a libdispatch_scheduler.
Prevents silent fallback when caller composes with a non-libdispatch
scheduler."
```

---

## Task 9: Update `examples/fsevents.cpp` to use `starts_on`

**Files:**
- Modify: `examples/fsevents.cpp`

**Step 1: Locate the call site**

Find where `fsevents_context` is constructed and `ctx.watch(...)` is fed into the pipeline (search for `ctx.watch`).

**Step 2: Construct a libdispatch_queue and use starts_on**

Add near the start of the demo's body:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("fsx.demo");
```

Wrap the `ctx.watch(opts)` expression in `stdexec::starts_on(pool.get_scheduler(), ...)`:

Before:
```cpp
auto pipeline = ctx.watch(opts)
              | exec::transform_each(...)
              | exec::ignore_all_values();
```

After:
```cpp
auto pipeline = stdexec::starts_on(pool.get_scheduler(), ctx.watch(opts))
              | exec::transform_each(...)
              | exec::ignore_all_values();
```

(Adjust to match the file's actual variable names / pipe layout.)

**Step 3: Build**

Run: `cmake --build build --target example.fsevents -j 8`
Expected: builds successfully.

**Step 4: Smoke test**

Run the binary briefly and verify it doesn't crash:

```bash
./build/examples/example.fsevents &
PID=$!
sleep 5
kill $PID 2>/dev/null
wait $PID 2>/dev/null
echo "demo finished"
```

Expected: prints something resembling FS events for the demo's working directory, then exits cleanly.

**Step 5: Commit**

```bash
git add examples/fsevents.cpp
git commit -m "examples: fsevents demo uses starts_on for queue selection"
```

---

## Task 10: Update `examples/fsevents_coro.cpp` to use `starts_on`

**Files:**
- Modify: `examples/fsevents_coro.cpp`

**Step 1: Locate the producer pipeline**

Find the `ctx.watch(...)` invocation. Per `fsevents_README.md` notes, this demo runs the producer pipeline on a dedicated `std::thread` with its own `sync_wait(when_any(timer, pipeline))`.

**Step 2: Apply the same starts_on wrapping**

Mirror the change in Task 9. Use a dedicated `libdispatch_queue` (or share with `consume(ch)` if appropriate — but per the README, the producer is intentionally isolated, so a dedicated `pool` for the producer is fine).

```cpp
exec::libdispatch_queue producer_pool = exec::libdispatch_queue::make_concurrent("fsx.coro.producer");
auto producer_pipeline =
    stdexec::starts_on(producer_pool.get_scheduler(), ctx.watch(opts))
  | exec::transform_each(stdexec::then([&](fsx::fs_batch b){ ch.push(b); }))
  | exec::ignore_all_values();
```

**Step 3: Build**

Run: `cmake --build build --target example.fsevents_coro -j 8`
Expected: builds successfully.

**Step 4: Smoke test**

Same pattern as Task 9 — run for ~5s and confirm clean exit.

**Step 5: Commit**

```bash
git add examples/fsevents_coro.cpp
git commit -m "examples: fsevents_coro demo uses starts_on for queue selection"
```

---

## Task 11: Update `fsevents_README.md`

**Files:**
- Modify: `examples/fsevents_README.md`

**Step 1: Update the API code block (lines 28–40)**

Replace the existing snippet with the new starts_on-based example:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("my.pool");
fsx::fsevents_context ctx{{"/path/to/dir"}};

stdexec::sync_wait(
    stdexec::starts_on(pool.get_scheduler(),
                       ctx.watch({.since = kFSEventStreamEventIdSinceNow,
                                  .latency = 0.2,
                                  .create_flags = ...}))
  | exec::transform_each(stdexec::then([](fsx::fs_batch b){ ... }))
  | exec::ignore_all_values());
```

**Step 2: Add a new section "Queue selection / scheduler"**

Insert after the `## API` section, before `## What happens under the hood`:

```markdown
## Queue selection / scheduler

The wrapper does not own a dispatch queue. Instead, the queue is selected
at the pipeline level via `stdexec::starts_on`:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("...");
sync_wait(stdexec::starts_on(pool.get_scheduler(), ctx.watch(opts)) | ...);
```

`__watch_sender::subscribe` requires the receiver env to expose a
`libdispatch_scheduler` via `get_scheduler` — composing with any other
scheduler type is a compile-time error. This prevents silently falling
back to a default queue when the caller intended e.g. a `static_thread_pool`.

Internally, each watch operation creates its own serial queue with the
user's queue as target (`dispatch_queue_create_with_target`). The serial
attribute is required by the wrapper's callback ↔ teardown serialization
idiom; the worker thread comes from the user's pool.
```

**Step 3: Commit**

```bash
git add examples/fsevents_README.md
git commit -m "docs: fsevents README documents starts_on-based queue selection"
```

---

## Task 12: Final verification

**Step 1: Full rebuild**

Run: `cmake --build build --target example.fsevents example.fsevents_coro test.libdispatch_ext -j 8`
Expected: all targets build cleanly.

**Step 2: Run all libdispatch tests**

Run: `./build/test/exec/test.libdispatch_ext`
Expected: all 10 test cases pass.

**Step 3: Smoke test both demos**

```bash
timeout 5 ./build/examples/example.fsevents 2>&1 | head -20
timeout 5 ./build/examples/example.fsevents_coro 2>&1 | head -20
```

Expected: each prints FS events output, exits via SIGTERM cleanly.

**Step 4: Diff review**

Run: `git log --oneline main..HEAD`
Expected: ~12 commits, one per task, with consistent message style.

Run: `git diff main..HEAD -- include/exec/libdispatch_queue.hpp examples/fsevents*.* docs/plans/*libdispatch* test/exec/* | wc -l`
Expected: under ~600 lines diff.

**Step 5: Use the verification-before-completion skill**

Before claiming the work done, invoke `superpowers:verification-before-completion` to walk through the checklist (build clean, tests green, demos run, no missing commits).

---

## Out of scope (do NOT implement here)

- `schedule_at` / `schedule_after` with real cancellation via `dispatch_source` TIMER
- Periodic / multi-shot timer sequence_sender
- `dispatch_source` READ / WRITE / SIGNAL wrappers
- `dispatch_io` wrapper
- `runloop_scheduler` for FSEvents' deprecated runloop API path
- Fixing the pre-existing `test_merge_each_threaded.cpp` build failure

If the implementer notices any of the above, file a follow-up note in the PR description but do not expand this PR.
