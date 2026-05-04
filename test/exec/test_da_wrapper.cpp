/*
 * Copyright (c) 2026 NVIDIA Corporation
 *
 * Licensed under the Apache License Version 2.0 with LLVM Exceptions
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *   https://llvm.org/LICENSE.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Structural tests for the DiskArbitration wrapper. APPLE-only (gated by
// STDEXEC_ENABLE_LIBDISPATCH at the CMake level). These tests do not trigger
// real DA events; they exercise the wrapper's lifecycle / env constraints.

#include "catch2/catch_all.hpp"

#include "../../examples/da_wrapper.hpp"

#include "exec/libdispatch_queue.hpp"
#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"
#include "stdexec/execution.hpp"

#include <chrono>
#include <thread>

namespace
{
  using namespace std::chrono_literals;

  // Compile-time invariant: the watch sender must reject receivers whose env
  // does not expose a libdispatch_scheduler. This mirrors the FSEvents wrapper
  // and is the wall against silently routing DA callbacks onto an unrelated
  // scheduler (e.g. static_thread_pool) when a user composes via starts_on.
  static_assert(!exec::__env_has_scheduler<stdexec::env<>, exec::libdispatch_scheduler>);

  TEST_CASE("dax::da_context watch can be cancelled before any DA event")
  {
    exec::libdispatch_queue  __pool = exec::libdispatch_queue::make_concurrent("test.dax.cancel");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    // Cancel via a short timer scheduled on a separate pool. Using an inline
    // `just()` would race with when_any's child startup (timer fires before the
    // watch is connected). Mirrors the fsevents demo's pattern.
    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                     exec::sequence_with_scheduler(__pool.get_scheduler(), __ctx.watch())
                       | exec::ignore_all_values()));

    // Reaching here means the watch's __on_stop_fn ran, the dispatch queue
    // drained, the DASession was released, and __active_ was cleared.
    SUCCEED("cancellation round-trip completed");
  }

  TEST_CASE("dax::da_context allows sequential subscriptions after each completes")
  {
    exec::libdispatch_queue  __pool = exec::libdispatch_queue::make_concurrent("test.dax.seq");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    auto __run_once = [&]
    {
      stdexec::sync_wait(
        exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                         | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                       exec::sequence_with_scheduler(__pool.get_scheduler(), __ctx.watch())
                         | exec::ignore_all_values()));
    };

    __run_once();
    __run_once();  // Would fail with "already has an active watch" if teardown
                   // forgot to clear __active_.
    SUCCEED("two sequential subscriptions completed");
  }

  TEST_CASE("dax::da_context rejects a second concurrent subscription")
  {
    exec::libdispatch_queue  __pool = exec::libdispatch_queue::make_concurrent("test.dax.dup");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    // Two concurrent subscriptions on the same context: one wins the
    // __active_ CAS, the other returns set_error from inside start().
    // when_any's error→stop semantics let us collect the failure.
    auto __watch = [&]
    {
      return exec::sequence_with_scheduler(__pool.get_scheduler(), __ctx.watch())
           | exec::ignore_all_values();
    };

    bool __saw_error = false;
    try
    {
      stdexec::sync_wait(
        exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                         | stdexec::then([] { std::this_thread::sleep_for(200ms); }),
                       __watch(),
                       __watch()));
    }
    catch (std::runtime_error const &)
    {
      __saw_error = true;
    }

    CHECK(__saw_error);
  }

  TEST_CASE("dax::da_context watch with narrowed description_keys lifecycles cleanly")
  {
    exec::libdispatch_queue __pool = exec::libdispatch_queue::make_concurrent("test.dax.desc_keys");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    // Smoke test: enabling description_changed with an explicit (non-empty)
    // key array exercises the CFStringCreateWithCString / CFArrayCreate /
    // CFRelease path inside __op. We can't trigger description_changed
    // deterministically without real disk I/O, so we just verify the
    // cancellation round-trip with the new field.
    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                       | stdexec::then(
                         []
                         {
                           std::this_thread::sleep_for(50ms);
    }),
                     exec::sequence_with_scheduler(__pool.get_scheduler(),
                                                   __ctx.watch({.watch_appeared            = false,
                                                                .watch_disappeared         = false,
                                                                .watch_description_changed = true,
                                                                .description_keys = {"DAVolumeNam"
                                                                                     "e",
                                                                                     "DAVolumePat"
                                                                                     "h"}}))
                       | exec::ignore_all_values()));

    SUCCEED("description_keys narrowing round-trip completed");
  }

  TEST_CASE("dax::da_context watch with default approval policies lifecycles cleanly")
  {
    exec::libdispatch_queue  __pool = exec::libdispatch_queue::make_concurrent("test.dax.appr_"
                                                                               "default");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    // Default-constructed watch_options leaves all three approval fields
    // monostate. The wrapper should NOT call DARegister*ApprovalCallback in
    // that case — verified structurally by exercising the round-trip and
    // confirming teardown still completes cleanly.
    dax::watch_options __opts{};
    static_assert(std::is_same_v<decltype(__opts.mount_approval), dax::approval_policy>);
    static_assert(std::is_same_v<decltype(__opts.unmount_approval), dax::approval_policy>);
    static_assert(std::is_same_v<decltype(__opts.eject_approval), dax::approval_policy>);
    CHECK(__opts.mount_approval.index() == 0);    // monostate
    CHECK(__opts.unmount_approval.index() == 0);  // monostate
    CHECK(__opts.eject_approval.index() == 0);    // monostate

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                     exec::sequence_with_scheduler(__pool.get_scheduler(), __ctx.watch(__opts))
                       | exec::ignore_all_values()));

    SUCCEED("default-approval round-trip completed");
  }

  TEST_CASE("dax::da_context watch with sync unmount/eject approval lifecycles cleanly")
  {
    exec::libdispatch_queue __pool = exec::libdispatch_queue::make_concurrent("test.dax.appr_sync");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    // Smoke test: setting sync predicates flips the wrapper into
    // DARegister*ApprovalCallback territory. We can't trigger an
    // unmount/eject deterministically without privileged disk ops, so we
    // only verify that the registration and teardown paths run cleanly.
    dax::watch_options __opts{};
    __opts.unmount_approval = approval::sync<dax::disk_info>{
      .predicate = [](dax::disk_info const &) { return true; },
    };
    __opts.eject_approval = approval::sync<dax::disk_info>{
      .predicate = [](dax::disk_info const &) { return true; },
    };

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                     exec::sequence_with_scheduler(__pool.get_scheduler(), __ctx.watch(__opts))
                       | exec::ignore_all_values()));

    SUCCEED("sync approval round-trip completed");
  }

  TEST_CASE("dax::da_context watch with bounded unmount approval lifecycles cleanly")
  {
    exec::libdispatch_queue  __pool = exec::libdispatch_queue::make_concurrent("test.dax.appr_bnd");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    dax::watch_options __opts{};
    __opts.unmount_approval = approval::bounded<dax::disk_info>{
      .predicate        = [](dax::disk_info const &, stdexec::inplace_stop_token) { return true; },
      .timeout          = 100ms,
      .on_timeout_allow = true,
    };

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                     exec::sequence_with_scheduler(__pool.get_scheduler(), __ctx.watch(__opts))
                       | exec::ignore_all_values()));

    SUCCEED("bounded approval round-trip completed");
  }

  TEST_CASE("dax::da_context watch with match filter lifecycles cleanly")
  {
    exec::libdispatch_queue  __pool = exec::libdispatch_queue::make_concurrent("test.dax.match");
    exec::static_thread_pool __tp{1};
    auto                     __timer_sched = __tp.get_scheduler();

    dax::da_context __ctx;

    // Smoke test: a non-empty match filter mixing both supported value types
    // (bool -> CFBoolean, string -> CFString) exercises CFDictionaryCreate
    // and the corresponding teardown path. We can't observe filtering
    // behavior without real disk I/O, so we only verify the cancellation
    // round-trip — same posture as the description_keys test above.
    dax::watch_options __opts{};
    __opts.match["DAMediaWhole"] = true;
    __opts.match["DAVolumeKind"] = std::string{"apfs"};

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                     exec::sequence_with_scheduler(__pool.get_scheduler(), __ctx.watch(__opts))
                       | exec::ignore_all_values()));

    SUCCEED("match filter round-trip completed");
  }
}  // namespace
