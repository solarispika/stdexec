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

// Structural tests for the velx (Windows volume sender) wrapper.
// Windows-only, gated by STDEXEC_ENABLE_WINDOWS_THREAD_POOL at the CMake
// level. These tests do not trigger real volume events; they exercise
// the wrapper's lifecycle / env constraints. Mirrors test_da_wrapper.cpp.

#include "catch2/catch_all.hpp"

#include "../../examples/velx_wrapper.hpp"

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"
#include "exec/windows/windows_thread_pool.hpp"
#include "stdexec/execution.hpp"

#include <chrono>
#include <thread>

namespace
{
  using namespace std::chrono_literals;

  // Compile-time invariant: the watch sender must reject receivers whose env
  // does not expose a windows_thread_pool::scheduler. Mirrors DA's
  // libdispatch_scheduler check; this is the wall against silently routing
  // CM callbacks onto an unrelated scheduler when a user composes via
  // starts_on.
  static_assert(!exec::__env_has_scheduler<stdexec::env<>, exec::windows_thread_pool::scheduler>);

  TEST_CASE("velx::volume_context watch can be cancelled before any volume event")
  {
    exec::windows_thread_pool wtp{2, 4};
    exec::static_thread_pool  tp{1};
    auto                      timer_sched = tp.get_scheduler();

    velx::volume_context ctx;

    // Cancel via a short timer. Using an inline `just()` would race with
    // when_any's child startup (timer fires before the watch is connected).
    // 50 ms is enough for start() to bring up CM + drainer + initial replay,
    // but short enough that the test stays fast.
    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                     exec::sequence_with_scheduler(wtp.get_scheduler(), ctx.watch())
                       | exec::ignore_all_values()));

    // Reaching here means the watch's on_stop_fn ran, the cleanup work
    // item ran, the drainer drained, CM_Unregister_Notification ran, and
    // active_ was cleared.
    SUCCEED("cancellation round-trip completed");
  }

  TEST_CASE("velx::volume_context allows sequential subscriptions after each completes")
  {
    exec::windows_thread_pool wtp{2, 4};
    exec::static_thread_pool  tp{1};
    auto                      timer_sched = tp.get_scheduler();

    velx::volume_context ctx;

    auto run_once = [&]
    {
      stdexec::sync_wait(
        exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                         | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                       exec::sequence_with_scheduler(wtp.get_scheduler(), ctx.watch())
                         | exec::ignore_all_values()));
    };

    run_once();
    run_once();

    // Reaching here means active_ was cleared by the cleanup work item
    // after the first run, allowing the second subscribe to CAS in.
    SUCCEED("two sequential subscriptions completed");
  }

  TEST_CASE("velx::volume_context watch with default v2 options lifecycles cleanly")
  {
    exec::windows_thread_pool wtp{2, 4};
    exec::static_thread_pool  tp{1};
    auto                      timer_sched = tp.get_scheduler();

    velx::volume_context ctx;

    // Default-constructed watch_options leaves watch_handle_events=false
    // and query_remove=monostate. The wrapper should NOT register any
    // per-device DEVICEHANDLE notifications and should NOT invoke any
    // approval predicate. Verified structurally: lifecycle round-trip.
    velx::watch_options opts{};
    static_assert(std::is_same_v<decltype(opts.query_remove), velx::approval_policy>);
    CHECK(opts.watch_handle_events == false);
    CHECK(opts.query_remove.index() == 0);  // monostate

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
                     exec::sequence_with_scheduler(wtp.get_scheduler(), ctx.watch(opts))
                       | exec::ignore_all_values()));

    SUCCEED("default v2 round-trip completed");
  }

  TEST_CASE("velx::volume_context watch with watch_handle_events lifecycles cleanly")
  {
    exec::windows_thread_pool wtp{2, 4};
    exec::static_thread_pool  tp{1};
    auto                      timer_sched = tp.get_scheduler();

    velx::volume_context ctx;

    // Smoke test: enabling watch_handle_events causes the wrapper to
    // open a HANDLE + register a CM_NOTIFY_FILTER_TYPE_DEVICEHANDLE
    // notification for each volume that surfaces during the run. We
    // can't deterministically force a volume mount/unmount in a test,
    // so we only verify that the per-device register/unregister paths
    // (executed by the drainer when initial-replay arrival events
    // process) lifecycle cleanly through teardown. Bumping the timer
    // above the v1 defaults so the drainer has time to attempt
    // registrations on whatever volumes were enumerated.
    velx::watch_options opts{};
    opts.watch_handle_events = true;

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(200ms); }),
                     exec::sequence_with_scheduler(wtp.get_scheduler(), ctx.watch(opts))
                       | exec::ignore_all_values()));

    SUCCEED("watch_handle_events round-trip completed");
  }

  TEST_CASE("velx::volume_context watch with sync query_remove approval lifecycles cleanly")
  {
    exec::windows_thread_pool wtp{2, 4};
    exec::static_thread_pool  tp{1};
    auto                      timer_sched = tp.get_scheduler();

    velx::volume_context ctx;

    velx::watch_options opts{};
    opts.watch_handle_events = true;
    opts.query_remove        = approval::sync<velx::volume_info>{
             .predicate = [](velx::volume_info const &) { return true; },
    };

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(200ms); }),
                     exec::sequence_with_scheduler(wtp.get_scheduler(), ctx.watch(opts))
                       | exec::ignore_all_values()));

    SUCCEED("sync query_remove approval round-trip completed");
  }

  TEST_CASE("velx::volume_context watch with bounded query_remove approval lifecycles cleanly")
  {
    exec::windows_thread_pool wtp{2, 4};
    exec::static_thread_pool  tp{1};
    auto                      timer_sched = tp.get_scheduler();

    velx::volume_context ctx;

    velx::watch_options opts{};
    opts.watch_handle_events = true;
    opts.query_remove        = approval::bounded<velx::volume_info>{
             .predicate = [](velx::volume_info const &, stdexec::inplace_stop_token) { return true; },
             .timeout   = 100ms,
             .on_timeout_allow = true,
    };

    stdexec::sync_wait(
      exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                       | stdexec::then([] { std::this_thread::sleep_for(200ms); }),
                     exec::sequence_with_scheduler(wtp.get_scheduler(), ctx.watch(opts))
                       | exec::ignore_all_values()));

    SUCCEED("bounded query_remove approval round-trip completed");
  }
}  // namespace
