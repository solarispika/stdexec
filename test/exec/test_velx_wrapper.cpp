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
  static_assert(!exec::__env_has_scheduler<stdexec::env<>,
                                           exec::windows_thread_pool::scheduler>);

  TEST_CASE("velx::volume_context watch can be cancelled before any volume event")
  {
    exec::windows_thread_pool __wtp{2, 4};
    exec::static_thread_pool  __tp{1};
    auto                      __timer_sched = __tp.get_scheduler();

    velx::volume_context __ctx;

    // Cancel via a short timer. Using an inline `just()` would race with
    // when_any's child startup (timer fires before the watch is connected).
    // 50 ms is enough for start() to bring up CM + drainer + initial replay,
    // but short enough that the test stays fast.
    stdexec::sync_wait(exec::when_any(
      stdexec::starts_on(__timer_sched, stdexec::just())
        | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
      velx::on_pool(__wtp.get_scheduler(), __ctx.watch())
        | exec::ignore_all_values()));

    // Reaching here means the watch's __on_stop_fn ran, the cleanup work
    // item ran, the drainer drained, CM_Unregister_Notification ran, and
    // __active_ was cleared.
    SUCCEED("cancellation round-trip completed");
  }

  TEST_CASE("velx::volume_context allows sequential subscriptions after each completes")
  {
    exec::windows_thread_pool __wtp{2, 4};
    exec::static_thread_pool  __tp{1};
    auto                      __timer_sched = __tp.get_scheduler();

    velx::volume_context __ctx;

    auto __run_once = [&]
    {
      stdexec::sync_wait(exec::when_any(
        stdexec::starts_on(__timer_sched, stdexec::just())
          | stdexec::then([] { std::this_thread::sleep_for(50ms); }),
        velx::on_pool(__wtp.get_scheduler(), __ctx.watch())
          | exec::ignore_all_values()));
    };

    __run_once();
    __run_once();

    // Reaching here means __active_ was cleared by the cleanup work item
    // after the first run, allowing the second subscribe to CAS in.
    SUCCEED("two sequential subscriptions completed");
  }
}  // namespace
