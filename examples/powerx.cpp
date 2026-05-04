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

// Demo: consume the powerx suspend/resume sequence sender.
//
// Runs for ~30s and prints each suspend/resume. To trigger real events:
//
//   macOS:    `pmset sleepnow`     (from another shell)
//             closing a laptop lid also fires SystemWillSleep / HasPoweredOn
//
//   Windows:  Start menu → Sleep
//             rundll32.exe powrprof.dll,SetSuspendState 0,1,0
//             closing the laptop lid

#if defined(__APPLE__) && defined(__MACH__)
#  include "exec/libdispatch_queue.hpp"
#  include "powerx_mac_wrapper.hpp"
#elif defined(_WIN32)
#  include "exec/windows/windows_thread_pool.hpp"
#  include "powerx_win_wrapper.hpp"
#endif

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"

#include <chrono>
#include <cstdio>
#include <thread>

using namespace std::chrono_literals;

namespace
{
  auto kind_label(powerx::power_event_kind k) -> char const *
  {
    switch (k)
    {
    case powerx::power_event_kind::suspend:
      return "suspend";
    case powerx::power_event_kind::resume:
      return "resume";
    }
    return "?";
  }
}  // namespace

auto main() -> int
{
  std::printf("powerx demo: watching system power events for 30s\n");
#if defined(__APPLE__) && defined(__MACH__)
  std::printf("  trigger with: `pmset sleepnow` (from another shell)\n");
  exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("powerx.demo");
  auto                    pq   = pool.get_scheduler();
#elif defined(_WIN32)
  std::printf("  trigger with: Start menu → Sleep, or close the lid\n");
  exec::windows_thread_pool pool;
  auto                      pq = pool.get_scheduler();
#endif

  powerx::power_context ctx;

  exec::static_thread_pool timer_pool{1};
  auto                     timer_sched = timer_pool.get_scheduler();

  // Race the watch against a 30s timer; whichever completes first cancels the
  // other via when_any's stop-token propagation.
  stdexec::sync_wait(exec::when_any(
    stdexec::starts_on(timer_sched, stdexec::just())
      | stdexec::then([&] { std::this_thread::sleep_for(30s); }),
    exec::sequence_with_scheduler(pq, ctx.watch({.watch_suspend = true, .watch_resume = true}))
      | exec::transform_each(stdexec::then([&](powerx::power_event e)
                                           { std::printf("[%s]\n", kind_label(e.kind)); }))
      | exec::ignore_all_values()));

  std::printf("done\n");
  return 0;
}
