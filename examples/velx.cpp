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

#include "velx_wrapper.hpp"

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"
#include "exec/windows/windows_thread_pool.hpp"

#include <chrono>
#include <cstdio>
#include <thread>

using namespace std::chrono_literals;

auto main() -> int
{
  std::printf("velx demo: watching volume interface arrivals/removals for 30s.\n"
              "  - Plug/unplug a USB drive to trigger events, OR\n"
              "  - Run from another terminal:  Mount-VHD / Dismount-VHD foo.vhdx (admin)\n");

  exec::windows_thread_pool __wtp{2, 4};

  velx::volume_context __ctx;

  // Timer pool drives the cancellation deadline. Using an inline `just()`
  // would race with when_any's child startup (timer fires before the
  // watch is connected). Mirrors the DA / RDC pool demos.
  exec::static_thread_pool __timer_pool{1};
  auto                     __timer_sched = __timer_pool.get_scheduler();

  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                     | stdexec::then([] { std::this_thread::sleep_for(30s); }),
                   exec::sequence_with_scheduler(__wtp.get_scheduler(), __ctx.watch())
                     | exec::transform_each(stdexec::then(
                       [](velx::volume_event __ev)
                       {
                         char const * __label = __ev.kind
                                                 == velx::volume_event_kind::interface_arrival
                                                ? "[arrival]"
                                                : "[removal]";
                         std::printf("%s %s\n", __label, __ev.device_path.c_str());
                       }))
                     | exec::ignore_all_values()));

  return 0;
}
