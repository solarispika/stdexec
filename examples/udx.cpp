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

// Demo: consume the udev sequence sender via a sender pipeline.
//
// Prints initial-replay [add] events for every currently-present block
// device, then any add/remove/change/etc events that fire during the
// next 30 seconds, then exits. Trigger live events from another shell:
//
//   # synthetic add events for existing block devices (no state change):
//   sudo udevadm trigger --action=add --subsystem-match=block
//
//   # transient dm-zero device (real add + remove):
//   sudo dmsetup create udx_test --table '0 1 zero'
//   sudo dmsetup remove udx_test
//
//   # if the loop kernel module has spare slots:
//   sudo losetup -f --show /tmp/udx_loop.img
//   sudo losetup -d /dev/loopN
//
// or by physically inserting / removing a USB drive.

#include "udx_wrapper.hpp"

#include "exec/linux/io_uring_context.hpp"
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
  auto kind_str(udx::device_kind k) noexcept -> char const *
  {
    switch (k)
    {
    case udx::device_kind::add:
      return "add";
    case udx::device_kind::remove:
      return "remove";
    case udx::device_kind::change:
      return "change";
    case udx::device_kind::online:
      return "online";
    case udx::device_kind::offline:
      return "offline";
    case udx::device_kind::bind:
      return "bind";
    case udx::device_kind::unbind:
      return "unbind";
    case udx::device_kind::move:
      return "move";
    case udx::device_kind::unknown:
      return "unknown";
    }
    return "?";
  }
}  // namespace

auto main() -> int
{
  std::printf("watching subsystem=block for 30s; trigger via:\n");
  std::printf("  sudo udevadm trigger --action=add --subsystem-match=block   # synthetic add\n");
  std::printf("  sudo dmsetup create udx_test --table '0 1 zero'             # real add\n");
  std::printf("  sudo dmsetup remove udx_test                                # real remove\n\n");

  exec::io_uring_context ring;
  std::thread            driver{[&] { ring.run_until_stopped(); }};

  udx::udev_context ctx;

  exec::static_thread_pool pool{1};
  auto                     sched = pool.get_scheduler();
  stdexec::sync_wait(exec::when_any(
    stdexec::starts_on(sched, stdexec::just())
      | stdexec::then([&] { std::this_thread::sleep_for(30s); }),
    exec::sequence_with_scheduler(ring.get_scheduler(),
                                  ctx.watch({.subsystem = "block", .initial_replay = true}))
      | exec::transform_each(stdexec::then(
        [](udx::device_event e)
        {
          std::printf("[%-7s] %s/%s sysname=%s devnode=%s\n",
                      kind_str(e.kind),
                      e.subsystem.c_str(),
                      e.devtype.c_str(),
                      e.sysname.c_str(),
                      e.devnode ? e.devnode->c_str() : "(none)");
        }))
      | exec::ignore_all_values()));

  ring.request_stop();
  driver.join();
  return 0;
}
