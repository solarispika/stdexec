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

// Demo: consume the FSEvents sequence sender via a sender pipeline.

#include "fsevents_wrapper.hpp"

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

auto main() -> int
{
  auto __dir = fs::temp_directory_path() / "fsx_demo";
  fs::create_directories(__dir);
  for (const auto& __e : fs::directory_iterator{__dir})
  {
    fs::remove_all(__e.path());
  }
  __dir = fs::canonical(__dir);  // FSEvents needs resolved (/private/...) paths
  std::printf("watching %s\n", __dir.c_str());

  fsx::fsevents_context __ctx{{__dir.string()}};

  std::atomic<bool> __mutator_stop{false};
  std::thread       __mutator{[&] {
    for (int __i = 0; !__mutator_stop.load() && __i < 5; ++__i)
    {
      std::this_thread::sleep_for(400ms);
      std::ofstream __f{__dir / ("file_" + std::to_string(__i) + ".txt")};
      __f << "hello " << __i << "\n";
    }
  }};

  // Run the watch until the timer wins, demonstrating cancellation through the
  // sequence-sender pipeline.
  exec::static_thread_pool __pool{1};
  auto                     __sched = __pool.get_scheduler();
  stdexec::sync_wait(exec::when_any(
    stdexec::starts_on(__sched, stdexec::just())
      | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
    __ctx.watch()
      | exec::transform_each(stdexec::then([&](fsx::fs_batch __b) {
          if (__b.must_rescan)
            std::printf("[rescan requested] flags imply MustScanSubDirs/RootChanged\n");
          for (const auto& __e : __b.events)
          {
            if (fsx::is_drop_notice(__e))
            {
              std::printf("[drop notice] flags=%#x path=%s\n",
                          static_cast<unsigned>(__e.flags),
                          __e.path.c_str());
              continue;
            }
            std::printf("event id=%llu flags=%#x path=%s\n",
                        static_cast<unsigned long long>(__e.id),
                        static_cast<unsigned>(__e.flags),
                        __e.path.c_str());
          }
          std::printf("checkpoint id=%llu\n", static_cast<unsigned long long>(__b.last_id));
        }))
      | exec::ignore_all_values()));

  __mutator_stop.store(true);
  __mutator.join();

  std::printf("final last_completed_id = %llu\n",
              static_cast<unsigned long long>(__ctx.last_completed_id()));
  return 0;
}
