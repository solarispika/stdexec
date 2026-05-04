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
  auto dir = fs::temp_directory_path() / "fsx_demo";
  fs::create_directories(dir);
  for (auto const & e: fs::directory_iterator{dir})
  {
    fs::remove_all(e.path());
  }
  dir = fs::canonical(dir);  // FSEvents needs resolved (/private/...) paths
  std::printf("watching %s\n", dir.c_str());

  fsx::fsevents_context ctx{{dir.string()}};

  std::atomic<bool> mutator_stop{false};
  std::thread       mutator{[&]
                        {
                          for (int i = 0; !mutator_stop.load() && i < 5; ++i)
                          {
                            std::this_thread::sleep_for(400ms);
                            std::ofstream f{dir / ("file_" + std::to_string(i) + ".txt")};
                            f << "hello " << i << "\n";
                          }
                        }};

  // Run the watch until the timer wins, demonstrating cancellation through the
  // sequence-sender pipeline.
  exec::static_thread_pool pool{1};
  auto                     sched    = pool.get_scheduler();
  exec::libdispatch_queue  fsx_pool = exec::libdispatch_queue::make_concurrent("fsx.demo");
  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(sched, stdexec::just())
                     | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                   exec::sequence_with_scheduler(fsx_pool.get_scheduler(), ctx.watch())
                     | exec::transform_each(stdexec::then(
                       [&](fsx::fs_batch b)
                       {
                         if (b.must_rescan)
                           std::printf("[rescan requested] flags imply "
                                       "MustScanSubDirs/RootChanged\n");
                         for (auto const & e: b.events)
                         {
                           if (fsx::is_drop_notice(e))
                           {
                             std::printf("[drop notice] flags=%#x path=%s\n",
                                         static_cast<unsigned>(e.flags),
                                         e.path.c_str());
                             continue;
                           }
                           std::printf("event id=%llu flags=%#x path=%s\n",
                                       static_cast<unsigned long long>(e.id),
                                       static_cast<unsigned>(e.flags),
                                       e.path.c_str());
                         }
                         std::printf("checkpoint id=%llu\n",
                                     static_cast<unsigned long long>(b.last_id));
                       }))
                     | exec::ignore_all_values()));

  mutator_stop.store(true);
  mutator.join();

  std::printf("final last_completed_id = %llu\n",
              static_cast<unsigned long long>(ctx.last_completed_id()));
  return 0;
}
