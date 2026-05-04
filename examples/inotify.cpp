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

// Demo: consume the inotify sequence sender via a sender pipeline.

#include "inotify_wrapper.hpp"

#include "exec/linux/io_uring_context.hpp"
#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

auto main() -> int
{
  auto dir = fs::temp_directory_path() / "inx_demo";
  fs::create_directories(dir);
  for (auto const & e: fs::directory_iterator{dir})
  {
    fs::remove_all(e.path());
  }
  std::printf("watching %s\n", dir.c_str());

  exec::io_uring_context ring;
  std::thread            driver{[&] { ring.run_until_stopped(); }};

  inx::inotify_context ctx{{dir.string()}};

  // jthread auto-stops + auto-joins on scope exit, so the mutator does
  // not outlive ctx / dir even if sync_wait throws.
  std::jthread mutator{[&](std::stop_token st)
                         {
                           for (int i = 0; !st.stop_requested() && i < 5; ++i)
                           {
                             std::this_thread::sleep_for(400ms);
                             std::ofstream f{dir / ("file_" + std::to_string(i) + ".txt")};
                             f << "hello " << i << "\n";
                           }
                         }};

  exec::static_thread_pool pool{1};
  auto                     sched = pool.get_scheduler();
  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(sched, stdexec::just())
                     | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                   exec::sequence_with_scheduler(ring.get_scheduler(), ctx.watch())
                     | exec::transform_each(stdexec::then(
                       [&](inx::fs_batch b)
                       {
                         if (b.overflow)
                         {
                           std::printf("[overflow] kernel inotify queue "
                                       "overflowed; rescan required\n");
                         }
                         for (auto const & e: b.events)
                         {
                           auto p = ctx.path_for(e.wd);
                           std::printf("wd=%d mask=%#x cookie=%u root=%s "
                                       "name=%s\n",
                                       e.wd,
                                       static_cast<unsigned>(e.mask),
                                       static_cast<unsigned>(e.cookie),
                                       p ? p->c_str() : "?",
                                       e.name.c_str());

                           // Demo dynamic add_watch: when a subdirectory is created, follow it.
                           if ((e.mask & IN_CREATE) && (e.mask & IN_ISDIR) && p)
                           {
                             try
                             {
                               ctx.add_watch(*p + "/" + e.name);
                             }
                             catch (std::system_error const & ex)
                             {
                               std::printf("add_watch failed: %s\n", ex.what());
                             }
                           }
                         }
                       }))
                     | exec::ignore_all_values()));

  ring.request_stop();
  driver.join();
  return 0;
}
