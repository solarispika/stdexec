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
  auto __dir = fs::temp_directory_path() / "inx_demo";
  fs::create_directories(__dir);
  for (auto const & __e: fs::directory_iterator{__dir})
  {
    fs::remove_all(__e.path());
  }
  std::printf("watching %s\n", __dir.c_str());

  exec::io_uring_context __ring;
  std::thread            __driver{[&] { __ring.run_until_stopped(); }};

  inx::inotify_context __ctx{{__dir.string()}};

  // jthread auto-stops + auto-joins on scope exit, so the mutator does
  // not outlive __ctx / __dir even if sync_wait throws.
  std::jthread __mutator{[&](std::stop_token __st)
                         {
                           for (int __i = 0; !__st.stop_requested() && __i < 5; ++__i)
                           {
                             std::this_thread::sleep_for(400ms);
                             std::ofstream __f{__dir / ("file_" + std::to_string(__i) + ".txt")};
                             __f << "hello " << __i << "\n";
                           }
                         }};

  exec::static_thread_pool __pool{1};
  auto                     __sched = __pool.get_scheduler();
  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(__sched, stdexec::just())
                     | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                   exec::sequence_with_scheduler(__ring.get_scheduler(), __ctx.watch())
                     | exec::transform_each(stdexec::then(
                       [&](inx::fs_batch __b)
                       {
                         if (__b.overflow)
                         {
                           std::printf("[overflow] kernel inotify queue "
                                       "overflowed; rescan required\n");
                         }
                         for (auto const & __e: __b.events)
                         {
                           auto __p = __ctx.path_for(__e.wd);
                           std::printf("wd=%d mask=%#x cookie=%u root=%s "
                                       "name=%s\n",
                                       __e.wd,
                                       static_cast<unsigned>(__e.mask),
                                       static_cast<unsigned>(__e.cookie),
                                       __p ? __p->c_str() : "?",
                                       __e.name.c_str());

                           // Demo dynamic add_watch: when a subdirectory is created, follow it.
                           if ((__e.mask & IN_CREATE) && (__e.mask & IN_ISDIR) && __p)
                           {
                             try
                             {
                               __ctx.add_watch(*__p + "/" + __e.name);
                             }
                             catch (std::system_error const & __ex)
                             {
                               std::printf("add_watch failed: %s\n", __ex.what());
                             }
                           }
                         }
                       }))
                     | exec::ignore_all_values()));

  __ring.request_stop();
  __driver.join();
  return 0;
}
