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

// Demo: consume the ReadDirectoryChangesW sequence sender via a sender pipeline.

#include "rdc_wrapper.hpp"

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

namespace
{
  auto action_name(DWORD a) -> char const *
  {
    switch (a)
    {
    case FILE_ACTION_ADDED:
      return "ADDED";
    case FILE_ACTION_REMOVED:
      return "REMOVED";
    case FILE_ACTION_MODIFIED:
      return "MODIFIED";
    case FILE_ACTION_RENAMED_OLD_NAME:
      return "RENAMED_OLD";
    case FILE_ACTION_RENAMED_NEW_NAME:
      return "RENAMED_NEW";
    default:
      return "UNKNOWN";
    }
  }
}  // namespace

auto main() -> int
{
  auto dir = fs::temp_directory_path() / "rdcx_demo";
  fs::create_directories(dir);
  for (auto const & e: fs::directory_iterator{dir})
  {
    fs::remove_all(e.path());
  }
  dir = fs::canonical(dir);
  std::wprintf(L"watching %ls\n", dir.c_str());

  rdcx::rdc_context ctx{dir.wstring()};

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
  auto                     sched = pool.get_scheduler();
  stdexec::sync_wait(exec::when_any(stdexec::starts_on(sched, stdexec::just())
                                      | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                                    ctx.watch()
                                      | exec::transform_each(stdexec::then(
                                        [&](rdcx::fs_batch b)
                                        {
                                          if (b.overflow)
                                          {
                                            std::printf("[overflow] kernel buffer outpaced user "
                                                        "buffer; rescan required\n");
                                            return;
                                          }
                                          for (auto const & e: b.events)
                                          {
                                            std::wprintf(L"action=%hs path=%ls\n",
                                                         action_name(e.action),
                                                         e.path.c_str());
                                          }
                                        }))
                                      | exec::ignore_all_values()));

  mutator_stop.store(true);
  mutator.join();
  return 0;
}
