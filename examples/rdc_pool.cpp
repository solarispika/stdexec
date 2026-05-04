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

// Demo: same shape as rdc.cpp, but the IO completions are driven by the
// Win32 thread pool instead of a dedicated worker thread.

#include "rdc_pool_wrapper.hpp"

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"
#include "exec/windows/windows_thread_pool.hpp"

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
  auto dir = fs::temp_directory_path() / "rdcx_pool_demo";
  fs::create_directories(dir);
  for (auto const & e: fs::directory_iterator{dir})
  {
    fs::remove_all(e.path());
  }
  dir = fs::canonical(dir);
  std::wprintf(L"watching %ls (pool variant)\n", dir.c_str());

  rdcx::pool::rdc_context ctx{dir.wstring()};

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

  exec::windows_thread_pool wtp{2, 4};

  exec::static_thread_pool timer_pool{1};
  auto                     timer_sched = timer_pool.get_scheduler();
  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                     | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                   exec::sequence_with_scheduler(wtp.get_scheduler(), ctx.watch())
                     | exec::transform_each(stdexec::then(
                       [&](rdcx::pool::fs_batch b)
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
