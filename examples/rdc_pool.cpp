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
  auto action_name(DWORD __a) -> char const *
  {
    switch (__a)
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
  auto __dir = fs::temp_directory_path() / "rdcx_pool_demo";
  fs::create_directories(__dir);
  for (auto const & __e: fs::directory_iterator{__dir})
  {
    fs::remove_all(__e.path());
  }
  __dir = fs::canonical(__dir);
  std::wprintf(L"watching %ls (pool variant)\n", __dir.c_str());

  rdcx::pool::rdc_context __ctx{__dir.wstring()};

  std::atomic<bool> __mutator_stop{false};
  std::thread       __mutator{[&]
                        {
                          for (int __i = 0; !__mutator_stop.load() && __i < 5; ++__i)
                          {
                            std::this_thread::sleep_for(400ms);
                            std::ofstream __f{__dir / ("file_" + std::to_string(__i) + ".txt")};
                            __f << "hello " << __i << "\n";
                          }
                        }};

  exec::windows_thread_pool __wtp{2, 4};

  exec::static_thread_pool __timer_pool{1};
  auto                     __timer_sched = __timer_pool.get_scheduler();
  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                     | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                   exec::sequence_with_scheduler(__wtp.get_scheduler(), __ctx.watch())
                     | exec::transform_each(stdexec::then(
                       [&](rdcx::pool::fs_batch __b)
                       {
                         if (__b.overflow)
                         {
                           std::printf("[overflow] kernel buffer outpaced user "
                                       "buffer; rescan required\n");
                           return;
                         }
                         for (auto const & __e: __b.events)
                         {
                           std::wprintf(L"action=%hs path=%ls\n",
                                        action_name(__e.action),
                                        __e.path.c_str());
                         }
                       }))
                     | exec::ignore_all_values()));

  __mutator_stop.store(true);
  __mutator.join();
  return 0;
}
