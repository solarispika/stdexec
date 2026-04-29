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

// Demo: consume the DiskArbitration sequence sender via a sender pipeline.
//
// Drives DA traffic by `hdiutil create + attach -nomount + detach` against a
// throwaway sparse image. No root, no real device.

#include "da_wrapper.hpp"

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace
{
  auto run_capture(const std::string& __cmd) -> std::string
  {
    std::string __out;
    FILE*       __p = ::popen(__cmd.c_str(), "r");
    if (!__p)
      return {};
    char __buf[1024];
    while (std::fgets(__buf, sizeof __buf, __p))
      __out += __buf;
    ::pclose(__p);
    return __out;
  }

  auto first_token(const std::string& __s) -> std::string
  {
    std::istringstream __ss{__s};
    std::string        __tok;
    __ss >> __tok;
    return __tok;
  }

  auto kind_label(dax::disk_event_kind __k) -> const char*
  {
    switch (__k)
    {
      case dax::disk_event_kind::appeared:            return "appeared";
      case dax::disk_event_kind::disappeared:         return "disappeared";
      case dax::disk_event_kind::description_changed: return "desc_changed";
    }
    return "?";
  }
}  // namespace

auto main() -> int
{
  const auto __image = fs::temp_directory_path() / "dax_demo.sparseimage";
  std::error_code __ec;
  fs::remove(__image, __ec);

  std::string __create =
    "hdiutil create -size 1m -fs HFS+ -volname dax_demo -quiet "
    + __image.string();
  if (std::system(__create.c_str()) != 0)
  {
    std::fprintf(stderr, "hdiutil create failed\n");
    return 1;
  }
  std::printf("created %s\n", __image.c_str());

  dax::da_context __ctx;

  std::thread __mutator{[&] {
    std::this_thread::sleep_for(500ms);

    auto __out = run_capture("hdiutil attach -nomount " + __image.string());
    auto __dev = first_token(__out);
    if (__dev.empty())
    {
      std::fprintf(stderr, "hdiutil attach produced no /dev/diskN\n");
      return;
    }
    std::printf("attached %s\n", __dev.c_str());

    std::this_thread::sleep_for(800ms);

    std::system(("hdiutil detach -quiet " + __dev).c_str());
    std::printf("detached %s\n", __dev.c_str());
  }};

  exec::static_thread_pool __pool{1};
  auto                     __timer_sched = __pool.get_scheduler();
  exec::libdispatch_queue  __dax_pool    = exec::libdispatch_queue::make_concurrent("dax.demo");

  // Run the watch until the timer wins, demonstrating cancellation through the
  // sequence-sender pipeline.
  stdexec::sync_wait(exec::when_any(
    stdexec::starts_on(__timer_sched, stdexec::just())
      | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
    dax::on_queue(__dax_pool.get_scheduler(),
                  __ctx.watch({.watch_appeared = true,
                               .watch_disappeared = true,
                               .watch_description_changed = false}))
      | exec::transform_each(stdexec::then(
        [&](dax::disk_event __e)
        {
          std::printf("[%s] bsd=%s",
                      kind_label(__e.kind),
                      __e.bsd_name.empty() ? "?" : __e.bsd_name.c_str());
          if (__e.volume_name)
            std::printf(" volume=\"%s\"", __e.volume_name->c_str());
          if (__e.volume_path)
            std::printf(" path=%s", __e.volume_path->c_str());
          std::printf("\n");
        }))
      | exec::ignore_all_values()));

  __mutator.join();
  fs::remove(__image, __ec);

  std::printf("done\n");
  return 0;
}
