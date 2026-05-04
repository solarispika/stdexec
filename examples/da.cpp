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
  auto run_capture(std::string const & cmd) -> std::string
  {
    std::string out;
    FILE*       p = ::popen(cmd.c_str(), "r");
    if (!p)
      return {};
    char buf[1024];
    while (std::fgets(buf, sizeof buf, p))
      out += buf;
    ::pclose(p);
    return out;
  }

  auto first_token(std::string const & s) -> std::string
  {
    std::istringstream ss{s};
    std::string        tok;
    ss >> tok;
    return tok;
  }

  auto kind_label(dax::disk_event_kind k) -> char const *
  {
    switch (k)
    {
    case dax::disk_event_kind::appeared:
      return "appeared";
    case dax::disk_event_kind::disappeared:
      return "disappeared";
    case dax::disk_event_kind::description_changed:
      return "desc_changed";
    }
    return "?";
  }
}  // namespace

auto main() -> int
{
  auto const      image = fs::temp_directory_path() / "dax_demo.sparseimage";
  std::error_code ec;
  fs::remove(image, ec);

  std::string create = "hdiutil create -size 1m -fs HFS+ -volname dax_demo -quiet "
                       + image.string();
  if (std::system(create.c_str()) != 0)
  {
    std::fprintf(stderr, "hdiutil create failed\n");
    return 1;
  }
  std::printf("created %s\n", image.c_str());

  dax::da_context ctx;

  std::thread mutator{[&]
                        {
                          std::this_thread::sleep_for(500ms);

                          auto out = run_capture("hdiutil attach -nomount " + image.string());
                          auto dev = first_token(out);
                          if (dev.empty())
                          {
                            std::fprintf(stderr, "hdiutil attach produced no /dev/diskN\n");
                            return;
                          }
                          std::printf("attached %s\n", dev.c_str());

                          std::this_thread::sleep_for(800ms);

                          std::system(("hdiutil detach -quiet " + dev).c_str());
                          std::printf("detached %s\n", dev.c_str());
                        }};

  exec::static_thread_pool pool{1};
  auto                     timer_sched = pool.get_scheduler();
  exec::libdispatch_queue  dax_pool    = exec::libdispatch_queue::make_concurrent("dax.demo");

  // Run the watch until the timer wins, demonstrating cancellation through the
  // sequence-sender pipeline.
  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(timer_sched, stdexec::just())
                     | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                   exec::sequence_with_scheduler(dax_pool.get_scheduler(),
                                                 ctx.watch({.watch_appeared            = true,
                                                              .watch_disappeared         = true,
                                                              .watch_description_changed = false}))
                     | exec::transform_each(stdexec::then(
                       [&](dax::disk_event e)
                       {
                         std::printf("[%s] bsd=%s",
                                     kind_label(e.kind),
                                     e.bsd_name.empty() ? "?" : e.bsd_name.c_str());
                         if (e.volume_name)
                           std::printf(" volume=\"%s\"", e.volume_name->c_str());
                         if (e.volume_path)
                           std::printf(" path=%s", e.volume_path->c_str());
                         std::printf("\n");
                       }))
                     | exec::ignore_all_values()));

  mutator.join();
  fs::remove(image, ec);

  std::printf("done\n");
  return 0;
}
