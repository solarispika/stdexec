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

#include "inotify_wrapper.hpp"

#include <cstdio>
#include <filesystem>

namespace fs = std::filesystem;

auto main() -> int
{
  auto __dir = fs::temp_directory_path() / "inx_smoke";
  fs::create_directories(__dir);
  inx::inotify_context __ctx{{__dir.string()}};
  auto __p = __ctx.path_for(1);  // wd=1 if first watch, but unrelied-on
  std::printf("inotify_context constructed; path_for(1) has_value=%d\n",
              __p.has_value());
  return 0;
}
