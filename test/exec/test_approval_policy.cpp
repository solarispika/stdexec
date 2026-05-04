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

// Behavior tests for the cross-platform approval::policy helper used by the
// DA wrapper (and, in a follow-up, the velx wrapper) to dispatch
// pre-removal verdict decisions onto either the OS callback thread (sync)
// or a bounded worker (bounded).

#include "catch2/catch_all.hpp"

#include "../../examples/approval_policy.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

namespace
{
  using namespace std::chrono_literals;

  struct fake_info
  {
    std::string name;
  };

  TEST_CASE("approval::resolve_verdict returns the no-policy default on monostate")
  {
    approval::policy<fake_info> p{};  // default-constructed = monostate

    bool factory_called = false;
    auto make_info      = [&]
    {
      factory_called = true;
      return fake_info{"unused"};
    };

    CHECK(approval::resolve_verdict(p, make_info, /*default=*/true) == true);
    CHECK(approval::resolve_verdict(p, make_info, /*default=*/false) == false);
    // monostate must short-circuit before the factory runs — building the
    // info from an OS handle is not free.
    CHECK_FALSE(factory_called);
  }

  TEST_CASE("approval::resolve_verdict invokes a sync predicate and returns its bool")
  {
    SECTION("sync allow")
    {
      approval::policy<fake_info> p = approval::sync<fake_info>{
        .predicate = [](fake_info const &) { return true; },
      };
      CHECK(approval::resolve_verdict(p, [] { return fake_info{"x"}; }) == true);
    }
    SECTION("sync deny")
    {
      approval::policy<fake_info> p = approval::sync<fake_info>{
        .predicate = [](fake_info const &) { return false; },
      };
      CHECK(approval::resolve_verdict(p, [] { return fake_info{"x"}; }) == false);
    }
    SECTION("predicate sees the info produced by the factory")
    {
      std::string                 seen;
      approval::policy<fake_info> p = approval::sync<fake_info>{
        .predicate =
          [&seen](fake_info const &i)
        {
          seen = i.name;
          return true;
        },
      };
      approval::resolve_verdict(p, [] { return fake_info{"hello"}; });
      CHECK(seen == "hello");
    }
  }

  TEST_CASE("approval::resolve_verdict bounded happy path returns predicate's bool")
  {
    approval::policy<fake_info> p = approval::bounded<fake_info>{
      .predicate =
        [](fake_info const &, stdexec::inplace_stop_token)
      {
        // Returns promptly — well under the timeout.
        return false;
      },
      .timeout          = 5s,
      .on_timeout_allow = true,
    };

    CHECK(approval::resolve_verdict(p, [] { return fake_info{"x"}; }) == false);
  }

  TEST_CASE("approval::resolve_verdict bounded timeout uses on_timeout_allow and signals stop")
  {
    std::atomic<bool> saw_stop{false};

    approval::policy<fake_info> p = approval::bounded<fake_info>{
      .predicate =
        [&saw_stop](fake_info const &, stdexec::inplace_stop_token tok)
      {
        // Spin until either stop is requested (the wrapper's escape hatch
        // after timeout) or a generous safety cap.
        for (int i = 0; i < 200; ++i)
        {
          if (tok.stop_requested())
          {
            saw_stop.store(true, std::memory_order_release);
            return true;  // value never observed by caller — past timeout
          }
          std::this_thread::sleep_for(10ms);
        }
        return true;
      },
      .timeout          = 50ms,
      .on_timeout_allow = false,  // intentionally distinct from predicate's return
    };

    CHECK(approval::resolve_verdict(p, [] { return fake_info{"x"}; }) == false);

    // Give the detached worker a moment to observe the stop request.
    for (int i = 0; i < 100 && !saw_stop.load(std::memory_order_acquire); ++i)
      std::this_thread::sleep_for(10ms);
    CHECK(saw_stop.load(std::memory_order_acquire));
  }
}  // namespace
