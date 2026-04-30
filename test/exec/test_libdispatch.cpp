/*
 * Copyright (c) 2024 Rishabh Dwivedi <rishabhdwivedi17@gmail.com>
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

#include "catch2/catch_all.hpp"
#include "exec/libdispatch_queue.hpp"
#include "stdexec/execution.hpp"

#include <numeric>
#include <string>
#include <utility>
#include <vector>

namespace
{
  TEST_CASE("libdispatch queue should be able to process tasks")
  {
    exec::libdispatch_queue queue;
    auto                    sch = queue.get_scheduler();

    std::vector<int> data{1, 2, 3, 4, 5};
    auto             add = [](auto const & data)
    {
      return std::accumulate(std::begin(data), std::end(data), 0);
    };
    auto sender = STDEXEC::just(std::move(data)) | STDEXEC::continues_on(sch) | STDEXEC::then(add);

    auto completion_scheduler = STDEXEC::get_completion_scheduler<STDEXEC::set_value_t>(
      STDEXEC::get_env(sender));

    CHECK(completion_scheduler == sch);
    auto [res] = STDEXEC::sync_wait(sender).value();
    CHECK(res == 15);
  }

  TEST_CASE("libdispatch queue bulk algorithm should call callback function with all allowed "
            "shapes")
  {
    exec::libdispatch_queue queue;
    auto                    sch = queue.get_scheduler();

    std::vector<int> data{1, 2, 3, 4, 5};
    auto             size                  = data.size();
    auto             expensive_computation = [](auto i, auto& data)
    {
      data[i] = 2 * data[i];
    };
    auto add = [](auto const & data)
    {
      return std::accumulate(std::begin(data), std::end(data), 0);
    };
    auto sender = STDEXEC::just(std::move(data)) | STDEXEC::continues_on(sch)
                | STDEXEC::bulk(STDEXEC::par, size, expensive_computation) | STDEXEC::then(add);

    auto completion_scheduler = STDEXEC::get_completion_scheduler<STDEXEC::set_value_t>(
      STDEXEC::get_env(sender));

    CHECK(completion_scheduler == sch);
    auto [res] = STDEXEC::sync_wait(sender).value();
    CHECK(res == 30);
  }

  TEST_CASE("libdispatch bulk should handle exceptions gracefully")
  {
    exec::libdispatch_queue queue;
    auto                    sch = queue.get_scheduler();

    std::vector<int> data{1, 2, 3, 4, 5};
    auto             size                  = data.size();
    auto             expensive_computation = [](auto i, auto data)
    {
      if (i == 0)
        throw 999;
      return 2 * data[i];
    };
    auto add = [](auto const & data)
    {
      return std::accumulate(std::begin(data), std::end(data), 0);
    };
    auto sender = STDEXEC::just(std::move(data)) | STDEXEC::continues_on(sch)
                | STDEXEC::bulk(STDEXEC::par, size, expensive_computation) | STDEXEC::then(add);

    STDEXEC_TRY
    {
      STDEXEC::sync_wait(sender);
      CHECK(false);
    }
    STDEXEC_CATCH(int e)
    {
      CHECK(e == 999);
    }
    STDEXEC_CATCH_ALL
    {
      FAIL("invalid exception caught");
    }
  }

  TEST_CASE("libdispatch_queue::native_handle returns a valid dispatch_queue_t")
  {
    exec::libdispatch_queue queue;
    dispatch_queue_t        handle = queue.native_handle();
    CHECK(handle != nullptr);
    // For default ctor (global queue), native_handle returns
    // dispatch_get_global_queue with the configured priority.
    CHECK(handle == dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0));
  }

  TEST_CASE("libdispatch_queue::make_serial creates a labelled serial queue")
  {
    auto             q   = exec::libdispatch_queue::make_serial("test.serial");
    dispatch_queue_t raw = q.native_handle();
    REQUIRE(raw != nullptr);
    CHECK(std::string{dispatch_queue_get_label(raw)} == "test.serial");
    auto sch = q.get_scheduler();
    auto [v] =
      STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([] { return 42; })).value();
    CHECK(v == 42);
  }

  TEST_CASE("libdispatch_queue::make_concurrent creates a labelled concurrent queue")
  {
    auto             q   = exec::libdispatch_queue::make_concurrent("test.concurrent");
    dispatch_queue_t raw = q.native_handle();
    REQUIRE(raw != nullptr);
    CHECK(std::string{dispatch_queue_get_label(raw)} == "test.concurrent");
    auto sch = q.get_scheduler();
    auto [v] = STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([] { return 7; })).value();
    CHECK(v == 7);
  }

  TEST_CASE("libdispatch_queue::make_serial(label, target) targets the parent queue")
  {
    auto parent = exec::libdispatch_queue::make_concurrent("test.parent");
    auto child  = exec::libdispatch_queue::make_serial("test.child", parent);
    REQUIRE(child.native_handle() != nullptr);
    REQUIRE(child.native_handle() != parent.native_handle());
    auto sch = child.get_scheduler();
    auto [v] =
      STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([] { return 99; })).value();
    CHECK(v == 99);
  }

  TEST_CASE("libdispatch_queue::wrap retains and releases the raw queue")
  {
    dispatch_queue_t raw = dispatch_queue_create("test.raw", DISPATCH_QUEUE_SERIAL);
    {
      auto wrapped = exec::libdispatch_queue::wrap(raw);
      CHECK(wrapped.native_handle() == raw);
      auto sch = wrapped.get_scheduler();
      auto [v] =
        STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([] { return 5; })).value();
      CHECK(v == 5);
    }
    // wrapped is gone — its dtor released its retain. Our manual retain (from
    // dispatch_queue_create) keeps raw alive. Release it.
    dispatch_release(raw);
  }

  TEST_CASE("libdispatch_queue is movable")
  {
    auto             q   = exec::libdispatch_queue::make_serial("test.move");
    dispatch_queue_t raw = q.native_handle();
    auto             q2  = std::move(q);
    CHECK(q2.native_handle() == raw);
    // q is moved-from; its dtor must not double-release raw.
    auto sch = q2.get_scheduler();
    auto [v] = STDEXEC::sync_wait(STDEXEC::schedule(sch) | STDEXEC::then([] { return 1; })).value();
    CHECK(v == 1);
  }

  TEST_CASE("libdispatch_scheduler exposes native_handle of underlying queue")
  {
    auto q   = exec::libdispatch_queue::make_serial("test.sch.handle");
    auto sch = q.get_scheduler();
    CHECK(sch.native_handle() == q.native_handle());
  }
}  // namespace
