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

// Demo: same FSEvents wrapper, but the consumer is a coroutine that
// `co_await`s an async channel fed by the sender pipeline.

#include "fsevents_wrapper.hpp"

#include "exec/libdispatch_queue.hpp"
#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/task.hpp"
#include "exec/when_any.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

// ---------------------------------------------------------------------------
// Tiny single-slot async channel: producer.push() blocks (backpressure),
// consumer.pop() returns a sender that completes with std::optional<T>
// (nullopt once the channel is closed and drained).
// ---------------------------------------------------------------------------

namespace fsxchan
{
  template <class T>
  class chan
  {
   public:
    void push(T v)
    {
      std::unique_lock lk{m_};
      push_cv_.wait(lk, [&] { return !slot_.has_value() || closed_; });
      if (closed_)
        return;
      if (waiter_)
      {
        auto* w = std::exchange(waiter_, nullptr);
        lk.unlock();
        w->deliver(std::optional<T>{std::move(v)});
        return;
      }
      slot_.emplace(std::move(v));
    }

    void close()
    {
      waiter_base* w = nullptr;
      {
        std::lock_guard lk{m_};
        closed_ = true;
        push_cv_.notify_all();
        w = std::exchange(waiter_, nullptr);
      }
      if (w)
        w->deliver(std::nullopt);
    }

    struct pop_sender;
    auto pop() -> pop_sender
    {
      return {this};
    }

   private:
    struct waiter_base
    {
      virtual void deliver(std::optional<T>) noexcept = 0;
    };

    std::mutex              m_;
    std::condition_variable push_cv_;
    std::optional<T>       slot_;
    bool                    closed_{false};
    waiter_base*          waiter_{nullptr};

   public:
    struct pop_sender
    {
      using sender_concept = stdexec::sender_t;
      using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(std::optional<T>),
                                       stdexec::set_stopped_t()>;

      chan* ch_;

      template <class Rcvr>
      struct op : waiter_base
      {
        chan* ch_;
        Rcvr rcvr_;

        explicit op(chan* ch, Rcvr r)
          : ch_{ch}
          , rcvr_{std::move(r)}
        {}

        struct on_stop_fn
        {
          op* self_;
          void  operator()() noexcept
          {
            bool was_waiter = false;
            {
              std::lock_guard lk{self_->ch_->m_};
              if (self_->ch_->waiter_ == self_)
              {
                self_->ch_->waiter_ = nullptr;
                was_waiter              = true;
              }
            }
            if (was_waiter)
              stdexec::set_stopped(static_cast<Rcvr&&>(self_->rcvr_));
          }
        };

        using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
        using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;
        std::optional<stop_callback_t> stop_cb_;

        void start() & noexcept
        {
          std::unique_lock lk{ch_->m_};
          if (ch_->slot_)
          {
            auto v = std::move(*ch_->slot_);
            ch_->slot_.reset();
            ch_->push_cv_.notify_one();
            lk.unlock();
            stdexec::set_value(static_cast<Rcvr&&>(rcvr_), std::optional<T>{std::move(v)});
            return;
          }
          if (ch_->closed_)
          {
            lk.unlock();
            stdexec::set_value(static_cast<Rcvr&&>(rcvr_), std::optional<T>{});
            return;
          }
          ch_->waiter_ = this;
          lk.unlock();
          stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)),
                             on_stop_fn{this});
        }

        void deliver(std::optional<T> v) noexcept override
        {
          stop_cb_.reset();
          stdexec::set_value(static_cast<Rcvr&&>(rcvr_), std::move(v));
        }
      };

      template <stdexec::receiver Rcvr>
      auto connect(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ch_, std::move(rcvr)};
      }
    };
  };
}  // namespace fsxchan

// ---------------------------------------------------------------------------
// Owned batch (events span dies with the callback frame; the coroutine needs
// to outlive that, so copy into its own vector).
// ---------------------------------------------------------------------------

struct fs_batch_owned
{
  std::vector<fsx::fs_event> events;
  FSEventStreamEventId       last_id{};
  bool                       had_drops{};
  bool                       must_rescan{};
};

// ---------------------------------------------------------------------------
// Coroutine consumer: pulls batches one at a time from the channel.
// ---------------------------------------------------------------------------

auto consume(fsxchan::chan<fs_batch_owned>& ch) -> exec::task<int>
{
  int count = 0;
  while (auto maybe = co_await ch.pop())
  {
    auto const & b = *maybe;
    if (b.must_rescan)
      std::printf("[coro] rescan requested\n");
    for (auto const & e: b.events)
    {
      if (fsx::is_drop_notice(e))
      {
        std::printf("[coro] drop notice flags=%#x path=%s\n",
                    static_cast<unsigned>(e.flags),
                    e.path.c_str());
        continue;
      }
      std::printf("[coro] id=%llu flags=%#x path=%s\n",
                  static_cast<unsigned long long>(e.id),
                  static_cast<unsigned>(e.flags),
                  e.path.c_str());
    }
    std::printf("[coro] batch consumed last_id=%llu (#%d)\n",
                static_cast<unsigned long long>(b.last_id),
                count);
    ++count;
  }
  std::printf("[coro] channel closed, total batches = %d\n", count);
  co_return count;
}

namespace fs = std::filesystem;
using namespace std::chrono_literals;

auto main() -> int
{
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  auto dir = fs::temp_directory_path() / "fsx_demo_coro";
  fs::create_directories(dir);
  for (auto const & e: fs::directory_iterator{dir})
    fs::remove_all(e.path());
  dir = fs::canonical(dir);
  std::printf("watching %s\n", dir.c_str());

  fsx::fsevents_context         ctx{{dir.string()}};
  fsxchan::chan<fs_batch_owned> ch;

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

  exec::static_thread_pool pool{2};
  auto                     sched    = pool.get_scheduler();
  exec::libdispatch_queue  fsx_pool = exec::libdispatch_queue::make_concurrent("fsx.coro."
                                                                                 "producer");

  // Run the producer on a worker thread (its push() blocks the dispatch queue
  // for backpressure), and consume() in the foreground. A 3s timer cancels
  // both via stop_token, then close() releases any blocked push.
  std::atomic<bool> producer_done{false};
  std::thread       producer_thread{
    [&]
    {
      auto pipeline = exec::sequence_with_scheduler(fsx_pool.get_scheduler(), ctx.watch())
                      | exec::transform_each(stdexec::then(
                        [&](fsx::fs_batch b)
                        {
                          std::printf("[prod] pushing batch last_id=%llu (%zu events)\n",
                                      static_cast<unsigned long long>(b.last_id),
                                      b.events.size());
                          ch.push(fs_batch_owned{
                                  {b.events.begin(), b.events.end()},
                            b.last_id,
                            b.had_drops,
                            b.must_rescan
                          });
                        }))
                      | exec::ignore_all_values();
      stdexec::sync_wait(exec::when_any(stdexec::starts_on(sched, stdexec::just())
                                          | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
                                        std::move(pipeline)));
      producer_done.store(true);
      ch.close();  // wake any blocked consumer pop
    }};

  auto [count] = stdexec::sync_wait(consume(ch)
                                      | stdexec::then(
                                        [](int n)
                                        {
                                          std::printf("[main] consumer returned %d\n", n);
                                          return n;
                                        }))
                     .value();
  (void) count;

  producer_thread.join();

  mutator_stop.store(true);
  mutator.join();

  std::printf("final last_completed_id = %llu\n",
              static_cast<unsigned long long>(ctx.last_completed_id()));
  return 0;
}
