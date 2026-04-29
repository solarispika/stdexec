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
  template <class _T>
  class chan
  {
   public:
    void push(_T __v)
    {
      std::unique_lock __lk{__m_};
      __push_cv_.wait(__lk, [&] { return !__slot_.has_value() || __closed_; });
      if (__closed_)
        return;
      if (__waiter_)
      {
        auto* __w = std::exchange(__waiter_, nullptr);
        __lk.unlock();
        __w->__deliver(std::optional<_T>{std::move(__v)});
        return;
      }
      __slot_.emplace(std::move(__v));
    }

    void close()
    {
      __waiter_base* __w = nullptr;
      {
        std::lock_guard __lk{__m_};
        __closed_ = true;
        __push_cv_.notify_all();
        __w = std::exchange(__waiter_, nullptr);
      }
      if (__w)
        __w->__deliver(std::nullopt);
    }

    struct __pop_sender;
    auto pop() -> __pop_sender { return {this}; }

   private:
    struct __waiter_base
    {
      virtual void __deliver(std::optional<_T>) noexcept = 0;
    };

    std::mutex              __m_;
    std::condition_variable __push_cv_;
    std::optional<_T>       __slot_;
    bool                    __closed_{false};
    __waiter_base*          __waiter_{nullptr};

   public:
    struct __pop_sender
    {
      using sender_concept = stdexec::sender_t;
      using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(std::optional<_T>),
                                       stdexec::set_stopped_t()>;

      chan* __ch_;

      template <class _Rcvr>
      struct __op : __waiter_base
      {
        chan* __ch_;
        _Rcvr __rcvr_;

        explicit __op(chan* __ch, _Rcvr __r)
          : __ch_{__ch}
          , __rcvr_{std::move(__r)}
        {}

        struct __on_stop_fn
        {
          __op* __self_;
          void  operator()() noexcept
          {
            bool __was_waiter = false;
            {
              std::lock_guard __lk{__self_->__ch_->__m_};
              if (__self_->__ch_->__waiter_ == __self_)
              {
                __self_->__ch_->__waiter_ = nullptr;
                __was_waiter              = true;
              }
            }
            if (__was_waiter)
              stdexec::set_stopped(static_cast<_Rcvr&&>(__self_->__rcvr_));
          }
        };

        using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
        using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;
        std::optional<__stop_callback_t> __stop_cb_;

        void start() & noexcept
        {
          std::unique_lock __lk{__ch_->__m_};
          if (__ch_->__slot_)
          {
            auto __v = std::move(*__ch_->__slot_);
            __ch_->__slot_.reset();
            __ch_->__push_cv_.notify_one();
            __lk.unlock();
            stdexec::set_value(static_cast<_Rcvr&&>(__rcvr_), std::optional<_T>{std::move(__v)});
            return;
          }
          if (__ch_->__closed_)
          {
            __lk.unlock();
            stdexec::set_value(static_cast<_Rcvr&&>(__rcvr_), std::optional<_T>{});
            return;
          }
          __ch_->__waiter_ = this;
          __lk.unlock();
          __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)),
                             __on_stop_fn{this});
        }

        void __deliver(std::optional<_T> __v) noexcept override
        {
          __stop_cb_.reset();
          stdexec::set_value(static_cast<_Rcvr&&>(__rcvr_), std::move(__v));
        }
      };

      template <stdexec::receiver _Rcvr>
      auto connect(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ch_, std::move(__rcvr)};
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

auto consume(fsxchan::chan<fs_batch_owned>& __ch) -> exec::task<int>
{
  int __count = 0;
  while (auto __maybe = co_await __ch.pop())
  {
    const auto& __b = *__maybe;
    if (__b.must_rescan)
      std::printf("[coro] rescan requested\n");
    for (const auto& __e : __b.events)
    {
      if (fsx::is_drop_notice(__e))
      {
        std::printf("[coro] drop notice flags=%#x path=%s\n",
                    static_cast<unsigned>(__e.flags),
                    __e.path.c_str());
        continue;
      }
      std::printf("[coro] id=%llu flags=%#x path=%s\n",
                  static_cast<unsigned long long>(__e.id),
                  static_cast<unsigned>(__e.flags),
                  __e.path.c_str());
    }
    std::printf("[coro] batch consumed last_id=%llu (#%d)\n",
                static_cast<unsigned long long>(__b.last_id),
                __count);
    ++__count;
  }
  std::printf("[coro] channel closed, total batches = %d\n", __count);
  co_return __count;
}

namespace fs = std::filesystem;
using namespace std::chrono_literals;

auto main() -> int
{
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  auto __dir = fs::temp_directory_path() / "fsx_demo_coro";
  fs::create_directories(__dir);
  for (const auto& __e : fs::directory_iterator{__dir})
    fs::remove_all(__e.path());
  __dir = fs::canonical(__dir);
  std::printf("watching %s\n", __dir.c_str());

  fsx::fsevents_context        __ctx{{__dir.string()}};
  fsxchan::chan<fs_batch_owned> __ch;

  std::atomic<bool> __mutator_stop{false};
  std::thread       __mutator{[&] {
    for (int __i = 0; !__mutator_stop.load() && __i < 5; ++__i)
    {
      std::this_thread::sleep_for(400ms);
      std::ofstream __f{__dir / ("file_" + std::to_string(__i) + ".txt")};
      __f << "hello " << __i << "\n";
    }
  }};

  exec::static_thread_pool __pool{2};
  auto                     __sched = __pool.get_scheduler();
  exec::libdispatch_queue  __fsx_pool =
    exec::libdispatch_queue::make_concurrent("fsx.coro.producer");

  // Run the producer on a worker thread (its push() blocks the dispatch queue
  // for backpressure), and consume() in the foreground. A 3s timer cancels
  // both via stop_token, then close() releases any blocked push.
  std::atomic<bool> __producer_done{false};
  std::thread       __producer_thread{[&] {
    auto __pipeline =
      fsx::on_queue(__fsx_pool.get_scheduler(), __ctx.watch())
      | exec::transform_each(stdexec::then([&](fsx::fs_batch __b) {
          std::printf("[prod] pushing batch last_id=%llu (%zu events)\n",
                      static_cast<unsigned long long>(__b.last_id),
                      __b.events.size());
          __ch.push(fs_batch_owned{
            {__b.events.begin(), __b.events.end()},
            __b.last_id,
            __b.had_drops,
            __b.must_rescan});
        }))
      | exec::ignore_all_values();
    stdexec::sync_wait(exec::when_any(
      stdexec::starts_on(__sched, stdexec::just())
        | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
      std::move(__pipeline)));
    __producer_done.store(true);
    __ch.close();  // wake any blocked consumer pop
  }};

  auto [__count] =
    stdexec::sync_wait(consume(__ch) | stdexec::then([](int __n) {
                        std::printf("[main] consumer returned %d\n", __n);
                        return __n;
                      }))
      .value();
  (void) __count;

  __producer_thread.join();

  __mutator_stop.store(true);
  __mutator.join();

  std::printf("final last_completed_id = %llu\n",
              static_cast<unsigned long long>(__ctx.last_completed_id()));
  return 0;
}
