# inotify × io_uring sequence sender — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Linux-only example wrapping `inotify` as an `exec::sequence_sender_t`, with IO completions driven by `exec::io_uring_context` (parallel to FSEvents/libdispatch on macOS and ReadDirectoryChangesW/Win32-thread-pool on Windows).

**Architecture:** Wrapper does not own a thread. User supplies `exec::io_uring_scheduler` via env using `inx::on_ring` (shared `exec::__on_scheduler_t` adapter). `IORING_OP_READ` on the inotify fd; CQE on the reactor thread parses the buffer, dispatches via `set_next`, and the next read is reposted from `next_receiver::set_value` (continuation-style backpressure, same shape as `rdcx::pool`). Cancellation via `IORING_OP_ASYNC_CANCEL`. Multi-path watch with thread-safe `add_watch` / `remove_watch` / `path_for(wd)`; single active subscription per context (CAS).

**Tech Stack:** C++20, stdexec, `exec/linux/io_uring_context.hpp`, Linux `<sys/inotify.h>`, liburing's `io_uring_sqe` / `io_uring_cqe` raw structs (no liburing helpers — io_uring_context.hpp uses raw structs and direct syscalls).

**Spec:** `docs/plans/2026-04-29-inotify-io_uring-sender-design.md`

---

## File map

- **Create**: `examples/inotify_wrapper.hpp` — header-only `inx` namespace
- **Create**: `examples/inotify.cpp` — demo
- **Create**: `examples/inotify_README.md`
- **Modify**: `examples/CMakeLists.txt` — add `example.inotify` to existing `if (LINUX)` block

---

## Task 1: Scaffold — `inotify_context` (fd + path map), CMakeLists, smoke demo

This task lands a buildable Linux-only target that opens an inotify fd, registers initial paths, exposes `add_watch` / `remove_watch` / `path_for`, and `close()`s the fd in dtor. No sequence sender yet. Catches early issues with includes, namespaces, kernel API, and the build hookup before adding async complexity.

**Files:**
- Create: `examples/inotify_wrapper.hpp`
- Create: `examples/inotify.cpp` (smoke version)
- Modify: `examples/CMakeLists.txt:65-69` (existing `if (LINUX)` block)

### Steps

- [ ] **Step 1.1 — Create header skeleton** (`examples/inotify_wrapper.hpp`)

```cpp
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
#pragma once

// Linux-only: wraps inotify as a stdexec sequence sender driven by
// exec::io_uring_context (IORING_OP_READ on the inotify fd).

#include <sys/inotify.h>
#include <unistd.h>

#include "exec/linux/io_uring_context.hpp"
#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace inx
{
  struct fs_event
  {
    int         wd;       // -1 means IN_Q_OVERFLOW (filtered from events span)
    std::uint32_t mask;
    std::uint32_t cookie;
    std::string name;
  };

  struct fs_batch
  {
    std::span<const fs_event> events;
    bool                      overflow;
  };

  struct watch_options
  {
    std::uint32_t mask = IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MODIFY
                       | IN_ATTRIB | IN_MOVED_FROM | IN_MOVED_TO | IN_MOVE_SELF
                       | IN_CLOSE_WRITE;
    std::size_t   buffer_size = 64 * 1024;
  };

  class inotify_context;

  namespace __detail
  {
    struct __op_base;
    template <class _Rcvr> struct __op;
    template <class _Rcvr> struct __next_receiver;
    struct __watch_sender;
  }

  class inotify_context
  {
   public:
    explicit inotify_context(std::vector<std::string> __initial_paths,
                             std::uint32_t __default_mask = watch_options{}.mask);
    ~inotify_context();

    inotify_context(const inotify_context&)                    = delete;
    auto operator=(const inotify_context&) -> inotify_context& = delete;

    auto add_watch(std::string_view __path,
                   std::optional<std::uint32_t> __mask = std::nullopt) -> int;

    auto remove_watch(int __wd) noexcept -> bool;

    auto path_for(int __wd) const -> std::optional<std::string>;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr> friend struct __detail::__op;
    friend struct __detail::__watch_sender;

    int                                       __fd_{-1};
    std::uint32_t                             __default_mask_;
    mutable std::mutex                        __map_mu_;
    std::unordered_map<int, std::string>      __wd_to_path_;
    std::atomic<__detail::__op_base*>         __active_{nullptr};
  };

  // env-injection adapter — mirrors fsx::on_queue / rdcx::pool::on_pool.
  inline constexpr exec::__on_scheduler_t on_ring{};

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base() = default;
    };

    struct __watch_sender
    {
      using sender_concept = exec::sequence_sender_tag;
      using completion_signatures = stdexec::completion_signatures<
        stdexec::set_value_t(),
        stdexec::set_stopped_t(),
        stdexec::set_error_t(std::exception_ptr)>;

      using __item_sender_t = decltype(stdexec::just(std::declval<fs_batch>()));
      using item_types      = exec::item_types<__item_sender_t>;

      inotify_context* __ctx_;
      watch_options    __opts_;
    };
  }

  // ---------- inotify_context impl ----------

  inline inotify_context::inotify_context(std::vector<std::string> __initial_paths,
                                          std::uint32_t __default_mask)
    : __default_mask_{__default_mask}
  {
    __fd_ = ::inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
    if (__fd_ < 0)
    {
      throw std::system_error{errno, std::system_category(), "inotify_init1"};
    }
    try
    {
      for (const auto& __p : __initial_paths)
      {
        add_watch(__p);
      }
    }
    catch (...)
    {
      ::close(__fd_);
      __fd_ = -1;
      throw;
    }
  }

  inline inotify_context::~inotify_context()
  {
    if (__fd_ >= 0)
    {
      ::close(__fd_);
    }
  }

  inline auto inotify_context::add_watch(std::string_view __path,
                                         std::optional<std::uint32_t> __mask) -> int
  {
    std::string __zpath{__path};  // inotify_add_watch needs NUL-terminated
    int __wd = ::inotify_add_watch(__fd_, __zpath.c_str(),
                                   __mask.value_or(__default_mask_));
    if (__wd < 0)
    {
      throw std::system_error{errno, std::system_category(), "inotify_add_watch"};
    }
    {
      std::lock_guard __lk{__map_mu_};
      __wd_to_path_[__wd] = std::move(__zpath);
    }
    return __wd;
  }

  inline auto inotify_context::remove_watch(int __wd) noexcept -> bool
  {
    {
      std::lock_guard __lk{__map_mu_};
      if (!__wd_to_path_.erase(__wd))
      {
        return false;
      }
    }
    // inotify_rm_watch: kernel will emit IN_IGNORED for this wd; harmless if it
    // races with our own erase above (the IN_IGNORED handler is a no-op then).
    return ::inotify_rm_watch(__fd_, __wd) == 0;
  }

  inline auto inotify_context::path_for(int __wd) const -> std::optional<std::string>
  {
    std::lock_guard __lk{__map_mu_};
    auto __it = __wd_to_path_.find(__wd);
    if (__it == __wd_to_path_.end())
    {
      return std::nullopt;
    }
    return __it->second;
  }

  inline auto inotify_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace inx
```

- [ ] **Step 1.2 — Create smoke demo** (`examples/inotify.cpp`)

```cpp
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
```

- [ ] **Step 1.3 — Wire into CMakeLists** (`examples/CMakeLists.txt:65-69`)

Replace:
```cmake
if (LINUX)
  set(stdexec_examples ${stdexec_examples}
                    "example.io_uring : io_uring.cpp"
  )
endif ()
```
with:
```cmake
if (LINUX)
  set(stdexec_examples ${stdexec_examples}
                    "example.io_uring : io_uring.cpp"
                    "example.inotify : inotify.cpp"
  )
endif ()
```

- [ ] **Step 1.4 — Build**

Run: `cmake --build build --target example.inotify`

Expected: target builds without errors. If `build/` doesn't exist, configure first with the project's standard CMake invocation (whatever `build/` was previously configured with — likely `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DSTDEXEC_BUILD_EXAMPLES=ON`).

- [ ] **Step 1.5 — Run smoke binary**

Run: `./build/examples/example.inotify`

Expected: prints `inotify_context constructed; path_for(1) has_value=...` and exits 0. It is acceptable for `has_value` to be either 0 or 1 — what we are checking is that the context constructed, registered the path, and didn't crash.

- [ ] **Step 1.6 — Commit**

```bash
git add examples/inotify_wrapper.hpp examples/inotify.cpp examples/CMakeLists.txt
git commit -m "$(cat <<'EOF'
examples: scaffold inotify_context (fd + wd→path map)

Linux-only example skeleton. inotify_context owns the inotify fd,
exposes thread-safe add_watch / remove_watch / path_for, and is wired
into the build via examples/CMakeLists.txt. No sequence sender yet —
that lands in the next commit.
EOF
)"
```

---

## Task 2: Sequence sender wiring — `__op`, `__watch_sender`, IORING_OP_READ, continuation backpressure

This task lands the working watch pipeline (no cancellation yet — that's Task 3). On `start()`, `__op` submits an `IORING_OP_READ` against the inotify fd via `__io_task_facade`. The CQE handler parses the buffer into `fs_event`s, builds an `fs_batch`, and dispatches via `set_next`. The `next_receiver::set_value` continuation reposts the next READ.

**Files:**
- Modify: `examples/inotify_wrapper.hpp` (add `__op`, `__next_receiver`, `__op_base::deliver`, `__read_task`, fill `__watch_sender::subscribe`)
- Modify: `examples/inotify.cpp` (real demo)

### Steps

- [ ] **Step 2.1 — Replace `__op_base` and add `__read_task` + `__op` template** (`examples/inotify_wrapper.hpp`)

In the `__detail` namespace, replace the empty `__op_base` and add the rest. The `__read_task` is a tiny inner type that satisfies `exec::__io_uring::__io_task` (`context() / ready() / submit() / complete()`); it holds a back-pointer to its outer `__op` so the CQE handler can call back. `__io_task_facade<__read_task>` wraps it into a real `__task` with vtable.

Insert in `namespace __detail` before `__watch_sender`:

```cpp
    struct __op_base
    {
      virtual ~__op_base() = default;
      virtual void __on_read_complete(const ::io_uring_cqe&) noexcept = 0;
    };

    template <class _Rcvr>
    struct __op;

    template <class _Rcvr>
    struct __next_receiver
    {
      using receiver_concept = stdexec::receiver_tag;

      __op<_Rcvr>* __self_;

      template <class... _Args>
      void set_value(_Args&&...) noexcept;

      void set_stopped() noexcept;

      template <class _E>
      void set_error(_E&&) noexcept;

      [[nodiscard]]
      auto get_env() const noexcept -> stdexec::env_of_t<_Rcvr>;
    };

    // Thin __io_task base that defers to its outer __op via a back-pointer.
    struct __read_task
    {
      __op_base*                     __outer_;
      experimental::execution::__io_uring::__context*  __ctx_;
      int                            __fd_;
      void*                          __buf_;
      std::size_t                    __buf_len_;

      auto context() noexcept
        -> experimental::execution::__io_uring::__context&
      {
        return *__ctx_;
      }

      static constexpr auto ready() noexcept -> bool { return false; }

      void submit(::io_uring_sqe& __sqe) noexcept
      {
        std::memset(&__sqe, 0, sizeof(__sqe));
        __sqe.opcode = IORING_OP_READ;
        __sqe.fd     = __fd_;
        __sqe.addr   = reinterpret_cast<std::uint64_t>(__buf_);
        __sqe.len    = static_cast<std::uint32_t>(__buf_len_);
        __sqe.off    = 0;
        // user_data is set by __io_uring_context::submit().
      }

      void complete(const ::io_uring_cqe& __cqe) noexcept
      {
        __outer_->__on_read_complete(__cqe);
      }
    };

    using __read_op_t =
      experimental::execution::__io_uring::__io_task_facade<__read_task>;

    template <class _Rcvr>
    struct __op : __op_base
    {
      using __item_sender_t   = decltype(stdexec::just(std::declval<fs_batch>()));
      using __next_sender_t   = exec::next_sender_of_t<_Rcvr, __item_sender_t>;
      using __next_receiver_t = __next_receiver<_Rcvr>;
      using __next_op_t       = stdexec::connect_result_t<__next_sender_t, __next_receiver_t>;

      inotify_context*               __ctx_;
      watch_options                  __opts_;
      _Rcvr                          __rcvr_;
      experimental::execution::__io_uring::__context*  __ring_;
      std::vector<std::byte>         __buffer_;
      std::vector<fs_event>          __staging_;
      std::optional<__read_op_t>     __read_op_;
      std::unique_ptr<__next_op_t>   __next_op_;
      std::atomic<bool>              __stop_requested_{false};
      std::exception_ptr             __error_;

      explicit __op(inotify_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
      {
        auto __sched = stdexec::get_scheduler(stdexec::get_env(__rcvr_));
        __ring_      = __sched.__context_;
        __buffer_.resize(__opts_.buffer_size);
      }

      void start() & noexcept
      {
        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{
                               "inotify_context already has an active watch"}));
          return;
        }
        __post_read();
      }

      void __post_read() noexcept
      {
        // Each read posts a fresh __io_task_facade. The previous one (if any)
        // has already had its complete() returned. Destructing the previous
        // here is safe because we are not nested in its callback (we are
        // either in start() or in next_receiver::set_value).
        __read_op_.emplace(std::in_place,
                           __read_task{
                             static_cast<__op_base*>(this),
                             __ring_,
                             __ctx_->__fd_,
                             __buffer_.data(),
                             __buffer_.size()});
        __read_op_->start();
      }

      // Called on the io_uring reactor thread.
      void __on_read_complete(const ::io_uring_cqe& __cqe) noexcept override
      {
        if (__cqe.res < 0)
        {
          if (__cqe.res == -ECANCELED || __stop_requested_.load(std::memory_order_acquire))
          {
            __finish_stopped();
            return;
          }
          __error_ = std::make_exception_ptr(std::system_error{
            -__cqe.res, std::system_category(), "inotify read"});
          __finish_error();
          return;
        }

        __parse_into_staging(static_cast<std::size_t>(__cqe.res));

        bool __overflow = false;
        // IN_Q_OVERFLOW arrives as a synthetic event with wd=-1; surface it
        // batch-level and remove it from the events span.
        std::erase_if(__staging_, [&](const fs_event& __e) {
          if (__e.wd == -1 && (__e.mask & IN_Q_OVERFLOW))
          {
            __overflow = true;
            return true;
          }
          return false;
        });

        fs_batch __batch{__staging_, __overflow};

        try
        {
          __next_op_.reset(new __next_op_t(stdexec::connect(
            exec::set_next(__rcvr_, stdexec::just(__batch)),
            __next_receiver_t{this})));
          stdexec::start(*__next_op_);
        }
        catch (...)
        {
          __error_ = std::current_exception();
          __finish_error();
        }
      }

      void __parse_into_staging(std::size_t __bytes) noexcept
      {
        __staging_.clear();
        std::size_t __off = 0;
        while (__off + sizeof(::inotify_event) <= __bytes)
        {
          const auto* __ev = reinterpret_cast<const ::inotify_event*>(
            __buffer_.data() + __off);
          std::size_t __record = sizeof(::inotify_event) + __ev->len;
          if (__off + __record > __bytes) break;

          // IN_IGNORED: the kernel has dropped this watch. Clean wd→path map.
          if ((__ev->mask & IN_IGNORED) && __ev->wd >= 0)
          {
            std::lock_guard __lk{__ctx_->__map_mu_};
            __ctx_->__wd_to_path_.erase(__ev->wd);
          }

          std::string __name;
          if (__ev->len > 0)
          {
            // name is NUL-padded; strlen gives the real size.
            __name.assign(__ev->name, ::strnlen(__ev->name, __ev->len));
          }
          __staging_.push_back({__ev->wd, __ev->mask, __ev->cookie,
                                std::move(__name)});

          __off += __record;
        }
      }

      void __on_next_value() noexcept
      {
        // Do NOT reset __next_op_ here — downstream may complete synchronously
        // inside set_next's start(); destroying the op from inside its own
        // set_value call would tear down the call stack. The next batch's
        // unique_ptr::reset(new ...) will destroy this child after start()
        // unwinds.
        if (__stop_requested_.load(std::memory_order_acquire))
        {
          __finish_stopped();
          return;
        }
        __post_read();
      }

      void __on_next_stopped() noexcept { __finish_stopped(); }

      void __on_next_error(std::exception_ptr __ep) noexcept
      {
        __error_ = std::move(__ep);
        __finish_error();
      }

      void __teardown() noexcept
      {
        __next_op_.reset();
        __read_op_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);
      }

      void __finish_stopped() noexcept
      {
        __teardown();
        stdexec::set_stopped(static_cast<_Rcvr&&>(__rcvr_));
      }

      void __finish_error() noexcept
      {
        __teardown();
        stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), std::move(__error_));
      }
    };

    template <class _Rcvr>
    template <class... _Args>
    void __next_receiver<_Rcvr>::set_value(_Args&&...) noexcept
    {
      __self_->__on_next_value();
    }

    template <class _Rcvr>
    void __next_receiver<_Rcvr>::set_stopped() noexcept
    {
      __self_->__on_next_stopped();
    }

    template <class _Rcvr>
    template <class _E>
    void __next_receiver<_Rcvr>::set_error(_E&& __e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<_E>, std::exception_ptr>)
      {
        __self_->__on_next_error(std::forward<_E>(__e));
      }
      else
      {
        __self_->__on_next_error(std::make_exception_ptr(std::forward<_E>(__e)));
      }
    }

    template <class _Rcvr>
    auto __next_receiver<_Rcvr>::get_env() const noexcept -> stdexec::env_of_t<_Rcvr>
    {
      return stdexec::get_env(__self_->__rcvr_);
    }
```

- [ ] **Step 2.2 — Fill `__watch_sender::subscribe`** (`examples/inotify_wrapper.hpp`)

Replace the empty `__watch_sender` definition body (already declared in Task 1) with one that has `subscribe`:

```cpp
    struct __watch_sender
    {
      using sender_concept = exec::sequence_sender_tag;
      using completion_signatures = stdexec::completion_signatures<
        stdexec::set_value_t(),
        stdexec::set_stopped_t(),
        stdexec::set_error_t(std::exception_ptr)>;

      using __item_sender_t = decltype(stdexec::just(std::declval<fs_batch>()));
      using item_types      = exec::item_types<__item_sender_t>;

      inotify_context* __ctx_;
      watch_options    __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                           exec::io_uring_scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };
```

- [ ] **Step 2.3 — Rewrite demo to consume events** (`examples/inotify.cpp`)

```cpp
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

// Demo: consume the inotify sequence sender via a sender pipeline.

#include "inotify_wrapper.hpp"

#include "exec/linux/io_uring_context.hpp"
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

auto main() -> int
{
  auto __dir = fs::temp_directory_path() / "inx_demo";
  fs::create_directories(__dir);
  for (const auto& __e : fs::directory_iterator{__dir})
  {
    fs::remove_all(__e.path());
  }
  std::printf("watching %s\n", __dir.c_str());

  exec::io_uring_context __ring;
  std::thread            __driver{[&] { __ring.run_until_stopped(); }};

  inx::inotify_context __ctx{{__dir.string()}};

  std::atomic<bool> __mutator_stop{false};
  std::thread       __mutator{[&] {
    for (int __i = 0; !__mutator_stop.load() && __i < 5; ++__i)
    {
      std::this_thread::sleep_for(400ms);
      std::ofstream __f{__dir / ("file_" + std::to_string(__i) + ".txt")};
      __f << "hello " << __i << "\n";
    }
  }};

  exec::static_thread_pool __pool{1};
  auto                     __sched = __pool.get_scheduler();
  stdexec::sync_wait(exec::when_any(
    stdexec::starts_on(__sched, stdexec::just())
      | stdexec::then([&] { std::this_thread::sleep_for(3s); }),
    inx::on_ring(__ring.get_scheduler(), __ctx.watch())
      | exec::transform_each(stdexec::then([&](inx::fs_batch __b) {
          if (__b.overflow)
          {
            std::printf("[overflow] kernel inotify queue overflowed; rescan required\n");
          }
          for (const auto& __e : __b.events)
          {
            auto __p = __ctx.path_for(__e.wd);
            std::printf("wd=%d mask=%#x cookie=%u root=%s name=%s\n",
                        __e.wd,
                        static_cast<unsigned>(__e.mask),
                        static_cast<unsigned>(__e.cookie),
                        __p ? __p->c_str() : "?",
                        __e.name.c_str());

            // Demo dynamic add_watch: when a subdirectory is created, follow it.
            if ((__e.mask & IN_CREATE) && (__e.mask & IN_ISDIR) && __p)
            {
              try
              {
                __ctx.add_watch(*__p + "/" + __e.name);
              }
              catch (const std::system_error& __ex)
              {
                std::printf("add_watch failed: %s\n", __ex.what());
              }
            }
          }
        }))
      | exec::ignore_all_values()));

  __mutator_stop.store(true);
  __mutator.join();
  __ring.request_stop();
  __driver.join();
  return 0;
}
```

- [ ] **Step 2.4 — Build**

Run: `cmake --build build --target example.inotify`

Expected: builds clean. If you get a compile error about `__io_task_facade` ambiguous ctor, double-check Step 2.1's `__post_read` — it must use `std::in_place` as the first ctor arg so the in-place facade ctor is selected.

- [ ] **Step 2.5 — Run**

Run: `./build/examples/example.inotify`

Expected: prints `watching /tmp/inx_demo`, then ~5 `wd=N mask=0x... name=file_M.txt` lines as the mutator creates files. Process exits 0 after ~3 seconds.

Note: this run **leaks the `__op`** if the timer wins and cancels — Task 3 fixes that. For Task 2 verification, the demo terminates only because `when_any` set_stopped propagates downstream, and the watch op's child next-receiver will eventually receive set_stopped, which currently routes through `__on_next_stopped` → `__finish_stopped`. The remaining hazard is an in-flight `IORING_OP_READ` at the moment of stop — which Task 3 addresses with `ASYNC_CANCEL`. For Task 2, expect the demo to either exit cleanly or hang for up to a few seconds until the next inotify event jolts the read; if it hangs, kill it manually — that's fine for this checkpoint.

- [ ] **Step 2.6 — Commit**

```bash
git add examples/inotify_wrapper.hpp examples/inotify.cpp
git commit -m "$(cat <<'EOF'
examples: inotify watch sender (READ + continuation backpressure)

__op submits IORING_OP_READ via __io_task_facade against the inotify
fd, parses the buffer into fs_event records (filtering IN_Q_OVERFLOW
to fs_batch::overflow, auto-cleaning wd→path on IN_IGNORED), and
dispatches via set_next. next_receiver::set_value reposts the next
READ — same continuation-style backpressure as rdcx::pool. No
cancellation yet (next commit).
EOF
)"
```

---

## Task 3: Cancellation — stop callback + `IORING_OP_ASYNC_CANCEL`

This task makes the watch cleanly cancellable. We register a stop callback at the end of `start()` (after the first read is in flight). The callback flips `__stop_requested_` and submits an `IORING_OP_ASYNC_CANCEL` SQE targeting the in-flight READ. The READ then completes with `-ECANCELED`, which `__on_read_complete` already routes to `__finish_stopped`.

**Files:**
- Modify: `examples/inotify_wrapper.hpp` (add `__cancel_task`, stop callback wiring)

### Steps

- [ ] **Step 3.1 — Add `__cancel_task` and stop-callback type** (`examples/inotify_wrapper.hpp`)

Insert in `namespace __detail` after `__read_task`:

```cpp
    // Single-shot SQE that cancels another in-flight task by user_data.
    // The target task's user_data is its __task* (set by io_uring_context).
    struct __cancel_task
    {
      experimental::execution::__io_uring::__context* __ctx_;
      void*                                           __target_user_data_;

      auto context() noexcept
        -> experimental::execution::__io_uring::__context&
      {
        return *__ctx_;
      }

      static constexpr auto ready() noexcept -> bool { return false; }

      void submit(::io_uring_sqe& __sqe) noexcept
      {
        std::memset(&__sqe, 0, sizeof(__sqe));
        __sqe.opcode = IORING_OP_ASYNC_CANCEL;
        __sqe.addr   = reinterpret_cast<std::uint64_t>(__target_user_data_);
      }

      void complete(const ::io_uring_cqe&) noexcept
      {
        // Cancellation result is discarded — the target task's own complete()
        // path is what drives finish_stopped. -ENOENT (already done) and
        // 0 (cancelled) are both valid outcomes.
      }
    };

    using __cancel_op_t =
      experimental::execution::__io_uring::__io_task_facade<__cancel_task>;
```

- [ ] **Step 3.2 — Add stop-callback machinery to `__op`** (`examples/inotify_wrapper.hpp`)

In `__op<_Rcvr>` add the on-stop function struct, the stop-callback typedefs, and a member to hold the cancel op + the stop callback. Also extend the lifecycle.

Add these member definitions inside `struct __op` (next to the existing members):

```cpp
      struct __on_stop_fn
      {
        __op* __self_;
        void  operator()() noexcept
        {
          __self_->__stop_requested_.store(true, std::memory_order_release);
          // If a read SQE is in flight, send a cancel SQE for it.
          // __read_op_.has_value() is the simplest gate: it's set across
          // the entire lifetime of an in-flight read, cleared only in
          // __teardown(). Submitting a cancel against an already-completed
          // read is harmless (kernel returns -ENOENT, complete() ignores it).
          if (__self_->__read_op_)
          {
            // user_data is the address of the __task base of the facade.
            void* __tgt = static_cast<
              experimental::execution::__io_uring::__task*>(&*__self_->__read_op_);
            __self_->__cancel_op_.emplace(std::in_place,
                                          __cancel_task{__self_->__ring_, __tgt});
            __self_->__cancel_op_->start();
          }
        }
      };

      using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
      using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;

      std::optional<__cancel_op_t>     __cancel_op_;
      std::optional<__stop_callback_t> __stop_cb_;
```

- [ ] **Step 3.3 — Register stop callback at the end of `start()`** (`examples/inotify_wrapper.hpp`)

Replace the `start()` body with:

```cpp
      void start() & noexcept
      {
        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(std::runtime_error{
                               "inotify_context already has an active watch"}));
          return;
        }
        __post_read();

        // Register stop callback last: if the token is already in stop state
        // it fires synchronously, which is now safe because the read is up.
        // Same pattern as fsevents_wrapper / rdc_wrapper.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)),
                           __on_stop_fn{this});
      }
```

- [ ] **Step 3.4 — Drop stop callback first in `__teardown()`** (`examples/inotify_wrapper.hpp`)

Replace `__teardown()` with:

```cpp
      void __teardown() noexcept
      {
        // Drop the stop callback first so any in-flight invocation finishes
        // before we destroy state it might touch.
        __stop_cb_.reset();
        __next_op_.reset();
        __cancel_op_.reset();
        __read_op_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);
      }
```

- [ ] **Step 3.5 — Build**

Run: `cmake --build build --target example.inotify`

Expected: builds clean.

- [ ] **Step 3.6 — Run, verify clean cancellation**

Run: `./build/examples/example.inotify`

Expected: prints `watching /tmp/inx_demo`, ~5 event lines, then **exits cleanly within 3.x seconds** (no hang). The 3-second timer in `when_any` triggers stop on the watch, the callback submits ASYNC_CANCEL, the in-flight READ completes with -ECANCELED, `__finish_stopped` runs, the pipeline tears down. `__ring.request_stop()` then unblocks the driver thread, `__driver.join()` returns, process exits.

Run it 5 times in a row to check for races: `for i in 1 2 3 4 5; do ./build/examples/example.inotify; done`. All runs should exit cleanly.

- [ ] **Step 3.7 — Commit**

```bash
git add examples/inotify_wrapper.hpp
git commit -m "$(cat <<'EOF'
examples: cancel in-flight inotify READ via IORING_OP_ASYNC_CANCEL

__on_stop_fn submits a cancel SQE targeting the in-flight READ's
user_data. Stop callback registered last in start() (after the first
READ is up). __teardown drops the stop callback first so an in-flight
invocation can't race the destruction of __read_op_ / __cancel_op_.
Same shape as the fsevents/rdc stop paths.
EOF
)"
```

---

## Task 4: README + final polish

Adds the README and tightens demo cleanup.

**Files:**
- Create: `examples/inotify_README.md`

### Steps

- [ ] **Step 4.1 — Write README** (`examples/inotify_README.md`)

```markdown
# inotify wrapper for stdexec

Linux-only example: wraps `inotify` as an `exec::sequence_sender_t`,
with IO completions driven by `exec::io_uring_context`. Companion to
`examples/fsevents_wrapper.hpp` (macOS) and
`examples/rdc_pool_wrapper.hpp` (Windows).

## Files

| File | Role |
|---|---|
| `inotify_wrapper.hpp` | Header-only `inx::inotify_context` |
| `inotify.cpp`         | Demo: watch + dynamic `add_watch` on `IN_CREATE \| IN_ISDIR` |
| `inotify_README.md`   | This file |

Build (only configured under Linux):

```sh
cmake --build build --target example.inotify
./build/examples/example.inotify
```

The demo writes to / watches a temp directory (`inx_demo`).

## API

```cpp
exec::io_uring_context ring;
std::thread driver{[&]{ ring.run_until_stopped(); }};

inx::inotify_context ctx{{"/path/to/dir"}};

stdexec::sync_wait(
    inx::on_ring(ring.get_scheduler(), ctx.watch())
  | exec::transform_each(stdexec::then([&](inx::fs_batch b) {
        if (b.overflow) { /* rescan */ }
        for (auto& e : b.events) {
          auto root = ctx.path_for(e.wd);
          // e.wd / e.mask / e.cookie / e.name
        }
      }))
  | exec::ignore_all_values());

ring.request_stop();
driver.join();
```

`inotify_context` is multi-path and supports thread-safe dynamic
`add_watch(path, mask?)`, `remove_watch(wd)`, `path_for(wd)` —
including while a watch is active. inotify is **not recursive**:
to follow subdirectories, the caller observes `IN_CREATE | IN_ISDIR`
and calls `add_watch` for the new path (see "Recursive watching"
below).

`fs_batch` carries `events` (span of `fs_event{wd, mask, cookie,
name}`) and an `overflow` flag. `name` is the basename relative to the
watched root (`path_for(wd)` resolves the root). `mask` is the raw
inotify bitmask (`IN_CREATE`, `IN_ISDIR`, ...).

## Platform comparison

| | macOS | Windows | **Linux** |
|---|---|---|---|
| Wrapper namespace | `fsx` | `rdcx::pool` | **`inx`** |
| Reactor scheduler | `exec::libdispatch_queue` | `exec::windows_thread_pool` | **`exec::io_uring_context`** |
| Env adapter | `fsx::on_queue` | `rdcx::pool::on_pool` | **`inx::on_ring`** |
| Source primitive | `FSEventStreamCreate` | `ReadDirectoryChangesW` | **`inotify_init1` + `IORING_OP_READ`** |
| Recursive | kernel-side | kernel-side | **caller-side** |
| Resume id | `last_completed_id()` | none | none |

All three share: `exec::sequence_sender_t`, `fs_batch` per IO
completion, single-active subscription per context (CAS-guarded),
continuation-style backpressure, env-injected scheduler enforced at
compile time, `IORING_OP_ASYNC_CANCEL` / `CancelIoEx` /
`FSEventStreamStop` cancellation routed through a stop callback.

## Pool selection / scheduler

The wrapper does not own a thread pool. The pool is selected at the
pipeline level via `inx::on_ring`:

```cpp
exec::io_uring_context ring;
sync_wait(inx::on_ring(ring.get_scheduler(), ctx.watch()) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require
`exec::io_uring_scheduler` in the receiver's env. Composing with any
other scheduler is a compile error.

`inx::on_ring` is an instance of the shared
`exec::__on_scheduler_t` adapter (also used by `fsx::on_queue` and
`rdcx::pool::on_pool`). See
[`sequence_sender_on_scheduler.md`](sequence_sender_on_scheduler.md)
for why this exists rather than `stdexec::starts_on`.

## What happens under the hood

```
inotify_init1 + initial inotify_add_watch (ctor)
        │
        ▼
submit IORING_OP_READ(inotify_fd, buf) ◄────────────────┐
        │                                               │
        ▼                                               │
CQE on io_uring reactor thread                          │
  · res < 0:  -ECANCELED → set_stopped; other → set_error
  · res ≥ 0:  parse buffer                              │
        │                                               │
        ▼                                               │
parse → vector<fs_event>                                │
  · IN_Q_OVERFLOW (wd=-1) → batch.overflow=true, drop synthetic event
  · IN_IGNORED → erase wd from wd→path map (under mutex)│
        │                                               │
        ▼                                               │
set_next(rcvr, just(fs_batch{...}))                     │
        │                                               │
        ▼                                               │
downstream pipeline                                     │
        │                                               │
        ▼                                               │
next_receiver::set_value ───────────────────────────────┘
        │
        ▼ set_stopped / set_error
__teardown → set_stopped/set_error(rcvr)
```

`__op` owns:
- a `std::vector<std::byte>` buffer (size = `watch_options::buffer_size`)
- a `std::optional<__io_task_facade<__read_task>>` for the in-flight READ
- a `std::optional<__io_task_facade<__cancel_task>>` for the cancel SQE
- a `std::unique_ptr<__next_op_t>` for the in-flight downstream pipe

## Backpressure

Continuation-style: the next `IORING_OP_READ` is posted from
`next_receiver::set_value`. While downstream is processing, no SQE is
in flight, so the kernel's per-fd inotify queue accumulates events.

The kernel's queue size is bounded by
`/proc/sys/fs/inotify/max_queued_events` (typical default: 16384).
When it overflows, the kernel emits a synthetic event with `wd=-1`
and `mask=IN_Q_OVERFLOW`. The wrapper surfaces this as
`fs_batch::overflow = true` (the synthetic event itself is filtered
out of `events`) — the inotify analogue of FSEvents'
`MustScanSubDirs` and RDC's `bytes_returned == 0`. After overflow,
real events resume, but events queued *before* the overflow are gone
forever; the caller must rescan.

Increase `watch_options::buffer_size` to give one READ room to absorb
larger event bursts, but the kernel queue is the actual bottleneck —
tune `/proc/sys/fs/inotify/max_queued_events` for high-rate workloads.

## Cancellation

```
upstream stop_token ──► __on_stop_fn
                              │
                              ▼
                    __stop_requested_ = true
                              │
                              ▼
                    submit IORING_OP_ASYNC_CANCEL
                    (target = in-flight READ's user_data)
                              │
                              ▼
                    READ CQE arrives with res = -ECANCELED
                              │
                              ▼
                    __on_read_complete → __finish_stopped
                              │
                              ▼
                    __teardown → set_stopped(rcvr)
```

If a downstream operation does not propagate `stop_token` while a
batch is in flight, the pipeline stalls (no new SQE → no CQE → no
continuation). Same hazard as the fsevents/rdc wrappers; `then`,
`transform_each`, `bulk` all propagate stop, so this is rare in
practice.

## Recursive watching

inotify watches one directory layer at a time. Build recursive
watching on top:

```cpp
// On startup, walk the tree once:
for (auto& p : fs::recursive_directory_iterator{root}) {
  if (p.is_directory()) ctx.add_watch(p.path().string());
}

// On every fs_batch event:
for (auto& e : b.events) {
  if ((e.mask & IN_CREATE) && (e.mask & IN_ISDIR)) {
    auto root = ctx.path_for(e.wd);
    if (root) ctx.add_watch(*root + "/" + e.name);
  }
}
```

Caveat: there is a race between the `recursive_directory_iterator`
walk and the first `add_watch` — events that occur in subdirectories
during the walk can be missed. For correctness, either: take a
snapshot under a lock the producer respects (rarely possible), or
plan to rescan after enabling all watches. inotify cannot make this
race go away; that is why FSEvents/RDC do recursion in kernel.

## inotify quirks worth knowing

| Quirk | Detail |
|---|---|
| **Not recursive** | `inotify_add_watch` is per-directory. Subtree expansion is the caller's job. |
| **`name` is NUL-padded** | `inotify_event::len` is the *padded* length of the name field. Use `strnlen(ev->name, ev->len)` to get the real string length. The wrapper does this. |
| **`name` empty for self-events** | When the event is on the watched directory itself (e.g. `IN_DELETE_SELF`, `IN_MOVE_SELF`), `len == 0` and there is no name. |
| **`IN_IGNORED` auto-cleans the wd** | When a watch is removed (kernel side, e.g. unlinked target, FS unmounted, or our own `inotify_rm_watch`), the kernel emits `IN_IGNORED` then never uses that wd again. The wrapper erases the wd from the wd→path map under the mutex when this fires; `path_for(wd)` returns `nullopt` afterwards. |
| **`IN_Q_OVERFLOW`** | Synthetic event with `wd == -1`; means events queued before this point were lost. The wrapper surfaces it as `fs_batch::overflow = true` and filters the synthetic event out of `events`. Caller must rescan. |
| **Repeated `add_watch` on same path** | inotify replaces the mask with the new value (without `IN_MASK_ADD`). Pass the full mask you want; do not OR-with-existing mentally. |
| **Inode-level, not path-level** | A watch follows the inode, not the path. If you watch `/tmp/foo` and someone renames `/tmp/foo` to `/tmp/bar`, you continue watching `/tmp/bar`. Hard links to the watched inode generate events too. |
| **Buffer alignment** | `read(2)` returns a stream of `struct inotify_event` records, each followed by a NUL-padded name field. The wrapper parses sequentially using `sizeof(struct inotify_event) + ev->len`. |
| **`O_NONBLOCK`** | The wrapper sets `IN_NONBLOCK` on the inotify fd. With `IORING_OP_READ`, this prevents io_uring from blocking the kernel-side worker if the buffer is unexpectedly empty when the SQE is processed. |

## Things deliberately NOT done

- **Recursive / subtree watching**: surfaced via dynamic `add_watch` — caller does the walking. See "Recursive watching" above.
- **fanotify backend**: requires `CAP_SYS_ADMIN`, has asymmetric kernel-version feature surface (`FAN_REPORT_FID` 5.1+, `FAN_REPORT_DIR_FID` 5.9+, etc.), and most example use cases don't need mount-level monitoring. Build a separate wrapper if you need it.
- **`IORING_OP_READ_MULTISHOT`**: would fold many CQEs into one SQE on Linux 6.0+, but requires a provided buffers ring and breaks the strict continuation-style backpressure alignment. Optimization for later; not appropriate for the example's first cut.
- **Multi-subscriber fan-out**: `__active_` is single-slot, CAS-guarded. Concurrent watches on the same context fail with `set_error`. Build a layer on top if you need fan-out.
- **`IN_MASK_ADD` semantics**: `add_watch` always passes whatever mask the caller supplied. Repeated calls with different masks replace the kernel's mask (default behaviour); we do not silently OR.
- **Resume / replay**: inotify has no event-id or resume cursor. After process restart you must rescan; nothing in the wrapper is going to save you from that.
- **Unit tests**: matching the precedent of `fsevents_wrapper.hpp` and `rdc_wrapper.hpp` — example wrappers ship with a demo, not a test suite.
```

- [ ] **Step 4.2 — Build**

Run: `cmake --build build --target example.inotify`

Expected: still builds clean.

- [ ] **Step 4.3 — Run final demo end-to-end**

Run: `./build/examples/example.inotify`

Expected: same clean run as Step 3.6. README does not affect runtime.

- [ ] **Step 4.4 — Commit**

```bash
git add examples/inotify_README.md
git commit -m "$(cat <<'EOF'
examples: README for the inotify wrapper

Documents the platform comparison (fsx / rdcx::pool / inx),
io_uring_context-driven backpressure model, IN_Q_OVERFLOW handling,
ASYNC_CANCEL cancellation, recursive-watching pattern, and the
inotify quirks (name NUL-padding, IN_IGNORED auto-clean,
inode-vs-path semantics) the caller needs to know.
EOF
)"
```

---

## Self-Review

**1. Spec coverage**

| Spec section | Implementing task |
|---|---|
| `inotify_context` ctor / dtor / fd ownership | Task 1 (Step 1.1) |
| `add_watch` / `remove_watch` / `path_for` (thread-safe) | Task 1 (Step 1.1) |
| `__op` lifecycle, single-active CAS | Task 2 (Step 2.1, 2.2) |
| `IORING_OP_READ` submit + CQE parse | Task 2 (Step 2.1) |
| Continuation-style backpressure | Task 2 (Step 2.1, `__on_next_value`) |
| `IN_Q_OVERFLOW` → `fs_batch::overflow` | Task 2 (Step 2.1, `__on_read_complete`) |
| `IN_IGNORED` auto-clean wd→path map | Task 2 (Step 2.1, `__parse_into_staging`) |
| Compile-time scheduler-type constraint via `__env_has_scheduler` | Task 2 (Step 2.2) |
| Stop callback + `IORING_OP_ASYNC_CANCEL` | Task 3 (Step 3.1, 3.2, 3.3) |
| Cleanup ordering (drop stop_cb first) | Task 3 (Step 3.4) |
| Demo with dynamic `add_watch` on `IN_CREATE \| IN_ISDIR` | Task 2 (Step 2.3) |
| CMakeLists wiring (Linux-only) | Task 1 (Step 1.3) |
| README with platform comparison | Task 4 (Step 4.1) |
| Recursive watching guidance | Task 4 (Step 4.1, "Recursive watching" section) |
| `inx::on_ring` shared adapter | Task 1 (Step 1.1, top-level `inline constexpr`) |

No spec section is left without a task.

**2. Placeholder scan**

No "TBD" / "TODO" / "implement later" / "add appropriate ..." in the plan. Every code block is the actual code to paste; every command is exact; every file path is exact.

**3. Type consistency**

- `inx::fs_event` has `wd: int`, `mask: uint32_t`, `cookie: uint32_t`, `name: std::string` — same in Task 1 declarations, Task 2 demo lambda, Task 4 README example. ✓
- `inx::fs_batch` has `events: span<const fs_event>`, `overflow: bool` — same throughout. ✓
- `inotify_context::add_watch(string_view, optional<uint32_t>) -> int` — same signature in declaration (Step 1.1), demo call (Step 2.3), and README (Step 4.1). ✓
- `inx::on_ring` — defined as `exec::__on_scheduler_t{}` instance in Step 1.1, used in Step 2.3 demo and Step 4.1 README. ✓
- `__op_base::__on_read_complete` — virtual added in Task 2 Step 2.1, overridden in `__op` same step, called from `__read_task::complete` same step. ✓
- `__io_task_facade<...>::start()` is the entry point used in Step 2.1 (`__post_read`) and Step 3.2 (cancel). Both use `std::in_place` ctor. ✓
- `experimental::execution::__io_uring::__context` is the underlying type the facade calls back through. We grab it via `scheduler.__context_` (public member). Used consistently. ✓

No type / signature drift across tasks.

**4. No dangling references**

All types referenced (`fs_event`, `fs_batch`, `watch_options`, `inotify_context`, `__op`, `__op_base`, `__next_receiver`, `__watch_sender`, `__read_task`, `__cancel_task`, `__read_op_t`, `__cancel_op_t`, `__on_stop_fn`, `__stop_callback_t`) are defined in the plan in the task that introduces them.

---

## Open questions / known limitations

- Kernel version: `IORING_OP_READ` is Linux 5.6+. `IORING_OP_ASYNC_CANCEL` is Linux 5.5+. The io_uring_context.hpp header already gates `IORING_OP_READ` behind `STDEXEC_HAS_IORING_OP_READ` and async cancel behind `STDEXEC_HAS_IO_URING_ASYNC_CANCELLATION`. The demo's CMake target is gated by `if (LINUX)` only — no kernel-version check at build time. If a build target lands on an unsupported kernel, the failure mode is at runtime, not at compile time. Mention in README only if reviewers ask; the existing `example.io_uring` has the same baseline.
- The `__scheduler::__context_` member is `public` but lives in the `__io_uring` detail namespace. Reaching through it is a known pattern for these example wrappers (rdc_pool's `windows_thread_pool::scheduler::native_handle()` is the equivalent escape hatch). If stdexec adds a public accessor in future, swap to it.

These are not blockers for this plan; they are the same constraints the existing `example.io_uring` already lives with.
