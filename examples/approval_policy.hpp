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

// Cross-platform approval / query-removal policy for OS event-source
// wrappers (DiskArbitration on macOS, CM_Register_Notification on
// Windows). The OS contract for these callbacks is "synchronously
// return a verdict so the OS can decide whether to proceed with the
// operation"; this header models that contract as a tagged union so
// users pick *explicitly* between (a) running their predicate inline
// on the OS callback thread and (b) running it on a worker thread
// with a wrapper-enforced timeout.
//
// Why a tagged union, not a single struct with a magic-zero timeout:
//   - sync mode has no meaningful stop_token (predicate runs to
//     completion); bounded mode requires one
//   - on_timeout_allow is dead in sync mode
//   - making the modes distinct types lets the compiler enforce that
//     each field appears only where it's meaningful, and lets a reader
//     tell at a glance which mode a callsite picked
//
// `Info` is the platform-specific per-event payload (e.g.
// `dax::disk_info`, `velx::device_info`). The wrapper passes a
// lazily-constructed Info to the predicate; for monostate the factory
// is never invoked, since building Info from a CF/HANDLE is non-free.

#include "stdexec/stop_token.hpp"

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <utility>
#include <variant>

namespace approval
{
  // Predicate runs inline on whatever thread the OS callback delivered
  // on (DA's session queue / CM's thread-pool thread). Must return
  // promptly — the OS will keep the unmount/remove operation pending
  // until the predicate returns.
  template <class Info>
  struct sync
  {
    std::function<bool(Info)> predicate;
  };

  // Predicate runs on a worker thread; the wrapper waits up to
  // `timeout` for it to finish. If the predicate completes in time,
  // its return value is the verdict. Otherwise the wrapper requests
  // stop on the predicate's stop_token, detaches the worker, and
  // returns `on_timeout_allow`. Use this when the verdict logic might
  // legitimately block (service shutdown, file flushing) and you'd
  // rather give the OS a default than wedge its callback queue.
  //
  // The token is a `stdexec::inplace_stop_token` because libc++ on
  // Apple toolchains does not yet ship `std::stop_token` (Xcode 16.x
  // as of 2026-05). The semantic surface is the same:
  // `tok.stop_requested()` returns true once the wrapper times out.
  template <class Info>
  struct bounded
  {
    std::function<bool(Info, stdexec::inplace_stop_token)> predicate;
    std::chrono::milliseconds                              timeout{};
    bool                                                   on_timeout_allow{true};
  };

  // monostate = "no policy registered" — the wrapper should not even
  // call DARegister*ApprovalCallback / CM_Register_Notification's
  // approval path; the verdict is whatever the wrapper picks as its
  // default-allow.
  template <class Info>
  using policy = std::variant<std::monostate, sync<Info>, bounded<Info>>;

  // Resolve a policy to a verdict. `make_info` is called at most once,
  // and only when the policy actually needs the info (i.e. not for
  // monostate). `default_when_empty` is the verdict returned when the
  // policy is monostate — wrappers typically pass `true` ("allow if
  // unconfigured") to match the prior no-callback behavior.
  template <class Info, class InfoFactory>
  bool resolve_verdict(policy<Info> const &__p,
                       InfoFactory       &&__make_info,
                       bool                __default_when_empty = true)
  {
    return std::visit(
      [&](auto const &__pp) -> bool
      {
        using __T = std::decay_t<decltype(__pp)>;
        if constexpr (std::is_same_v<__T, std::monostate>)
        {
          return __default_when_empty;
        }
        else if constexpr (std::is_same_v<__T, sync<Info>>)
        {
          return __pp.predicate(__make_info());
        }
        else  // bounded<Info>
        {
          // Copy what the worker thread will need before launching, so
          // a `t.detach()` after timeout cannot leave the worker
          // holding references into our stack frame or into the
          // user-owned policy variant.
          auto       __pred             = __pp.predicate;
          bool const __on_timeout_allow = __pp.on_timeout_allow;
          auto const __timeout          = __pp.timeout;

          // The source must outlive the worker — `inplace_stop_token`
          // is a non-owning pointer back to the source. After
          // `t.detach()` the worker can outlive this stack frame, so
          // share ownership via shared_ptr captured in the worker's
          // closure.
          auto __src = std::make_shared<stdexec::inplace_stop_source>();
          auto __pr  = std::make_shared<std::promise<bool>>();
          auto __fut = __pr->get_future();

          std::thread __worker(
            [__pred = std::move(__pred),
             __info = __make_info(),
             __src,
             __on_timeout_allow,
             __pr]() mutable
            {
              try
              {
                __pr->set_value(__pred(std::move(__info), __src->get_token()));
              }
              catch (...)
              {
                __pr->set_value(__on_timeout_allow);
              }
            });

          if (__fut.wait_for(__timeout) == std::future_status::ready)
          {
            __worker.join();
            return __fut.get();
          }
          // Past timeout: signal the predicate to bail (cooperatively;
          // a predicate that ignores the token will keep running on
          // the detached thread until it returns naturally), then
          // commit to the default verdict.
          __src->request_stop();
          __worker.detach();
          return __on_timeout_allow;
        }
      },
      __p);
  }
}  // namespace approval
