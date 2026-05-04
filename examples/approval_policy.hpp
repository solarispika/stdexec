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
  // The token is a `stdexec::inplace_stop_token` rather than
  // `std::stop_token` for portability — older Apple toolchains
  // (Xcode 16.x) ship `<stop_token>` as a stub. Xcode 26 onwards has
  // `std::stop_token`, but stdexec's primitive is the canonical one
  // used elsewhere in the codebase. Semantic surface is the same:
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
  bool resolve_verdict(policy<Info> const &p,
                       InfoFactory       &&make_info,
                       bool                default_when_empty = true)
  {
    return std::visit(
      [&](auto const &pp) -> bool
      {
        using T = std::decay_t<decltype(pp)>;
        if constexpr (std::is_same_v<T, std::monostate>)
        {
          return default_when_empty;
        }
        else if constexpr (std::is_same_v<T, sync<Info>>)
        {
          return pp.predicate(make_info());
        }
        else  // bounded<Info>
        {
          // Copy what the worker thread will need before launching, so
          // a `t.detach()` after timeout cannot leave the worker
          // holding references into our stack frame or into the
          // user-owned policy variant.
          auto       pred             = pp.predicate;
          bool const on_timeout_allow = pp.on_timeout_allow;
          auto const timeout          = pp.timeout;

          // The source must outlive the worker — `inplace_stop_token`
          // is a non-owning pointer back to the source. After
          // `t.detach()` the worker can outlive this stack frame, so
          // share ownership via shared_ptr captured in the worker's
          // closure.
          auto src = std::make_shared<stdexec::inplace_stop_source>();
          auto pr  = std::make_shared<std::promise<bool>>();
          auto fut = pr->get_future();

          std::thread worker(
            [pred = std::move(pred),
             info = make_info(),
             src,
             on_timeout_allow,
             pr]() mutable
            {
              try
              {
                pr->set_value(pred(std::move(info), src->get_token()));
              }
              catch (...)
              {
                pr->set_value(on_timeout_allow);
              }
            });

          if (fut.wait_for(timeout) == std::future_status::ready)
          {
            worker.join();
            return fut.get();
          }
          // Past timeout: signal the predicate to bail (cooperatively;
          // a predicate that ignores the token will keep running on
          // the detached thread until it returns naturally), then
          // commit to the default verdict.
          src->request_stop();
          worker.detach();
          return on_timeout_allow;
        }
      },
      p);
  }
}  // namespace approval
