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

// Shared sequence-sender-aware env-injection adapter.
//
// Used by examples/fsevents_wrapper.hpp (`fsx::on_queue`) and
// examples/rdc_pool_wrapper.hpp (`rdcx::pool::on_pool`) to expose a
// scheduler via `get_scheduler` in the receiver env without losing
// sequence-sender semantics on the wrapped sender. See
// examples/sequence_sender_on_scheduler.md for the full explanation,
// and docs/plans/2026-04-29-stdexec-write_env-sequence-sender-issue.md
// for the upstream issue this works around.
//
// Each wrapper defines its own thin CPO instance:
//
//   namespace fsx {
//     inline constexpr examples_detail::__on_scheduler_t on_queue{};
//   }
//   namespace rdcx::pool {
//     inline constexpr examples_detail::__on_scheduler_t on_pool{};
//   }

#include "exec/sequence_senders.hpp"
#include "stdexec/execution.hpp"

#include <utility>

namespace examples_detail
{
  template <class _Sched>
  struct __sched_prop
  {
    _Sched __sched_;

    // Returns by value. Returning by reference (as `stdexec::prop{...}` does)
    // would fail any `same_as<..., _Sched>` constraint downstream because the
    // query result becomes `_Sched const&`.
    [[nodiscard]]
    constexpr auto query(stdexec::get_scheduler_t) const noexcept -> _Sched
    {
      return __sched_;
    }
  };

  template <class _Rcvr, class _Sched>
  struct __on_scheduler_rcvr
  {
    using receiver_concept = stdexec::receiver_tag;

    _Rcvr  __rcvr_;
    _Sched __sched_;

    [[nodiscard]]
    auto get_env() const noexcept
    {
      return stdexec::env{__sched_prop<_Sched>{__sched_}, stdexec::get_env(__rcvr_)};
    }

    template <class _Item>
    auto set_next(_Item&& __item) -> exec::next_sender_of_t<_Rcvr, _Item>
    {
      return exec::set_next(__rcvr_, static_cast<_Item&&>(__item));
    }

    void set_value() noexcept
    {
      stdexec::set_value(static_cast<_Rcvr&&>(__rcvr_));
    }

    void set_stopped() noexcept
    {
      stdexec::set_stopped(static_cast<_Rcvr&&>(__rcvr_));
    }

    template <class _E>
    void set_error(_E&& __e) noexcept
    {
      stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), static_cast<_E&&>(__e));
    }
  };

  template <class _Snd, class _Sched>
  struct __on_scheduler_sender
  {
    using sender_concept        = exec::sequence_sender_tag;
    using item_types            = exec::__item_types_of_t<_Snd>;
    using completion_signatures = stdexec::__completion_signatures_of_t<_Snd>;

    _Snd   __snd_;
    _Sched __sched_;

    template <stdexec::receiver _Rcvr>
    auto subscribe(_Rcvr __rcvr) &&
      -> exec::subscribe_result_t<_Snd, __on_scheduler_rcvr<_Rcvr, _Sched>>
    {
      return exec::subscribe(static_cast<_Snd&&>(__snd_),
                             __on_scheduler_rcvr<_Rcvr, _Sched>{std::move(__rcvr),
                                                                std::move(__sched_)});
    }
  };

  struct __on_scheduler_t
  {
    template <stdexec::scheduler _Sched, class _Snd>
    auto operator()(_Sched __sched, _Snd __snd) const -> __on_scheduler_sender<_Snd, _Sched>
    {
      return {std::move(__snd), std::move(__sched)};
    }
  };

  // For wrappers whose `subscribe` requires a specific scheduler type in
  // the receiver's env (typically the one this adapter injected).
  template <class _Env, class _Sched>
  concept __env_has_scheduler =
    stdexec::__callable<stdexec::get_scheduler_t, _Env const&>
    && std::same_as<stdexec::__call_result_t<stdexec::get_scheduler_t, _Env const&>, _Sched>;
}  // namespace examples_detail
