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

// Windows-only: wraps Cfgmgr32 device-interface notifications for
// GUID_DEVINTERFACE_VOLUME as a stdexec sequence sender driven by
// exec::windows_thread_pool. Mirrors the DA wrapper (examples/da_wrapper.hpp)
// in shape; the only material divergence is that CM callbacks run on a
// Cfgmgr32-internal thread we do not own, so we cannot block them — events
// are handed off via an MPSC queue + a drainer pool work item that performs
// the DA-style binary_semaphore handshake on the user's pool.

#ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#  define NOMINMAX
#endif
#include <windows.h>
// windows.h must come first
#include <initguid.h>
#include <cfgmgr32.h>
#include <ioevent.h>          // GUID_DEVINTERFACE_VOLUME

#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/windows/windows_thread_pool.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cctype>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

namespace velx
{
  enum class volume_event_kind
  {
    interface_arrival,
    interface_removal
  };

  struct volume_event
  {
    volume_event_kind kind;
    std::string       device_path;  // UTF-8 lowercased "\\?\volume{guid}"
  };

  struct watch_options
  {};  // empty in v1, reserved for v2

  class volume_context;

  namespace __detail
  {
    struct __op_base;
    template <class _Rcvr>
    struct __op;
    template <class _Rcvr>
    struct __next_receiver;
    struct __watch_sender;

    enum __finish_kind : int
    {
      __finish_none    = 0,
      __finish_stopped = 1,
      __finish_error   = 2,
    };
  }  // namespace __detail

  class volume_context
  {
   public:
    volume_context()  = default;
    ~volume_context() = default;

    volume_context(volume_context const &)                    = delete;
    auto operator=(volume_context const &) -> volume_context& = delete;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
    template <class _Rcvr>
    friend struct __detail::__next_receiver;
    friend struct __detail::__watch_sender;

    std::atomic<__detail::__op_base*> __active_{nullptr};
  };

  // env-injection adapter — mirrors fsx::on_queue / dax::on_queue /
  // inx::on_ring / rdcx::pool::on_pool. Same `exec::__on_scheduler_t`
  // instance type promoted in include/exec/on_scheduler.hpp.
  inline constexpr exec::__on_scheduler_t on_pool{};

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base() = default;
    };

    struct __watch_sender
    {
      using sender_concept = exec::sequence_sender_tag;
      using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(),
                                       stdexec::set_stopped_t(),
                                       stdexec::set_error_t(std::exception_ptr)>;

      using __item_sender_t = decltype(stdexec::just(std::declval<volume_event>()));
      using item_types      = exec::item_types<__item_sender_t>;

      volume_context* __ctx_;
      watch_options   __opts_;
    };
  }  // namespace __detail

  inline auto volume_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace velx
