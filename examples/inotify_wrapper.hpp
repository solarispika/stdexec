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
