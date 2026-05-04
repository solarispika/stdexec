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
    int           wd;  // -1 means IN_Q_OVERFLOW (filtered from events span)
    std::uint32_t mask;
    std::uint32_t cookie;
    std::string   name;
  };

  struct fs_batch
  {
    std::span<fs_event const> events;
    bool                      overflow;
  };

  struct watch_options
  {
    std::uint32_t mask = IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MODIFY | IN_ATTRIB
                       | IN_MOVED_FROM | IN_MOVED_TO | IN_MOVE_SELF | IN_CLOSE_WRITE;
    std::size_t buffer_size = 64 * 1024;
  };

  class inotify_context;

  namespace detail
  {
    struct op_base;
    template <class Rcvr>
    struct op;
    template <class Rcvr>
    struct next_receiver;
    struct watch_sender;
  }  // namespace detail

  class inotify_context
  {
   public:
    explicit inotify_context(std::vector<std::string> initial_paths,
                             std::uint32_t            default_mask = watch_options{}.mask);
    ~inotify_context();

    inotify_context(inotify_context const &)                    = delete;
    auto operator=(inotify_context const &) -> inotify_context& = delete;

    auto
    add_watch(std::string_view path, std::optional<std::uint32_t> mask = std::nullopt) -> int;

    auto remove_watch(int wd) noexcept -> bool;

    auto path_for(int wd) const -> std::optional<std::string>;

    auto watch(watch_options opts = {}) -> detail::watch_sender;

   private:
    template <class Rcvr>
    friend struct detail::op;
    template <class Rcvr>
    friend struct detail::next_receiver;
    friend struct detail::watch_sender;

    int                                  fd_{-1};
    std::uint32_t                        default_mask_{};
    mutable std::mutex                   map_mu_;
    std::unordered_map<int, std::string> wd_to_path_;
    std::atomic<detail::op_base*>    active_{nullptr};
  };

  namespace detail
  {
    struct op_base
    {
      virtual ~op_base()                                               = default;
      virtual void on_read_complete(::io_uring_cqe const &) noexcept   = 0;
      virtual void on_cancel_complete(::io_uring_cqe const &) noexcept = 0;
      virtual void on_finalize_complete() noexcept                     = 0;
    };

    template <class Rcvr>
    struct op;

    template <class Rcvr>
    struct next_receiver
    {
      using receiver_concept = stdexec::receiver_tag;

      op<Rcvr>* self_;

      template <class... Args>
      void set_value(Args&&...) noexcept;

      void set_stopped() noexcept;

      template <class E>
      void set_error(E&&) noexcept;

      [[nodiscard]]
      auto get_env() const noexcept -> stdexec::env_of_t<Rcvr>;
    };

    // Thin io_task base that defers to its outer op via a back-pointer.
    struct read_task
    {
      op_base*                                      outer_;
      experimental::execution::__io_uring::__context* ctx_;
      int                                             fd_;
      void*                                           buf_;
      std::size_t                                     buf_len_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& sqe) noexcept
      {
        std::memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = IORING_OP_READ;
        sqe.fd     = fd_;
        sqe.addr   = reinterpret_cast<std::uint64_t>(buf_);
        sqe.len    = static_cast<std::uint32_t>(buf_len_);
        sqe.off    = 0;
        // user_data is set by io_uring_context::submit().
      }

      void complete(::io_uring_cqe const & cqe) noexcept
      {
        outer_->on_read_complete(cqe);
      }
    };

    using read_op_t = experimental::execution::__io_uring::__io_task_facade<read_task>;

    // Single-shot SQE that cancels another in-flight task by user_data.
    // The target task's user_data is its task* (set by io_uring_context).
    // Like read_task, this holds a back-pointer to the outer op so its
    // complete() callback can decrement the pending-CQE counter — without
    // it, the read CQE could drive teardown and free the cancel facade
    // memory before the kernel posted the cancel CQE (UAF in the reactor).
    struct cancel_task
    {
      op_base*                                      outer_;
      experimental::execution::__io_uring::__context* ctx_;
      void*                                           target_user_data_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& sqe) noexcept
      {
        std::memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = IORING_OP_ASYNC_CANCEL;
        sqe.addr   = reinterpret_cast<std::uint64_t>(target_user_data_);
      }

      void complete(::io_uring_cqe const & cqe) noexcept
      {
        // Cancellation result is discarded — the target task's own complete()
        // path is what drives the finish kind. -ENOENT (already done) and
        // 0 (cancelled) are both valid outcomes. We only need to inform
        // the outer op that the cancel CQE has now landed.
        outer_->on_cancel_complete(cqe);
      }
    };

    using cancel_op_t = experimental::execution::__io_uring::__io_task_facade<cancel_task>;

    // Deferred-finalize trampoline: submits an IORING_OP_NOP whose CQE arrives
    // back on the reactor in a fresh frame. This is the unique safe site for
    // calling finalize_and_complete — no matter whether the original
    // "want-to-finish" trigger came from a CQE handler or from an async
    // downstream receiver callback, the NOP CQE delivers in a stack frame
    // that has fully unwound from any nested set_next chain. Same shape as
    // rdc_pool_wrapper.hpp's schedule_cleanup + SubmitThreadpoolWork.
    struct finalize_task
    {
      op_base*                                      outer_;
      experimental::execution::__io_uring::__context* ctx_;

      auto context() noexcept -> experimental::execution::__io_uring::__context&
      {
        return *ctx_;
      }

      static constexpr auto ready() noexcept -> bool
      {
        return false;
      }

      void submit(::io_uring_sqe& sqe) noexcept
      {
        std::memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = IORING_OP_NOP;
      }

      void complete(::io_uring_cqe const &) noexcept
      {
        outer_->on_finalize_complete();
      }
    };

    using finalize_op_t = experimental::execution::__io_uring::__io_task_facade<finalize_task>;

    template <class Rcvr>
    struct op : op_base
    {
      using item_sender_t   = decltype(stdexec::just(std::declval<fs_batch>()));
      using next_sender_t   = exec::next_sender_of_t<Rcvr, item_sender_t>;
      using next_receiver_t = next_receiver<Rcvr>;
      using next_op_t       = stdexec::connect_result_t<next_sender_t, next_receiver_t>;

      enum class finish_kind
      {
        none,
        stopped,
        error
      };

      struct on_stop_fn
      {
        op* self_;
        void  operator()() noexcept
        {
          self_->stop_requested_.store(true, std::memory_order_release);

          // Defensive: stop_callback semantics fire at most once, but if a
          // future caller re-arms a stop source we'd double-emplace
          // cancel_op_. Guard against that.
          if (self_->cancel_op_.has_value())
          {
            return;
          }

          // Read the in-flight READ's user_data (its task*) atomically.
          // We MUST NOT touch read_op_ directly here — the reactor thread
          // may be in post_read calling read_op_.emplace, which destroys
          // and reconstructs the optional in place; concurrent access is UB.
          // The shadow pointer is published by post_read after emplace and
          // cleared by finalize_and_complete; a stale read is harmless
          // because the kernel returns -ENOENT for a missed target.
          auto* tgt = self_->read_user_data_.load(std::memory_order_acquire);
          if (tgt == nullptr)
          {
            return;
          }
          // Account for the cancel CQE we are about to submit — the read CQE
          // and the cancel CQE will both arrive, in that order, so finalize
          // must not run until both have been observed.
          self_->pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
          self_->cancel_op_.emplace(std::in_place,
                                        cancel_task{static_cast<op_base*>(self_),
                                                      self_->ring_,
                                                      tgt});
          self_->cancel_op_->start();
        }
      };

      using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
      using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;

      inotify_context*                                ctx_;
      watch_options                                   opts_;
      Rcvr                                           rcvr_;
      experimental::execution::__io_uring::__context* ring_;
      // uint64_t (alignment 8) guarantees the >=4-byte alignment that
      // ::inotify_event requires for its int wd / uint32_t fields.
      // std::vector<std::byte>::data() is only aligned to alignof(byte)==1
      // by the standard. Same trick as rdc_pool_wrapper.hpp's vector<DWORD>.
      std::vector<std::uint64_t>       buffer_;
      std::vector<fs_event>            staging_;
      std::optional<read_op_t>       read_op_;
      std::optional<cancel_op_t>     cancel_op_;
      std::optional<finalize_op_t>   finalize_op_;
      std::unique_ptr<next_op_t>     next_op_;
      std::optional<stop_callback_t> stop_cb_;
      std::atomic<bool>                stop_requested_{false};
      // CAS-gated single-shot flag: only the first caller of
      // request_finalize submits the NOP. Subsequent callers no-op so the
      // counter dance and the finish_kind aren't disturbed.
      std::atomic<bool> finalize_scheduled_{false};
      // Shadow of the in-flight READ facade's task*. Published (release) by
      // post_read after emplace, read (acquire) by on_stop_fn off-thread.
      std::atomic<experimental::execution::__io_uring::__task*> read_user_data_{nullptr};
      // Counts CQEs we expect: each posted READ + each posted CANCEL adds 1,
      // each delivered CQE subtracts 1. Finalize fires only when this reaches
      // 0 with a finish_kind set. Mirrors the n_ops_ pattern used by
      // __stoppable_task_facade::__stop_operation in io_uring_context.hpp.
      std::atomic<int> pending_cqes_{0};
      // finish_kind_ is written by the unique winner of the
      // finalize_scheduled_ CAS in request_finalize, and read by
      // finalize_and_complete (running in the NOP CQE's reactor frame).
      // The CAS publishes the write; the kernel's CQE delivery
      // happens-before the read. Plain (non-atomic) is therefore safe.
      finish_kind      finish_kind_{finish_kind::none};
      std::exception_ptr error_;

      explicit op(inotify_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{o}
        , rcvr_{std::move(r)}
      {
        auto sched = stdexec::get_scheduler(stdexec::get_env(rcvr_));
        ring_      = sched.__context_;
        // Round buffer_size up to the next uint64_t.
        buffer_.resize((opts_.buffer_size + sizeof(std::uint64_t) - 1) / sizeof(std::uint64_t));
      }

      void start() & noexcept
      {
        op_base* expected = nullptr;
        if (!ctx_->active_.compare_exchange_strong(expected, this))
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"inotify_context already "
                                                                        "has an active watch"}));
          return;
        }
        post_read();

        // Register stop callback last: if the token is already in stop state
        // it fires synchronously, which is now safe because the read is up.
        // Same pattern as fsevents_wrapper / rdc_wrapper.
        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});
      }

      void post_read() noexcept
      {
        // Each read posts a fresh io_task_facade. The previous one (if any)
        // has already had its complete() returned. Destructing the previous
        // here is safe because we are not nested in its callback (we are
        // either in start() or in next_receiver::set_value).
        // Note: io_task_facade has two ctor overloads; overload (B) takes
        // Args... matching Base's own ctor, overload (A) takes
        // (task* parent, Args...). read_task is an aggregate with no
        // leading task* field, so overload (B) wins and base_ is
        // copy/move-constructed from our brace-init. If a future change
        // adds a leading task* member to read_task it would silently
        // flip overload selection — keep the leading member as op_base*.

        // Increment BEFORE the emplace+submit so that even if the CQE were
        // delivered synchronously by submit() (it isn't, but) the matching
        // decrement in on_read_complete sees a non-zero pre-state.
        pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
        read_op_.emplace(std::in_place,
                           read_task{static_cast<op_base*>(this),
                                       ring_,
                                       ctx_->fd_,
                                       buffer_.data(),
                                       buffer_.size() * sizeof(std::uint64_t)});
        // Publish the new facade's task* for on_stop_fn to read with
        // release ordering. Must happen AFTER emplace and BEFORE start() so
        // a stop callback that fires concurrently sees the new pointer.
        auto* tgt = static_cast<experimental::execution::__io_uring::__task*>(&*read_op_);
        read_user_data_.store(tgt, std::memory_order_release);
        read_op_->start();
      }

      // Single entry to "we want to finish". CAS-gated so concurrent
      // requests collapse to one NOP submission. Increments pending_cqes_
      // so that the NOP CQE is part of the same counter dance as the read
      // and (optional) cancel CQEs — on_finalize_complete is the unique
      // last-out site that actually drives finalize_and_complete.
      void request_finalize(finish_kind k) noexcept
      {
        bool expected = false;
        if (!finalize_scheduled_.compare_exchange_strong(expected,
                                                           true,
                                                           std::memory_order_acq_rel))
        {
          return;
        }
        finish_kind_ = k;
        pending_cqes_.fetch_add(1, std::memory_order_acq_rel);
        finalize_op_.emplace(std::in_place,
                               finalize_task{static_cast<op_base*>(this), ring_});
        finalize_op_->start();
      }

      // Called on the io_uring reactor thread.
      void on_read_complete(::io_uring_cqe const & cqe) noexcept override
      {
        // The READ CQE is in. Clear the published shadow user_data so any
        // stop callback that fires from this point on does NOT submit a
        // cancel for a target that just landed (it would only race the
        // next post_read's republish anyway, but keep it tidy).
        read_user_data_.store(nullptr, std::memory_order_release);

        if (cqe.res < 0)
        {
          if (cqe.res == -ECANCELED || stop_requested_.load(std::memory_order_acquire))
          {
            request_finalize(finish_kind::stopped);
          }
          else
          {
            error_ = std::make_exception_ptr(
              std::system_error{-cqe.res, std::system_category(), "inotify read"});
            request_finalize(finish_kind::error);
          }
        }
        else
        {
          parse_into_staging(static_cast<std::size_t>(cqe.res));

          bool overflow = false;
          // IN_Q_OVERFLOW arrives as a synthetic event with wd=-1; surface it
          // batch-level and remove it from the events span.
          std::erase_if(staging_,
                        [&](fs_event const & e)
                        {
                          if (e.wd == -1 && (e.mask & IN_Q_OVERFLOW))
                          {
                            overflow = true;
                            return true;
                          }
                          return false;
                        });

          fs_batch batch{staging_, overflow};

          try
          {
            next_op_.reset(
              new next_op_t(stdexec::connect(exec::set_next(rcvr_, stdexec::just(batch)),
                                               next_receiver_t{this})));
            stdexec::start(*next_op_);
          }
          catch (...)
          {
            error_ = std::current_exception();
            request_finalize(finish_kind::error);
          }
        }

        // Tail decrement for THIS read CQE. Finalization is NOT triggered
        // here; the NOP CQE submitted by request_finalize is the unique
        // safe-frame finalizer (see on_finalize_complete). On the steady
        // value path, post_read (called from set_value) has already
        // bumped the counter for the next read, so this dec doesn't strand.
        pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
      }

      // Called on the io_uring reactor thread when the cancel CQE lands.
      // Plain decrement — finalization is owned by on_finalize_complete.
      void on_cancel_complete(::io_uring_cqe const &) noexcept override
      {
        pending_cqes_.fetch_sub(1, std::memory_order_acq_rel);
      }

      // The NOP CQE submitted by request_finalize lands here on the
      // reactor in a fresh frame. This is the ONE site that calls
      // finalize_and_complete — by this point any nested set_next chain
      // has unwound, the read CQE's debt is settled, and (if a cancel was
      // submitted) the cancel CQE has been observed too.
      void on_finalize_complete() noexcept override
      {
        if (pending_cqes_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
          finalize_and_complete();
        }
      }

      void parse_into_staging(std::size_t bytes) noexcept
      {
        staging_.clear();
        auto const * raw = reinterpret_cast<std::byte const *>(buffer_.data());
        std::size_t  off = 0;
        while (off + sizeof(::inotify_event) <= bytes)
        {
          auto const * ev     = reinterpret_cast<::inotify_event const *>(raw + off);
          std::size_t  record = sizeof(::inotify_event) + ev->len;
          if (off + record > bytes)
            break;

          // IN_IGNORED: the kernel has dropped this watch. Clean wd→path map.
          if ((ev->mask & IN_IGNORED) && ev->wd >= 0)
          {
            std::lock_guard lk{ctx_->map_mu_};
            ctx_->wd_to_path_.erase(ev->wd);
          }

          std::string name;
          if (ev->len > 0)
          {
            // name is NUL-padded; strlen gives the real size.
            name.assign(ev->name, ::strnlen(ev->name, ev->len));
          }
          staging_.push_back({ev->wd, ev->mask, ev->cookie, std::move(name)});

          off += record;
        }
      }

      void on_next_value() noexcept
      {
        // Do NOT reset next_op_ here — downstream may complete synchronously
        // inside set_next's start(); destroying the op from inside its own
        // set_value call would tear down the call stack. The next batch's
        // unique_ptr::reset(new ...) will destroy this child after start()
        // unwinds.
        //
        // May run either synchronously from on_read_complete's
        // start(*next_op_) call, OR asynchronously from a downstream that
        // hops threads. Both cases are handled identically: route through
        // request_finalize so the NOP CQE drives termination.
        if (stop_requested_.load(std::memory_order_acquire))
        {
          request_finalize(finish_kind::stopped);
          return;
        }
        post_read();
      }

      // Downstream completed with set_stopped. Route through the NOP-CQE
      // trampoline regardless of caller (sync nested or async), so that
      // finalize_and_complete always runs in a fresh reactor frame.
      void on_next_stopped() noexcept
      {
        request_finalize(finish_kind::stopped);
      }

      // Downstream completed with set_error — same shape as next_stopped.
      void on_next_error(std::exception_ptr ep) noexcept
      {
        error_ = std::move(ep);
        request_finalize(finish_kind::error);
      }

      // The single completion path. Called ONLY from
      // on_finalize_complete, which runs in a fresh reactor CQE frame
      // delivered by the IORING_OP_NOP submitted by request_finalize.
      // By this point: every read/cancel CQE has been observed (counter
      // reached 0 with the NOP itself decrementing last), and any nested
      // set_next chain has unwound — so resetting next_op_, cancel_op_,
      // read_op_ and the in-flight finalize_op_ facade we're inside is
      // safe (the latter mirrors io_task_facade's documented self-destroy
      // pattern). Drop the stop callback first to prevent a late stop
      // request from observing half-torn-down state, then drop the
      // downstream op, then the io_uring facades, then release the active
      // slot. Finally, complete the user's receiver.
      void finalize_and_complete() noexcept
      {
        stop_cb_.reset();
        next_op_.reset();
        cancel_op_.reset();
        read_op_.reset();
        finalize_op_.reset();
        ctx_->active_.store(nullptr, std::memory_order_release);

        if (finish_kind_ == finish_kind::error)
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_), std::move(error_));
        }
        else
        {
          stdexec::set_stopped(static_cast<Rcvr&&>(rcvr_));
        }
      }
    };

    template <class Rcvr>
    template <class... Args>
    void next_receiver<Rcvr>::set_value(Args&&...) noexcept
    {
      self_->on_next_value();
    }

    template <class Rcvr>
    void next_receiver<Rcvr>::set_stopped() noexcept
    {
      self_->on_next_stopped();
    }

    template <class Rcvr>
    template <class E>
    void next_receiver<Rcvr>::set_error(E&& e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<E>, std::exception_ptr>)
      {
        self_->on_next_error(std::forward<E>(e));
      }
      else
      {
        self_->on_next_error(std::make_exception_ptr(std::forward<E>(e)));
      }
    }

    template <class Rcvr>
    auto next_receiver<Rcvr>::get_env() const noexcept -> stdexec::env_of_t<Rcvr>
    {
      return stdexec::get_env(self_->rcvr_);
    }

    struct watch_sender
    {
      using sender_concept = exec::sequence_sender_tag;
      using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(),
                                       stdexec::set_stopped_t(),
                                       stdexec::set_error_t(std::exception_ptr)>;

      using item_sender_t = decltype(stdexec::just(std::declval<fs_batch>()));
      using item_types      = exec::item_types<item_sender_t>;

      inotify_context* ctx_;
      watch_options    opts_;

      template <stdexec::receiver Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>, exec::io_uring_scheduler>
      auto subscribe(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ctx_, opts_, std::move(rcvr)};
      }
    };
  }  // namespace detail

  // ---------- inotify_context impl ----------

  inline inotify_context::inotify_context(std::vector<std::string> initial_paths,
                                          std::uint32_t            default_mask)
    : default_mask_{default_mask}
  {
    fd_ = ::inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
    if (fd_ < 0)
    {
      throw std::system_error{errno, std::system_category(), "inotify_init1"};
    }
    try
    {
      for (auto const & p: initial_paths)
      {
        add_watch(p);
      }
    }
    catch (...)
    {
      ::close(fd_);
      fd_ = -1;
      throw;
    }
  }

  inline inotify_context::~inotify_context()
  {
    if (fd_ >= 0)
    {
      ::close(fd_);
    }
  }

  inline auto
  inotify_context::add_watch(std::string_view path, std::optional<std::uint32_t> mask) -> int
  {
    std::string zpath{path};  // inotify_add_watch needs NUL-terminated
    int wd = ::inotify_add_watch(fd_, zpath.c_str(), mask.value_or(default_mask_));
    if (wd < 0)
    {
      throw std::system_error{errno, std::system_category(), "inotify_add_watch"};
    }
    {
      std::lock_guard lk{map_mu_};
      wd_to_path_[wd] = std::move(zpath);
    }
    return wd;
  }

  inline auto inotify_context::remove_watch(int wd) noexcept -> bool
  {
    // First confirm we know about this wd. If not, nothing to remove.
    {
      std::lock_guard lk{map_mu_};
      if (!wd_to_path_.contains(wd))
      {
        return false;
      }
    }
    // Call the kernel before erasing the map entry so path_for() reflects
    // the live watch set: a concurrent path_for(wd) during this window
    // returns the path (still true from the kernel's POV until rm_watch
    // succeeds). EINVAL means the kernel has already auto-removed the wd
    // (e.g. file unlinked) — treat that as success and clean up the stale
    // map entry. The IN_IGNORED that follows hits an empty map slot
    // (handler is a no-op then).
    int rc = ::inotify_rm_watch(fd_, wd);
    if (rc != 0 && errno != EINVAL)
    {
      return false;
    }
    {
      std::lock_guard lk{map_mu_};
      wd_to_path_.erase(wd);
    }
    return true;
  }

  inline auto inotify_context::path_for(int wd) const -> std::optional<std::string>
  {
    std::lock_guard lk{map_mu_};
    auto            it = wd_to_path_.find(wd);
    if (it == wd_to_path_.end())
    {
      return std::nullopt;
    }
    return it->second;
  }

  inline auto inotify_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, opts};
  }
}  // namespace inx
