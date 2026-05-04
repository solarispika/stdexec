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

// macOS-only: wraps DiskArbitration as a stdexec sequence sender.
//
// Mirrors the FSEvents wrapper (`examples/fsevents_wrapper.hpp`) and the
// shared libdispatch-sequence-sender pattern documented in
// `docs/plans/2026-04-29-da-libdispatch-sender-design.md`.

#include <CoreFoundation/CoreFoundation.h>
#include <DiskArbitration/DiskArbitration.h>
#include <dispatch/dispatch.h>

#include "approval_policy.hpp"
#include "exec/libdispatch_queue.hpp"
#include "exec/on_scheduler.hpp"
#include "exec/sequence_senders.hpp"
#include "stdexec/execution.hpp"

#include <atomic>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace dax
{
  enum class disk_event_kind
  {
    appeared,
    disappeared,
    description_changed
  };

  struct disk_event
  {
    disk_event_kind            kind;
    std::string                bsd_name;      // e.g. "disk7s1"; empty if framework returns NULL
    std::optional<std::string> volume_name;   // kDADiskDescriptionVolumeNameKey
    std::optional<std::string> volume_path;   // kDADiskDescriptionVolumePathKey (POSIX path)
    std::vector<std::string>   changed_keys;  // populated only for description_changed
  };

  // Per-approval-callback payload — same identity fields as `disk_event`
  // (no `kind` / `changed_keys` since approvals are about a specific
  // mount/unmount/eject of an identified disk, not a state-change event).
  struct disk_info
  {
    std::string                bsd_name;
    std::optional<std::string> volume_name;
    std::optional<std::string> volume_path;
  };

  // Tagged-union policy specialised for dax::disk_info — see
  // examples/approval_policy.hpp for the sync/bounded/monostate
  // semantics. velx defines its own `velx::approval_policy` over its
  // own info type using the same template.
  using approval_policy = approval::policy<disk_info>;

  struct watch_options
  {
    bool watch_appeared{true};
    bool watch_disappeared{true};
    bool watch_description_changed{false};

    // When `watch_description_changed` is true, controls which DA description
    // keys cause a description_changed event to fire. Empty (default) = watch
    // all keys, matching DA's behavior when `NULL` is passed. Otherwise the
    // entries are forwarded (as a `CFArrayRef` of `CFString`s) as the `watch`
    // array to `DARegisterDiskDescriptionChangedCallback`. Values are the
    // raw strings behind the `kDADiskDescription*` constants — e.g.
    // `"DAVolumeName"` for `kDADiskDescriptionVolumeNameKey`,
    // `"DAVolumePath"` for `kDADiskDescriptionVolumePathKey`. Mirrors the
    // shape of `disk_event::changed_keys`, so a callback's reported key can
    // be compared directly against this watch list.
    // Ignored when `watch_description_changed` is false.
    std::vector<std::string> description_keys{};

    // Match filter forwarded as the `match` CFDictionaryRef argument to all
    // three DARegister*Callback calls — only disks whose description matches
    // every entry fire callbacks. Empty (default) = pass `nullptr` = match
    // every disk (prior behavior).
    //
    // Keys are the raw DA description key strings ("DAMediaWhole",
    // "DAVolumeKind", ...) — same form as `description_keys`. Values cover
    // the two CF types DA uses for match values: `bool` (most match keys
    // are CFBoolean, e.g. `DAMediaWhole`) and `std::string` (a few are
    // CFString, e.g. `DAVolumeKind = "apfs"`). Other CF value types
    // (CFNumber, CFUUID, ...) are not modeled — extend if needed.
    std::map<std::string, std::variant<bool, std::string>> match{};

    // Approval / pre-event hook policies. Default = monostate = the
    // wrapper does NOT call DARegister*ApprovalCallback for that lifecycle
    // verb (i.e. DA proceeds with no input from this listener — the
    // pre-existing v1 behavior).
    //
    // When set to approval::sync<disk_info>, the predicate runs inline on
    // the wrapper's private dispatch queue (the OS callback thread) and
    // its bool return is the verdict (true = allow, false = deny).
    //
    // When set to approval::bounded<disk_info>, the predicate runs on a
    // worker thread with a wrapper-enforced timeout; if it overruns, the
    // wrapper returns `on_timeout_allow` to DA and signals the predicate
    // via its `stdexec::inplace_stop_token`. See
    // `examples/approval_policy.hpp`.
    approval_policy mount_approval{};
    approval_policy unmount_approval{};
    approval_policy eject_approval{};
  };

  class da_context;

  namespace detail
  {
    struct op_base
    {
      virtual ~op_base()                      = default;
      virtual void deliver(disk_event) noexcept = 0;
    };

    template <class Rcvr>
    struct op;

    template <class Rcvr>
    struct next_receiver;

    struct watch_sender;

    inline auto cfstring_to_string(CFStringRef s) -> std::optional<std::string>
    {
      if (!s)
        return std::nullopt;
      if (char const * p = CFStringGetCStringPtr(s, kCFStringEncodingUTF8))
        return std::string{p};
      CFIndex     len = CFStringGetLength(s);
      CFIndex     max = CFStringGetMaximumSizeForEncoding(len, kCFStringEncodingUTF8) + 1;
      std::string buf(static_cast<size_t>(max), '\0');
      if (!CFStringGetCString(s, buf.data(), max, kCFStringEncodingUTF8))
        return std::nullopt;
      buf.resize(std::strlen(buf.data()));
      return buf;
    }

    inline auto cfurl_to_string(CFURLRef url) -> std::optional<std::string>
    {
      if (!url)
        return std::nullopt;
      CFStringRef path = CFURLCopyFileSystemPath(url, kCFURLPOSIXPathStyle);
      auto        s    = cfstring_to_string(path);
      if (path)
        CFRelease(path);
      return s;
    }

    inline auto make_disk_info(DADiskRef disk) -> disk_info
    {
      disk_info i;
      if (char const * bsd = DADiskGetBSDName(disk))
        i.bsd_name = std::string{bsd};

      if (CFDictionaryRef desc = DADiskCopyDescription(disk))
      {
        if (auto name = static_cast<CFStringRef>(
              CFDictionaryGetValue(desc, kDADiskDescriptionVolumeNameKey)))
          i.volume_name = cfstring_to_string(name);
        if (auto path = static_cast<CFURLRef>(
              CFDictionaryGetValue(desc, kDADiskDescriptionVolumePathKey)))
          i.volume_path = cfurl_to_string(path);
        CFRelease(desc);
      }
      return i;
    }

    // Translate a bool verdict into DA's expected return type.
    // nullptr = allow; a non-NULL DADissenterRef vetoes the operation.
    // The dissenter is owned by DA after return; the registered status
    // (kDAReturnNotPermitted) and reason string surface in `diskutil`
    // and the system log, so the user can tell *who* vetoed.
    inline auto verdict_to_dissenter(bool allow) noexcept -> DADissenterRef
    {
      if (allow)
        return nullptr;
      return DADissenterCreate(kCFAllocatorDefault,
                               kDAReturnNotPermitted,
                               CFSTR("denied by dax::watch_options approval policy"));
    }

    inline auto
    make_disk_event(disk_event_kind kind, DADiskRef disk, CFArrayRef changed_keys = nullptr)
      -> disk_event
    {
      disk_event ev;
      ev.kind = kind;

      if (char const * bsd = DADiskGetBSDName(disk))
        ev.bsd_name = std::string{bsd};

      if (CFDictionaryRef desc = DADiskCopyDescription(disk))
      {
        if (auto name = static_cast<CFStringRef>(
              CFDictionaryGetValue(desc, kDADiskDescriptionVolumeNameKey)))
          ev.volume_name = cfstring_to_string(name);
        if (auto path = static_cast<CFURLRef>(
              CFDictionaryGetValue(desc, kDADiskDescriptionVolumePathKey)))
          ev.volume_path = cfurl_to_string(path);
        CFRelease(desc);
      }

      if (changed_keys)
      {
        CFIndex n = CFArrayGetCount(changed_keys);
        ev.changed_keys.reserve(static_cast<size_t>(n));
        for (CFIndex i = 0; i < n; ++i)
        {
          auto k = static_cast<CFStringRef>(CFArrayGetValueAtIndex(changed_keys, i));
          if (auto s = cfstring_to_string(k))
            ev.changed_keys.push_back(std::move(*s));
        }
      }

      return ev;
    }
  }  // namespace detail

  class da_context
  {
   public:
    da_context() = default;

    ~da_context() = default;

    da_context(da_context const &)                    = delete;
    auto operator=(da_context const &) -> da_context& = delete;

    auto watch(watch_options opts = {}) -> detail::watch_sender;

   private:
    template <class Rcvr>
    friend struct detail::op;
    friend struct detail::watch_sender;

    std::atomic<detail::op_base*> active_{nullptr};
  };

  namespace detail
  {
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

    template <class Rcvr>
    struct op : op_base
    {
      using item_sender_t   = decltype(stdexec::just(std::declval<disk_event>()));
      using next_sender_t   = exec::next_sender_of_t<Rcvr, item_sender_t>;
      using next_receiver_t = next_receiver<Rcvr>;
      using next_op_t       = stdexec::connect_result_t<next_sender_t, next_receiver_t>;

      struct on_stop_fn
      {
        op* self_;
        void  operator()() noexcept;
      };

      using stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<Rcvr>>;
      using stop_callback_t = stdexec::stop_callback_for_t<stop_token_t, on_stop_fn>;

      da_context*                      ctx_;
      watch_options                    opts_;
      Rcvr                            rcvr_;
      dispatch_queue_t                 queue_{nullptr};
      DASessionRef                     session_{nullptr};
      CFArrayRef                       desc_keys_array_{nullptr};
      CFDictionaryRef                  match_dict_{nullptr};
      std::atomic<bool>                stop_requested_{false};
      std::binary_semaphore            delivery_done_{0};
      int                              delivery_state_{0};  // 1=value, 2=stopped, 3=error
      std::exception_ptr               error_;
      std::optional<stop_callback_t> stop_cb_;
      std::unique_ptr<next_op_t>     next_op_;

      static auto make_internal_queue(Rcvr const & r) -> dispatch_queue_t
      {
        auto sch  = stdexec::get_scheduler(stdexec::get_env(r));
        auto attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL,
                                                              QOS_CLASS_UNSPECIFIED,
                                                              0);
        return dispatch_queue_create_with_target("dax.session", attr, sch.native_handle());
      }

      explicit op(da_context* c, watch_options o, Rcvr r)
        : ctx_{c}
        , opts_{o}
        , rcvr_{std::move(r)}
        , queue_{make_internal_queue(rcvr_)}
      {}

      ~op()
      {
        if (queue_)
          dispatch_release(queue_);
      }

      static void on_appeared_cb(DADiskRef disk, void* ctx) noexcept
      {
        static_cast<op_base*>(ctx)->deliver(
          make_disk_event(disk_event_kind::appeared, disk));
      }

      static void on_disappeared_cb(DADiskRef disk, void* ctx) noexcept
      {
        static_cast<op_base*>(ctx)->deliver(
          make_disk_event(disk_event_kind::disappeared, disk));
      }

      static void on_desc_changed_cb(DADiskRef disk, CFArrayRef keys, void* ctx) noexcept
      {
        static_cast<op_base*>(ctx)->deliver(
          make_disk_event(disk_event_kind::description_changed, disk, keys));
      }

      // Approval callbacks. Signature: synchronous return of DADissenterRef
      // (NULL = allow). All three share the same shape: resolve the configured
      // policy via the helper, lazily building disk_info only if the policy
      // actually needs it.
      static auto on_mount_approval_cb(DADiskRef disk, void* ctx) noexcept -> DADissenterRef
      {
        auto*      self  = static_cast<op*>(ctx);
        bool const allow = approval::resolve_verdict<disk_info>(self->opts_.mount_approval,
                                                                  [&]
                                                                  {
                                                                    return make_disk_info(disk);
                                                                  });
        return verdict_to_dissenter(allow);
      }

      static auto on_unmount_approval_cb(DADiskRef disk, void* ctx) noexcept -> DADissenterRef
      {
        auto*      self  = static_cast<op*>(ctx);
        bool const allow = approval::resolve_verdict<disk_info>(self->opts_.unmount_approval,
                                                                  [&]
                                                                  {
                                                                    return make_disk_info(disk);
                                                                  });
        return verdict_to_dissenter(allow);
      }

      static auto on_eject_approval_cb(DADiskRef disk, void* ctx) noexcept -> DADissenterRef
      {
        auto*      self  = static_cast<op*>(ctx);
        bool const allow = approval::resolve_verdict<disk_info>(self->opts_.eject_approval,
                                                                  [&]
                                                                  {
                                                                    return make_disk_info(disk);
                                                                  });
        return verdict_to_dissenter(allow);
      }

      void start() & noexcept
      {
        session_ = DASessionCreate(kCFAllocatorDefault);
        if (!session_)
        {
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"DASessionCreate failed"}));
          return;
        }

        op_base* expected = nullptr;
        if (!ctx_->active_.compare_exchange_strong(expected, this))
        {
          CFRelease(session_);
          session_ = nullptr;
          stdexec::set_error(static_cast<Rcvr&&>(rcvr_),
                             std::make_exception_ptr(std::runtime_error{"da_context already has an "
                                                                        "active watch"}));
          return;
        }

        void* self_as_void = static_cast<op_base*>(this);

        if (!opts_.match.empty())
        {
          std::vector<CFStringRef> keys;
          std::vector<CFTypeRef>   values;
          // CFBoolean is shared/static and must not be released; track only
          // the CFStrings we created so we can drop our owning refs after
          // CFDictionaryCreate retains them.
          std::vector<CFStringRef> string_values_to_release;
          keys.reserve(opts_.match.size());
          values.reserve(opts_.match.size());
          for (auto const& [k, v]: opts_.match)
          {
            CFStringRef ks = CFStringCreateWithCString(kCFAllocatorDefault,
                                                         k.c_str(),
                                                         kCFStringEncodingUTF8);
            if (!ks)
              continue;
            CFTypeRef vs = nullptr;
            if (bool const * b = std::get_if<bool>(&v))
            {
              vs = *b ? kCFBooleanTrue : kCFBooleanFalse;
            }
            else
            {
              vs = CFStringCreateWithCString(kCFAllocatorDefault,
                                               std::get<std::string>(v).c_str(),
                                               kCFStringEncodingUTF8);
              if (!vs)
              {
                CFRelease(ks);
                continue;
              }
              string_values_to_release.push_back(static_cast<CFStringRef>(vs));
            }
            keys.push_back(ks);
            values.push_back(vs);
          }
          match_dict_ = CFDictionaryCreate(kCFAllocatorDefault,
                                             reinterpret_cast<void const **>(keys.data()),
                                             values.data(),
                                             static_cast<CFIndex>(keys.size()),
                                             &kCFTypeDictionaryKeyCallBacks,
                                             &kCFTypeDictionaryValueCallBacks);
          for (CFStringRef ks: keys)
            CFRelease(ks);
          for (CFStringRef vs: string_values_to_release)
            CFRelease(vs);
        }

        if (opts_.watch_appeared)
        {
          DARegisterDiskAppearedCallback(session_,
                                         /*match=*/match_dict_,
                                         &on_appeared_cb,
                                         self_as_void);
        }
        if (opts_.watch_disappeared)
        {
          DARegisterDiskDisappearedCallback(session_,
                                            /*match=*/match_dict_,
                                            &on_disappeared_cb,
                                            self_as_void);
        }
        if (opts_.watch_description_changed)
        {
          if (!opts_.description_keys.empty())
          {
            std::vector<CFStringRef> cf_keys;
            cf_keys.reserve(opts_.description_keys.size());
            for (auto const & k: opts_.description_keys)
            {
              if (CFStringRef s = CFStringCreateWithCString(kCFAllocatorDefault,
                                                              k.c_str(),
                                                              kCFStringEncodingUTF8))
                cf_keys.push_back(s);
            }
            desc_keys_array_ = CFArrayCreate(kCFAllocatorDefault,
                                               reinterpret_cast<void const **>(cf_keys.data()),
                                               static_cast<CFIndex>(cf_keys.size()),
                                               &kCFTypeArrayCallBacks);
            // kCFTypeArrayCallBacks retains each element on insert; drop the
            // refs we owned from CFStringCreateWithCString.
            for (CFStringRef s: cf_keys)
              CFRelease(s);
          }
          DARegisterDiskDescriptionChangedCallback(session_,
                                                   /*match=*/match_dict_,
                                                   /*watch=*/desc_keys_array_,
                                                   &on_desc_changed_cb,
                                                   self_as_void);
        }
        if (opts_.mount_approval.index() != 0)
        {
          DARegisterDiskMountApprovalCallback(session_,
                                              /*match=*/match_dict_,
                                              &on_mount_approval_cb,
                                              self_as_void);
        }
        if (opts_.unmount_approval.index() != 0)
        {
          DARegisterDiskUnmountApprovalCallback(session_,
                                                /*match=*/match_dict_,
                                                &on_unmount_approval_cb,
                                                self_as_void);
        }
        if (opts_.eject_approval.index() != 0)
        {
          DARegisterDiskEjectApprovalCallback(session_,
                                              /*match=*/match_dict_,
                                              &on_eject_approval_cb,
                                              self_as_void);
        }

        DASessionSetDispatchQueue(session_, queue_);

        // Register stop callback last; if the token is already in stop state it
        // fires synchronously, which is now safe because the session is fully up.
        stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(rcvr_)), on_stop_fn{this});
      }

      // Called from the dispatch queue (from on_*_cb).
      void deliver(disk_event ev) noexcept override
      {
        if (stop_requested_.load(std::memory_order_acquire))
          return;

        delivery_state_ = 0;

        try
        {
          next_op_.reset(new next_op_t(
            stdexec::connect(exec::set_next(rcvr_, stdexec::just(std::move(ev))),
                             next_receiver_t{this})));
          stdexec::start(*next_op_);
        }
        catch (...)
        {
          error_          = std::current_exception();
          delivery_state_ = 3;
          delivery_done_.release();
        }

        delivery_done_.acquire();
        int const state = delivery_state_;
        next_op_.reset();

        if (state == 2)
        {
          stop_requested_.store(true, std::memory_order_release);
          schedule_finish_stopped();
        }
        else if (state == 3)
        {
          stop_requested_.store(true, std::memory_order_release);
          schedule_finish_error(std::move(error_));
        }
      }

      void schedule_finish_stopped() noexcept
      {
        dispatch_async_f(
          queue_,
          this,
          +[](void* p) noexcept
          {
            auto* o = static_cast<op*>(p);
            if (!o->session_)
              return;
            o->teardown_session();
            stdexec::set_stopped(static_cast<Rcvr&&>(o->rcvr_));
          });
      }

      void schedule_finish_error(std::exception_ptr ep) noexcept
      {
        struct closure
        {
          op*              o;
          std::exception_ptr ep;
        };
        auto* c = new closure{this, std::move(ep)};
        dispatch_async_f(
          queue_,
          c,
          +[](void* p) noexcept
          {
            std::unique_ptr<closure> cu{static_cast<closure*>(p)};
            if (!cu->o->session_)
              return;
            cu->o->teardown_session();
            stdexec::set_error(static_cast<Rcvr&&>(cu->o->rcvr_), std::move(cu->ep));
          });
      }

      void teardown_session() noexcept
      {
        if (!session_)
          return;
        DASessionSetDispatchQueue(session_, nullptr);
        void* self_as_void = static_cast<op_base*>(this);
        if (opts_.watch_appeared)
        {
          DAUnregisterCallback(session_,
                               reinterpret_cast<void*>(&on_appeared_cb),
                               self_as_void);
        }
        if (opts_.watch_disappeared)
        {
          DAUnregisterCallback(session_,
                               reinterpret_cast<void*>(&on_disappeared_cb),
                               self_as_void);
        }
        if (opts_.watch_description_changed)
        {
          DAUnregisterCallback(session_,
                               reinterpret_cast<void*>(&on_desc_changed_cb),
                               self_as_void);
        }
        if (opts_.mount_approval.index() != 0)
        {
          DAUnregisterCallback(session_,
                               reinterpret_cast<void*>(&on_mount_approval_cb),
                               self_as_void);
        }
        if (opts_.unmount_approval.index() != 0)
        {
          DAUnregisterCallback(session_,
                               reinterpret_cast<void*>(&on_unmount_approval_cb),
                               self_as_void);
        }
        if (opts_.eject_approval.index() != 0)
        {
          DAUnregisterCallback(session_,
                               reinterpret_cast<void*>(&on_eject_approval_cb),
                               self_as_void);
        }
        if (desc_keys_array_)
        {
          CFRelease(desc_keys_array_);
          desc_keys_array_ = nullptr;
        }
        if (match_dict_)
        {
          CFRelease(match_dict_);
          match_dict_ = nullptr;
        }
        CFRelease(session_);
        session_ = nullptr;
        stop_cb_.reset();
        ctx_->active_.store(nullptr, std::memory_order_release);
      }
    };

    template <class Rcvr>
    void op<Rcvr>::on_stop_fn::operator()() noexcept
    {
      self_->stop_requested_.store(true, std::memory_order_release);
      // Cleanup must run on the dispatch queue to serialize with any in-flight
      // DA callback. If a delivery is currently blocked on the semaphore,
      // downstream stop_token propagation completes the next-sender (with
      // set_stopped), which unblocks deliver() so this enqueued teardown can
      // run.
      dispatch_async_f(
        self_->queue_,
        self_,
        +[](void* p) noexcept
        {
          auto* o = static_cast<op*>(p);
          if (!o->session_)
            return;  // deliver() already finished us
          o->teardown_session();
          stdexec::set_stopped(static_cast<Rcvr&&>(o->rcvr_));
        });
    }

    template <class Rcvr>
    template <class... Args>
    void next_receiver<Rcvr>::set_value(Args&&...) noexcept
    {
      self_->delivery_state_ = 1;
      self_->delivery_done_.release();
    }

    template <class Rcvr>
    void next_receiver<Rcvr>::set_stopped() noexcept
    {
      self_->delivery_state_ = 2;
      self_->delivery_done_.release();
    }

    template <class Rcvr>
    template <class E>
    void next_receiver<Rcvr>::set_error(E&& e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<E>, std::exception_ptr>)
      {
        self_->error_ = std::forward<E>(e);
      }
      else
      {
        self_->error_ = std::make_exception_ptr(std::forward<E>(e));
      }
      self_->delivery_state_ = 3;
      self_->delivery_done_.release();
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

      using item_sender_t = decltype(stdexec::just(std::declval<disk_event>()));
      using item_types      = exec::item_types<item_sender_t>;

      da_context*   ctx_;
      watch_options opts_;

      template <stdexec::receiver Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<Rcvr>, exec::libdispatch_scheduler>
      auto subscribe(Rcvr rcvr) const -> op<Rcvr>
      {
        return op<Rcvr>{ctx_, opts_, std::move(rcvr)};
      }
    };
  }  // namespace detail

  inline auto da_context::watch(watch_options opts) -> detail::watch_sender
  {
    return {this, opts};
  }
}  // namespace dax
