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

#include <DiskArbitration/DiskArbitration.h>
#include <CoreFoundation/CoreFoundation.h>
#include <dispatch/dispatch.h>

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
    std::string                bsd_name;     // e.g. "disk7s1"; empty if framework returns NULL
    std::optional<std::string> volume_name;  // kDADiskDescriptionVolumeNameKey
    std::optional<std::string> volume_path;  // kDADiskDescriptionVolumePathKey (POSIX path)
    std::vector<std::string>   changed_keys; // populated only for description_changed
  };

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
  };

  class da_context;

  namespace __detail
  {
    struct __op_base
    {
      virtual ~__op_base()                    = default;
      virtual void deliver(disk_event) noexcept = 0;
    };

    template <class _Rcvr>
    struct __op;

    template <class _Rcvr>
    struct __next_receiver;

    struct __watch_sender;

    inline auto __cfstring_to_string(CFStringRef __s) -> std::optional<std::string>
    {
      if (!__s)
        return std::nullopt;
      if (const char* __p = CFStringGetCStringPtr(__s, kCFStringEncodingUTF8))
        return std::string{__p};
      CFIndex __len = CFStringGetLength(__s);
      CFIndex __max = CFStringGetMaximumSizeForEncoding(__len, kCFStringEncodingUTF8) + 1;
      std::string __buf(static_cast<size_t>(__max), '\0');
      if (!CFStringGetCString(__s, __buf.data(), __max, kCFStringEncodingUTF8))
        return std::nullopt;
      __buf.resize(std::strlen(__buf.data()));
      return __buf;
    }

    inline auto __cfurl_to_string(CFURLRef __url) -> std::optional<std::string>
    {
      if (!__url)
        return std::nullopt;
      CFStringRef __path = CFURLCopyFileSystemPath(__url, kCFURLPOSIXPathStyle);
      auto        __s    = __cfstring_to_string(__path);
      if (__path)
        CFRelease(__path);
      return __s;
    }

    inline auto __make_disk_event(disk_event_kind __kind,
                                  DADiskRef       __disk,
                                  CFArrayRef      __changed_keys = nullptr) -> disk_event
    {
      disk_event __ev;
      __ev.kind = __kind;

      if (const char* __bsd = DADiskGetBSDName(__disk))
        __ev.bsd_name = std::string{__bsd};

      if (CFDictionaryRef __desc = DADiskCopyDescription(__disk))
      {
        if (auto __name = static_cast<CFStringRef>(
              CFDictionaryGetValue(__desc, kDADiskDescriptionVolumeNameKey)))
          __ev.volume_name = __cfstring_to_string(__name);
        if (auto __path = static_cast<CFURLRef>(
              CFDictionaryGetValue(__desc, kDADiskDescriptionVolumePathKey)))
          __ev.volume_path = __cfurl_to_string(__path);
        CFRelease(__desc);
      }

      if (__changed_keys)
      {
        CFIndex __n = CFArrayGetCount(__changed_keys);
        __ev.changed_keys.reserve(static_cast<size_t>(__n));
        for (CFIndex __i = 0; __i < __n; ++__i)
        {
          auto __k = static_cast<CFStringRef>(CFArrayGetValueAtIndex(__changed_keys, __i));
          if (auto __s = __cfstring_to_string(__k))
            __ev.changed_keys.push_back(std::move(*__s));
        }
      }

      return __ev;
    }
  }  // namespace __detail

  class da_context
  {
   public:
    da_context() = default;

    ~da_context() = default;

    da_context(const da_context&)                    = delete;
    auto operator=(const da_context&) -> da_context& = delete;

    auto watch(watch_options __opts = {}) -> __detail::__watch_sender;

   private:
    template <class _Rcvr>
    friend struct __detail::__op;
    friend struct __detail::__watch_sender;

    std::atomic<__detail::__op_base*> __active_{nullptr};
  };

  namespace __detail
  {
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

    template <class _Rcvr>
    struct __op : __op_base
    {
      using __item_sender_t   = decltype(stdexec::just(std::declval<disk_event>()));
      using __next_sender_t   = exec::next_sender_of_t<_Rcvr, __item_sender_t>;
      using __next_receiver_t = __next_receiver<_Rcvr>;
      using __next_op_t       = stdexec::connect_result_t<__next_sender_t, __next_receiver_t>;

      struct __on_stop_fn
      {
        __op* __self_;
        void  operator()() noexcept;
      };

      using __stop_token_t    = stdexec::stop_token_of_t<stdexec::env_of_t<_Rcvr>>;
      using __stop_callback_t = stdexec::stop_callback_for_t<__stop_token_t, __on_stop_fn>;

      da_context*                      __ctx_;
      watch_options                    __opts_;
      _Rcvr                            __rcvr_;
      dispatch_queue_t                 __queue_{nullptr};
      DASessionRef                     __session_{nullptr};
      CFArrayRef                       __desc_keys_array_{nullptr};
      CFDictionaryRef                  __match_dict_{nullptr};
      bool                             __reg_appeared_{false};
      bool                             __reg_disappeared_{false};
      bool                             __reg_desc_changed_{false};
      std::atomic<bool>                __stop_requested_{false};
      std::binary_semaphore            __delivery_done_{0};
      int                              __delivery_state_{0};  // 1=value, 2=stopped, 3=error
      std::exception_ptr               __error_;
      std::optional<__stop_callback_t> __stop_cb_;
      std::unique_ptr<__next_op_t>     __next_op_;

      static auto __make_internal_queue(_Rcvr const& __r) -> dispatch_queue_t
      {
        auto __sch  = stdexec::get_scheduler(stdexec::get_env(__r));
        auto __attr = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL,
                                                              QOS_CLASS_UNSPECIFIED,
                                                              0);
        return dispatch_queue_create_with_target("dax.session", __attr, __sch.native_handle());
      }

      explicit __op(da_context* __c, watch_options __o, _Rcvr __r)
        : __ctx_{__c}
        , __opts_{__o}
        , __rcvr_{std::move(__r)}
        , __queue_{__make_internal_queue(__rcvr_)}
      {}

      ~__op()
      {
        if (__queue_)
          dispatch_release(__queue_);
      }

      static void __on_appeared_cb(DADiskRef __disk, void* __ctx) noexcept
      {
        static_cast<__op_base*>(__ctx)->deliver(
          __make_disk_event(disk_event_kind::appeared, __disk));
      }

      static void __on_disappeared_cb(DADiskRef __disk, void* __ctx) noexcept
      {
        static_cast<__op_base*>(__ctx)->deliver(
          __make_disk_event(disk_event_kind::disappeared, __disk));
      }

      static void __on_desc_changed_cb(DADiskRef  __disk,
                                       CFArrayRef __keys,
                                       void*      __ctx) noexcept
      {
        static_cast<__op_base*>(__ctx)->deliver(
          __make_disk_event(disk_event_kind::description_changed, __disk, __keys));
      }

      void start() & noexcept
      {
        __session_ = DASessionCreate(kCFAllocatorDefault);
        if (!__session_)
        {
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(
                               std::runtime_error{"DASessionCreate failed"}));
          return;
        }

        __op_base* __expected = nullptr;
        if (!__ctx_->__active_.compare_exchange_strong(__expected, this))
        {
          CFRelease(__session_);
          __session_ = nullptr;
          stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_),
                             std::make_exception_ptr(
                               std::runtime_error{"da_context already has an active watch"}));
          return;
        }

        void* __self_as_void = static_cast<__op_base*>(this);

        if (!__opts_.match.empty())
        {
          std::vector<CFStringRef> __keys;
          std::vector<CFTypeRef>   __values;
          // CFBoolean is shared/static and must not be released; track only
          // the CFStrings we created so we can drop our owning refs after
          // CFDictionaryCreate retains them.
          std::vector<CFStringRef> __string_values_to_release;
          __keys.reserve(__opts_.match.size());
          __values.reserve(__opts_.match.size());
          for (const auto& [__k, __v] : __opts_.match)
          {
            CFStringRef __ks = CFStringCreateWithCString(kCFAllocatorDefault,
                                                          __k.c_str(),
                                                          kCFStringEncodingUTF8);
            if (!__ks)
              continue;
            CFTypeRef __vs = nullptr;
            if (const bool* __b = std::get_if<bool>(&__v))
            {
              __vs = *__b ? kCFBooleanTrue : kCFBooleanFalse;
            }
            else
            {
              __vs = CFStringCreateWithCString(kCFAllocatorDefault,
                                                std::get<std::string>(__v).c_str(),
                                                kCFStringEncodingUTF8);
              if (!__vs)
              {
                CFRelease(__ks);
                continue;
              }
              __string_values_to_release.push_back(static_cast<CFStringRef>(__vs));
            }
            __keys.push_back(__ks);
            __values.push_back(__vs);
          }
          __match_dict_ = CFDictionaryCreate(
            kCFAllocatorDefault,
            reinterpret_cast<const void**>(__keys.data()),
            __values.data(),
            static_cast<CFIndex>(__keys.size()),
            &kCFTypeDictionaryKeyCallBacks,
            &kCFTypeDictionaryValueCallBacks);
          for (CFStringRef __ks : __keys)
            CFRelease(__ks);
          for (CFStringRef __vs : __string_values_to_release)
            CFRelease(__vs);
        }

        if (__opts_.watch_appeared)
        {
          DARegisterDiskAppearedCallback(__session_,
                                         /*match=*/__match_dict_,
                                         &__on_appeared_cb,
                                         __self_as_void);
          __reg_appeared_ = true;
        }
        if (__opts_.watch_disappeared)
        {
          DARegisterDiskDisappearedCallback(__session_,
                                            /*match=*/__match_dict_,
                                            &__on_disappeared_cb,
                                            __self_as_void);
          __reg_disappeared_ = true;
        }
        if (__opts_.watch_description_changed)
        {
          if (!__opts_.description_keys.empty())
          {
            std::vector<CFStringRef> __cf_keys;
            __cf_keys.reserve(__opts_.description_keys.size());
            for (const auto& __k : __opts_.description_keys)
            {
              if (CFStringRef __s = CFStringCreateWithCString(kCFAllocatorDefault,
                                                              __k.c_str(),
                                                              kCFStringEncodingUTF8))
                __cf_keys.push_back(__s);
            }
            __desc_keys_array_ = CFArrayCreate(
              kCFAllocatorDefault,
              reinterpret_cast<const void**>(__cf_keys.data()),
              static_cast<CFIndex>(__cf_keys.size()),
              &kCFTypeArrayCallBacks);
            // kCFTypeArrayCallBacks retains each element on insert; drop the
            // refs we owned from CFStringCreateWithCString.
            for (CFStringRef __s : __cf_keys)
              CFRelease(__s);
          }
          DARegisterDiskDescriptionChangedCallback(__session_,
                                                   /*match=*/__match_dict_,
                                                   /*watch=*/__desc_keys_array_,
                                                   &__on_desc_changed_cb,
                                                   __self_as_void);
          __reg_desc_changed_ = true;
        }

        DASessionSetDispatchQueue(__session_, __queue_);

        // Register stop callback last; if the token is already in stop state it
        // fires synchronously, which is now safe because the session is fully up.
        __stop_cb_.emplace(stdexec::get_stop_token(stdexec::get_env(__rcvr_)),
                           __on_stop_fn{this});
      }

      // Called from the dispatch queue (from __on_*_cb).
      void deliver(disk_event __ev) noexcept override
      {
        if (__stop_requested_.load(std::memory_order_acquire))
          return;

        __delivery_state_ = 0;

        try
        {
          __next_op_.reset(new __next_op_t(stdexec::connect(
            exec::set_next(__rcvr_, stdexec::just(std::move(__ev))),
            __next_receiver_t{this})));
          stdexec::start(*__next_op_);
        }
        catch (...)
        {
          __error_          = std::current_exception();
          __delivery_state_ = 3;
          __delivery_done_.release();
        }

        __delivery_done_.acquire();
        const int __state = __delivery_state_;
        __next_op_.reset();

        if (__state == 2)
        {
          __stop_requested_.store(true, std::memory_order_release);
          __schedule_finish_stopped();
        }
        else if (__state == 3)
        {
          __stop_requested_.store(true, std::memory_order_release);
          __schedule_finish_error(std::move(__error_));
        }
      }

      void __schedule_finish_stopped() noexcept
      {
        dispatch_async_f(__queue_, this, +[](void* __p) noexcept {
          auto* __o = static_cast<__op*>(__p);
          if (!__o->__session_)
            return;
          __o->__teardown_session();
          stdexec::set_stopped(static_cast<_Rcvr&&>(__o->__rcvr_));
        });
      }

      void __schedule_finish_error(std::exception_ptr __ep) noexcept
      {
        struct __closure
        {
          __op*              __o;
          std::exception_ptr __ep;
        };
        auto* __c = new __closure{this, std::move(__ep)};
        dispatch_async_f(__queue_, __c, +[](void* __p) noexcept {
          std::unique_ptr<__closure> __cu{static_cast<__closure*>(__p)};
          if (!__cu->__o->__session_)
            return;
          __cu->__o->__teardown_session();
          stdexec::set_error(static_cast<_Rcvr&&>(__cu->__o->__rcvr_), std::move(__cu->__ep));
        });
      }

      void __teardown_session() noexcept
      {
        if (!__session_)
          return;
        DASessionSetDispatchQueue(__session_, nullptr);
        void* __self_as_void = static_cast<__op_base*>(this);
        if (__reg_appeared_)
        {
          DAUnregisterCallback(__session_,
                               reinterpret_cast<void*>(&__on_appeared_cb),
                               __self_as_void);
          __reg_appeared_ = false;
        }
        if (__reg_disappeared_)
        {
          DAUnregisterCallback(__session_,
                               reinterpret_cast<void*>(&__on_disappeared_cb),
                               __self_as_void);
          __reg_disappeared_ = false;
        }
        if (__reg_desc_changed_)
        {
          DAUnregisterCallback(__session_,
                               reinterpret_cast<void*>(&__on_desc_changed_cb),
                               __self_as_void);
          __reg_desc_changed_ = false;
        }
        if (__desc_keys_array_)
        {
          CFRelease(__desc_keys_array_);
          __desc_keys_array_ = nullptr;
        }
        if (__match_dict_)
        {
          CFRelease(__match_dict_);
          __match_dict_ = nullptr;
        }
        CFRelease(__session_);
        __session_ = nullptr;
        __stop_cb_.reset();
        __ctx_->__active_.store(nullptr, std::memory_order_release);
      }
    };

    template <class _Rcvr>
    void __op<_Rcvr>::__on_stop_fn::operator()() noexcept
    {
      __self_->__stop_requested_.store(true, std::memory_order_release);
      // Cleanup must run on the dispatch queue to serialize with any in-flight
      // DA callback. If a delivery is currently blocked on the semaphore,
      // downstream stop_token propagation completes the next-sender (with
      // set_stopped), which unblocks deliver() so this enqueued teardown can
      // run.
      dispatch_async_f(__self_->__queue_, __self_, +[](void* __p) noexcept {
        auto* __o = static_cast<__op*>(__p);
        if (!__o->__session_)
          return;  // deliver() already finished us
        __o->__teardown_session();
        stdexec::set_stopped(static_cast<_Rcvr&&>(__o->__rcvr_));
      });
    }

    template <class _Rcvr>
    template <class... _Args>
    void __next_receiver<_Rcvr>::set_value(_Args&&...) noexcept
    {
      __self_->__delivery_state_ = 1;
      __self_->__delivery_done_.release();
    }

    template <class _Rcvr>
    void __next_receiver<_Rcvr>::set_stopped() noexcept
    {
      __self_->__delivery_state_ = 2;
      __self_->__delivery_done_.release();
    }

    template <class _Rcvr>
    template <class _E>
    void __next_receiver<_Rcvr>::set_error(_E&& __e) noexcept
    {
      if constexpr (std::is_same_v<std::decay_t<_E>, std::exception_ptr>)
      {
        __self_->__error_ = std::forward<_E>(__e);
      }
      else
      {
        __self_->__error_ = std::make_exception_ptr(std::forward<_E>(__e));
      }
      __self_->__delivery_state_ = 3;
      __self_->__delivery_done_.release();
    }

    template <class _Rcvr>
    auto __next_receiver<_Rcvr>::get_env() const noexcept -> stdexec::env_of_t<_Rcvr>
    {
      return stdexec::get_env(__self_->__rcvr_);
    }

    struct __watch_sender
    {
      using sender_concept        = exec::sequence_sender_tag;
      using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t(),
                                                                   stdexec::set_stopped_t(),
                                                                   stdexec::set_error_t(
                                                                     std::exception_ptr)>;

      using __item_sender_t = decltype(stdexec::just(std::declval<disk_event>()));
      using item_types      = exec::item_types<__item_sender_t>;

      da_context*   __ctx_;
      watch_options __opts_;

      template <stdexec::receiver _Rcvr>
        requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                           exec::libdispatch_scheduler>
      auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>
      {
        return __op<_Rcvr>{__ctx_, __opts_, std::move(__rcvr)};
      }
    };
  }  // namespace __detail

  // See examples/sequence_sender_on_scheduler.md.
  inline constexpr exec::__on_scheduler_t on_queue{};

  inline auto da_context::watch(watch_options __opts) -> __detail::__watch_sender
  {
    return {this, __opts};
  }
}  // namespace dax
