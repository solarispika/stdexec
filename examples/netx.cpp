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

// Demo: consume the netx interface-change sequence sender.
//
// Runs for ~20s and prints a counter on each interface_change_event. To
// trigger events:
//
//   macOS:    networksetup -setairportpower en0 off / on
//             toggle Wi-Fi from System Settings, plug/unplug ethernet
//
//   Windows:  netsh interface set interface "Wi-Fi" admin=disabled / enabled
//             toggle Wi-Fi from Settings, plug/unplug ethernet
//
// SCDynamicStore / NotifyIpInterfaceChange are hint APIs — the wrapper
// delivers a notification but no payload; consumers re-query interface
// state in their downstream then().

#if defined(__APPLE__) && defined(__MACH__)
#  include "exec/libdispatch_queue.hpp"
#  include "netx_mac_wrapper.hpp"
#elif defined(_WIN32)
#  include "exec/windows/windows_thread_pool.hpp"
#  include "netx_win_wrapper.hpp"
#endif

#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence/transform_each.hpp"
#include "exec/static_thread_pool.hpp"
#include "exec/when_any.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#if defined(__APPLE__) && defined(__MACH__)
#  include <ifaddrs.h>
#  include <net/if.h>
#  include <netdb.h>
#  include <sys/socket.h>
#elif defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
// winsock2 must precede iphlpapi
#  include <iphlpapi.h>
#  include <vector>
#endif

using namespace std::chrono_literals;

namespace
{
#if defined(__APPLE__) && defined(__MACH__)
  auto sample_active_interfaces() -> std::string
  {
    ifaddrs* __ifa = nullptr;
    if (::getifaddrs(&__ifa) != 0)
      return "(getifaddrs failed)";
    std::string __result;
    for (ifaddrs* __p = __ifa; __p; __p = __p->ifa_next)
    {
      if (!__p->ifa_addr || !(__p->ifa_flags & IFF_UP) || (__p->ifa_flags & IFF_LOOPBACK))
        continue;
      auto const __family = __p->ifa_addr->sa_family;
      if (__family != AF_INET && __family != AF_INET6)
        continue;
      char __host[NI_MAXHOST]{};
      if (::getnameinfo(__p->ifa_addr,
                        __family == AF_INET ? sizeof(sockaddr_in) : sizeof(sockaddr_in6),
                        __host,
                        sizeof __host,
                        nullptr,
                        0,
                        NI_NUMERICHOST)
          != 0)
        continue;
      if (!__result.empty())
        __result += ", ";
      __result += __p->ifa_name;
      __result += "=";
      __result += __host;
    }
    ::freeifaddrs(__ifa);
    return __result.empty() ? "(no UP interfaces)" : __result;
  }
#elif defined(_WIN32)
  auto sample_active_interfaces() -> std::string
  {
    ULONG const __flags = GAA_FLAG_SKIP_DNS_SERVER | GAA_FLAG_SKIP_MULTICAST
                        | GAA_FLAG_SKIP_ANYCAST;
    ULONG             __len = 16384;
    std::vector<char> __buf(__len);
    DWORD             __rc = ::GetAdaptersAddresses(AF_UNSPEC,
                                        __flags,
                                        nullptr,
                                        reinterpret_cast<PIP_ADAPTER_ADDRESSES>(__buf.data()),
                                        &__len);
    if (__rc == ERROR_BUFFER_OVERFLOW)
    {
      __buf.assign(__len, '\0');
      __rc = ::GetAdaptersAddresses(AF_UNSPEC,
                                    __flags,
                                    nullptr,
                                    reinterpret_cast<PIP_ADAPTER_ADDRESSES>(__buf.data()),
                                    &__len);
    }
    if (__rc != NO_ERROR)
      return "(GetAdaptersAddresses failed)";

    std::string __result;
    for (auto* __a = reinterpret_cast<PIP_ADAPTER_ADDRESSES>(__buf.data()); __a; __a = __a->Next)
    {
      if (__a->OperStatus != IfOperStatusUp || __a->IfType == IF_TYPE_SOFTWARE_LOOPBACK)
        continue;
      for (auto* __ua = __a->FirstUnicastAddress; __ua; __ua = __ua->Next)
      {
        char __host[NI_MAXHOST]{};
        if (::getnameinfo(__ua->Address.lpSockaddr,
                          __ua->Address.iSockaddrLength,
                          __host,
                          sizeof __host,
                          nullptr,
                          0,
                          NI_NUMERICHOST)
            != 0)
          continue;
        if (!__result.empty())
          __result += ", ";
        __result += __a->AdapterName;
        __result += "=";
        __result += __host;
      }
    }
    return __result.empty() ? "(no UP interfaces)" : __result;
  }
#endif
}  // namespace

auto main() -> int
{
  std::printf("netx demo: watching network changes for 20s\n");
#if defined(__APPLE__) && defined(__MACH__)
  std::printf("  trigger with: `networksetup -setairportpower en0 off/on` or toggle Wi-Fi\n");
  exec::libdispatch_queue __pool = exec::libdispatch_queue::make_concurrent("netx.demo");
  auto                    __nq   = __pool.get_scheduler();
#elif defined(_WIN32)
  std::printf("  trigger with: `netsh interface set interface \"Wi-Fi\" admin=disabled/enabled`\n");
  exec::windows_thread_pool __pool;
  auto                      __nq = __pool.get_scheduler();
#endif

  std::printf("[baseline] %s\n", sample_active_interfaces().c_str());

  netx::net_context __ctx;

  exec::static_thread_pool __timer_pool{1};
  auto                     __timer_sched = __timer_pool.get_scheduler();

  std::atomic<int> __counter{0};

  stdexec::sync_wait(
    exec::when_any(stdexec::starts_on(__timer_sched, stdexec::just())
                     | stdexec::then([&] { std::this_thread::sleep_for(20s); }),
                   exec::sequence_with_scheduler(__nq, __ctx.watch({}))
                     | exec::transform_each(stdexec::then(
                       [&](netx::interface_change_event)
                       {
                         int const __n = __counter.fetch_add(1) + 1;
                         std::printf("[change #%d] %s\n", __n, sample_active_interfaces().c_str());
                       }))
                     | exec::ignore_all_values()));

  std::printf("done (%d changes observed)\n", __counter.load());
  return 0;
}
