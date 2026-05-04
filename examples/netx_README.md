# netx — interface-change sequence sender

Cross-platform (macOS + Windows) example wrapping the OS network-change
notification APIs as an `exec::sequence_sender_t`. Both platforms expose
the same `netx::net_context` namespace; downstream code `#if`-selects
the right header. See
`docs/plans/2026-05-04-powerx-netx-system-signal-senders-design.md` for
the full design.

| Platform | Source API | Scheduler | Pattern reference |
|---|---|---|---|
| macOS   | `SCDynamicStoreCreate` + `SCDynamicStoreSetDispatchQueue` | `exec::libdispatch_queue` | `da_wrapper.hpp` |
| Windows | `NotifyIpInterfaceChange`                                | `exec::windows_thread_pool` | `velx_wrapper.hpp` |

## Files

| File | Role |
|---|---|
| `netx_mac_wrapper.hpp` | macOS implementation (SCDynamicStore + libdispatch) |
| `netx_win_wrapper.hpp` | Windows implementation (NotifyIpInterfaceChange + thread pool) |
| `netx.cpp` | Cross-platform demo: print interface change events for 20s |
| `netx_README.md` | This file |

Build:

```sh
# macOS
cmake --build build --target example.netx
./build/examples/example.netx

# Windows
cmake --build build --target example.netx
.\build\examples\Debug\example.netx.exe
```

Trigger events from another shell:

```sh
# macOS
networksetup -setairportpower en0 off
networksetup -setairportpower en0 on

# Windows
netsh interface set interface "Wi-Fi" admin=disabled
netsh interface set interface "Wi-Fi" admin=enabled
```

Or toggle Wi-Fi from System Settings, plug/unplug ethernet, change DHCP
scope, etc.

## API

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("my.net");
netx::net_context ctx;

stdexec::sync_wait(
    exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch({}))
  | exec::transform_each(stdexec::then([](netx::interface_change_event) {
      // Hint received — re-query interface state with getifaddrs /
      // SCNetworkInterfaceCopyAll / your own checksum if you need
      // change detection.
      reload_routes();
    }))
  | exec::ignore_all_values());
```

`interface_change_event` is intentionally empty. SCDynamicStore is a
hint API: it tells you *something* in the IPv4 or IPv6 global state
changed, but consumers must re-read the interface table themselves to
discover what.

### Why no diff / payload

The Windows side (and OrangeDrive's MacNetworkDetector) uses a checksum
over `getifaddrs` output to filter out spurious notifications. That
debounce is an application policy: which interfaces matter
(BeeDriveTap*, lo0, etc.) and how to compare addresses depend on
the consumer. Modeling all of that in the wrapper would push platform
abstractions into the wrong layer; consumers should run their own
`then()` that compares `getifaddrs` snapshots if they care.

OrangeDrive-side filters (BeeDriveTap*/BeeDriveTun* skip, 2-second
cooldown after start, IPv4+IPv6 unicast checksum) belong in the
downstream `transform_each` once netx is integrated — not in the
wrapper.

### No approval / veto

Both `SCDynamicStore` and the equivalent `NotifyIpInterfaceChange` on
Windows are post-event broadcasts. There is no veto path. v1 reflects
that contract — same "no approval surface" stance as `udx` (which is
also kernel-broadcast-only).

## Queue selection / scheduler

The wrapper does not own a dispatch queue. The queue is selected at the
pipeline level via `exec::sequence_with_scheduler`:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("...");
sync_wait(exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch({})) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require a
`libdispatch_scheduler` in the receiver's env. Composing with any other
scheduler type is a compile error — same posture as the FSEvents and DA
wrappers.

### Internal serial queue

Each watch operation creates its own serial queue with the user's queue
as target (`dispatch_queue_create_with_target`). `SCDynamicStoreSetDispatchQueue`
binds the dynamic store to this serial queue, so notification
callbacks arrive serialized.

## What happens under the hood

```
[user pipeline]
  exec::sequence_with_scheduler(pool, ctx.watch({}))

[wrapper start()]
  CAS net_context::__active_ from null → this
  store = SCDynamicStoreCreate(callback=on_change_cb, ctx=this)
  patterns = {kSCDynamicStoreDomainState/Network/Global/IPv4,
              kSCDynamicStoreDomainState/Network/Global/IPv6}
  SCDynamicStoreSetNotificationKeys(store, nullptr, patterns)
  SCDynamicStoreSetDispatchQueue(store, internal_queue)
  install stop_callback

[SC fires on internal_queue]
  on_change_cb(store, changedKeys, ctx)
    deliver({})         // payload is empty by design

[deliver()]
  connect set_next(rcvr, just(event)) | __next_receiver
  start; binary_semaphore handshake
  on set_value: continue
  on set_stopped: schedule teardown + set_stopped
  on set_error:  schedule teardown + set_error

[stop_token fires OR delivery returns stopped/error]
  dispatch_async_f(internal_queue, teardown_session):
    SCDynamicStoreSetDispatchQueue(store, nullptr)
    CFRelease(store)
    clear active slot
    set_stopped / set_error
```

## OrangeDrive comparison

`MacNetworkDetector` (OrangeDrive `lib/detector/mac/network-detector.cpp`)
spins a `std::jthread`, builds `SCDynamicStoreCreateRunLoopSource`,
adds it to a fresh `CFRunLoopGetCurrent()`, runs `CFRunLoopRun()`, and
joins the thread on `Stop()`. It also subscribes to `PowerDetector` so
that `Stop()` fires on suspend and `Start()` re-fires on resume.

`netx::net_context` collapses the runloop ownership:

| OrangeDrive | netx |
|---|---|
| `std::jthread` + `CFRunLoopAddSource` + `CFRunLoopRun` | none — store binds to user's libdispatch queue |
| `m_cv.wait` for runloop start/stop | none — `start()` is synchronous; teardown via `dispatch_async_f` |
| `std::vector<callback>` + `cbMutex` | one receiver per `net_context` (CAS-guarded) |
| `NetworkDetector` ctor subscribes to `PowerDetector` | application-level pipeline composition (e.g. `when_any` of `powerx` and `netx`) |

The Power-coupling — auto-Stop on suspend, auto-Start on resume —
becomes pipeline composition rather than ctor wiring. The wrapper is
deliberately unopinionated about this: subscribers can compose with
`powerx` however they like. See the design doc Section "API 預覽" for a
sketch.

Threading invariants follow the libdispatch sequence-sender pattern:
per-op private serial queue with the user's queue as target.
