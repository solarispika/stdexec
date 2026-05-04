# powerx — system suspend/resume sequence sender

Cross-platform (macOS + Windows) example wrapping the OS suspend/resume
notification APIs as an `exec::sequence_sender_t`. Both platforms expose
the same `powerx::power_context` namespace; downstream code `#if`-selects
the right header. See
`docs/plans/2026-05-04-powerx-netx-system-signal-senders-design.md` for
the full design.

| Platform | Source API | Scheduler | Pattern reference |
|---|---|---|---|
| macOS   | `IORegisterForSystemPower` + `IONotificationPortSetDispatchQueue` | `exec::libdispatch_queue` | `da_wrapper.hpp` |
| Windows | `RegisterSuspendResumeNotification(DEVICE_NOTIFY_CALLBACK)` | `exec::windows_thread_pool` | `velx_wrapper.hpp` |

## Files

| File | Role |
|---|---|
| `powerx_mac_wrapper.hpp` | macOS implementation (IOPMLib + libdispatch) |
| `powerx_win_wrapper.hpp` | Windows implementation (callback subscription + thread pool) |
| `powerx.cpp` | Cross-platform demo: print suspend/resume for 30s |
| `powerx_README.md` | This file |

Build:

```sh
# macOS
cmake --build build --target example.powerx
./build/examples/example.powerx

# Windows
cmake --build build --target example.powerx
.\build\examples\Debug\example.powerx.exe
```

Trigger events from another shell:

```sh
# macOS
pmset sleepnow            # immediate sleep
caffeinate -t 1           # nudge wake assertions

# Windows
rundll32.exe powrprof.dll,SetSuspendState 0,1,0   # immediate sleep
```

Closing a laptop lid also fires the suspend / resume path on both
platforms.

## API

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("my.power");
powerx::power_context ctx;

stdexec::sync_wait(
    exec::sequence_with_scheduler(
        pool.get_scheduler(),
        ctx.watch({.watch_suspend = true,
                   .watch_resume  = true}))
  | exec::transform_each(stdexec::then([](powerx::power_event e) {
      switch (e.kind) {
        case powerx::power_event_kind::suspend: pause_sync(); break;
        case powerx::power_event_kind::resume:  resume_sync(); break;
      }
    }))
  | exec::ignore_all_values());
```

`power_event` carries only `kind`. Battery / display / thermal events are
out of scope for v1; see design doc Section 6.

### No approval / veto in v1

macOS exposes `kIOMessageCanSystemSleep`, which lets a listener veto
entry into sleep via `IOCancelPowerChange` (within ~30s). The wrapper
intentionally does not surface this: OrangeDrive (the motivating
consumer) does not use it, and supporting it cleanly requires a
per-event approval policy. A future field
`watch_options::system_will_sleep_approval` typed
`approval::bounded<sleep_request>` is reserved for that — see design
doc Section 5.

The wrapper subscribes to `kIOMessageSystemWillSleep` instead (the
non-vetoable, post-decision notification). It calls `IOAllowPowerChange`
*before* delivering the event to the receiver, so consumers see
fait-accompli "suspend has been allowed" semantics. This matches the
behavior of `MacPowerDetector` in OrangeDrive.

## Queue selection / scheduler

The wrapper does not own a dispatch queue. The queue is selected at the
pipeline level via `exec::sequence_with_scheduler`:

```cpp
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("...");
sync_wait(exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch(opts)) | ...);
```

`__watch_sender::subscribe` is constrained at compile time to require a
`libdispatch_scheduler` in the receiver's env. Composing with any other
scheduler type is a compile error — same posture as the FSEvents and DA
wrappers.

### Internal serial queue

Each watch operation creates its own serial queue with the user's queue
as target (`dispatch_queue_create_with_target`). IOKit's
`IONotificationPortSetDispatchQueue` then binds the system-power
notification port to this serial queue, so callbacks arrive serialized;
the worker thread comes from the user's pool.

## What happens under the hood

```
[user pipeline]
  exec::sequence_with_scheduler(pool, ctx.watch({...}))

[wrapper start()]
  CAS power_context::__active_ from null → this   (single-active invariant)
  IORegisterForSystemPower(this, &port, &on_cb, &notifier)
  IONotificationPortSetDispatchQueue(port, internal_queue)
  install stop_callback

[IOKit fires on internal_queue]
  on_power_event_cb(message_type)
    if WillSleep:
      IOAllowPowerChange(power_port, msg_arg)        // before deliver
      deliver({suspend})
    if HasPoweredOn:
      deliver({resume})

[deliver()]
  connect set_next(rcvr, just(event)) | __next_receiver
  start; binary_semaphore handshake
  on set_value: continue
  on set_stopped: schedule teardown + set_stopped
  on set_error:  schedule teardown + set_error

[stop_token fires OR delivery returns stopped/error]
  dispatch_async_f(internal_queue, teardown_session):
    IONotificationPortSetDispatchQueue(port, nullptr)
    IODeregisterForSystemPower(&notifier)
    IOServiceClose(power_port)
    IONotificationPortDestroy(port)
    clear active slot
    set_stopped / set_error
```

## OrangeDrive comparison

`MacPowerDetector` (OrangeDrive `lib/detector/mac/power-detector.cpp`)
owns a `std::jthread`, runs `CFRunLoopGetCurrent()` + `CFRunLoopRun()`
inside it, and uses a `std::condition_variable` to wait for the runloop
pointer to materialize / be cleared on `Stop()`. Its callback registry
is a `std::vector<std::function<void(Event)>>` guarded by a mutex on
`Add*Callback()` but invoked unlocked on the runloop thread.

`powerx::power_context` collapses all of that:

| OrangeDrive | powerx |
|---|---|
| `std::jthread` per detector | none — IOKit binds to user's libdispatch queue |
| `CFRunLoopGetCurrent()` + `CFRunLoopRun()` | none |
| `m_cv.wait` for runloop start/stop | none — `start()` is synchronous; teardown via `dispatch_async_f` |
| `std::vector<callback>` + `cbMutex` | one receiver per `power_context` (CAS-guarded) |
| `Stop()` post-condition is "runloop nulled" | `set_stopped` on receiver is the single source of truth |
| `IOAllowPowerChange` after callbacks | `IOAllowPowerChange` *before* deliver |

Threading invariants follow the libdispatch sequence-sender pattern:
per-op private serial queue with the user's queue as target; the
user's queue is never used directly as the callback queue.
