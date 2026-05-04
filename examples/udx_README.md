# udx (libudev) wrapper for stdexec

Linux-only example: wraps libudev's `udev_monitor` on the kernel
netlink uevent stream as an `exec::sequence_sender_t`, with completion
driven by `exec::io_uring_context` (`IORING_OP_POLL_ADD` on the
monitor fd). Companion to `examples/da_wrapper.hpp` (macOS /
DiskArbitration) and `examples/velx_wrapper.hpp` (Windows /
`CM_Register_Notification`) on the *device-level* event source axis;
companion to `examples/inotify_wrapper.hpp` on the *Linux /
io_uring* axis.

## Files

| File | Role |
|---|---|
| `udx_wrapper.hpp` | Header-only `udx::udev_context` |
| `udx.cpp`         | Demo: 30-second observer for block subsystem |
| `udx_README.md`   | This file |

Build (only configured under Linux when `pkg-config libudev` resolves):

```sh
sudo apt-get install -y libudev-dev pkg-config   # one-time
cmake --build build --target example.udx
./build/examples/example.udx
```

## API

```cpp
exec::io_uring_context ring;
std::thread driver{[&] { ring.run_until_stopped(); }};

udx::udev_context ctx;

stdexec::sync_wait(
    exec::sequence_with_scheduler(ring.get_scheduler(),
        ctx.watch({.subsystem      = "block",
                   .initial_replay = true}))
  | exec::transform_each(stdexec::then([](udx::device_event e) {
      // e.kind: add / remove / change / online / offline / bind / unbind / move / unknown
      // e.subsystem ("block"), e.devtype ("disk"/"partition"/...)
      // e.sysname ("sda1"), e.devnode ("/dev/sda1")
      // e.syspath ("/sys/devices/.../sda/sda1")
      // e.properties (only populated if want_properties=true)
    }))
  | exec::ignore_all_values());

ring.request_stop();
driver.join();
```

`watch_options` controls subscription scope:

```cpp
udx::watch_options opts{
    .subsystem      = "block",      // udev_monitor_filter_add_match_subsystem_devtype
    .devtype        = std::nullopt, // nullopt = match disk + partition
    .initial_replay = true,         // synthesize add events for currently-present devices
    .want_properties = false,       // copy ID_FS_TYPE / DEVPATH / DEVNAME / ... into device_event
    .property_keys  = {},           // empty + want_properties=true → all properties
};
ctx.watch(opts);
```

## Platform comparison

| | macOS (`dax`) | Windows (`velx`) | **Linux (`udx`)** |
|---|---|---|---|
| Reactor scheduler | `exec::libdispatch_queue` | `exec::windows_thread_pool` | **`exec::io_uring_context`** |
| Env adapter | `exec::sequence_with_scheduler` | `exec::sequence_with_scheduler` | **`exec::sequence_with_scheduler`** |
| Source primitive | `DARegisterDisk*Callback` | `CM_Register_Notification` (`GUID_DEVINTERFACE_VOLUME`) | **`udev_monitor` netlink fd + `IORING_OP_POLL_ADD`** |
| Item shape | per-event `disk_event` | per-event `volume_event` | **per-event `device_event`** |
| Initial replay | DA daemon-side | `CM_Get_Device_Interface_List_PRESENT` | **`udev_enumerate_scan_devices`** |
| Approval | mount/unmount/eject | query-remove | **none — see below** |
| Backpressure | `binary_semaphore` on dispatch queue | MPSC + drainer + `binary_semaphore` | **continuation-style** (next POLL armed from `set_value`) |

All four share: `sequence_sender_t`, single-active subscription per
context (CAS-guarded), env-injected scheduler enforced at compile
time, native cancellation routed through stop callback.

## Why no approval surface

Unlike `dax` (DiskArbitration mount/unmount/eject approval) and
`velx` (CM `QUERYREMOVE` approval), the kernel uevent layer is
**broadcast-only**: by the time userspace receives `add` / `remove`,
the kernel has already done it. There is no veto path in libudev,
and this is not a wrapper limitation — it is the OS API contract.

If you actually need approval-flavored semantics on Linux:

| Need | Where to look |
|---|---|
| Block kernel auto-mount on a device | Ship a udev rule with `ENV{UDISKS_IGNORE}="1"` (configuration, not callback). Out of scope for `udx`. |
| Approve / deny user-initiated mount or eject | udisks2 over D-Bus + polkit policy. Would be a separate `udisksx::` wrapper, not this one. |
| Block file access | `fanotify` with `FAN_OPEN_PERM` / `FAN_PRE_ACCESS`. Different layer entirely. |
| React after a removal completes | This wrapper. Receive the `remove` event in your `transform_each` and clean up. The kernel has already disconnected by then; your code is post-event. |

The design doc
(`docs/plans/2026-05-04-udx-libudev-sender-design.md`) records why
three pseudo-approval paths were considered and rejected — most
importantly, dynamic udev rule generation with `RUN+=` IPC back to
the wrapper, which is technically possible but trades a per-process
library API for global system state, root requirement, 30 s `RUN+=`
timeout, and crash leakage. The trade is structurally bad enough
that the wrapper does not even offer it as an opt-in.

## What happens under the hood

```
                   ┌─────────────────────────────────────────────────────────┐
   start()         │  1. CAS into udev_context::__active_                    │
                   │  2. udev_new + udev_monitor_new_from_netlink("udev")    │
                   │  3. filter_add_match_subsystem_devtype                  │
                   │  4. enable_receiving (so live events queue up           │
                   │     starting NOW, before enumerate)                     │
                   │  5. udev_enumerate_scan_devices → synthesize `add`      │
                   │     events into __pending_                              │
                   │  6. drain_or_poll                                       │
                   │  7. install stop callback                               │
                   └─────────────────────────────────────────────────────────┘
                                       │
                   ┌───────────────────┴─────────────────┐
                   ▼                                     ▼
          __pending_ non-empty                  __pending_ empty
          pop front → set_next                  arm IORING_OP_POLL_ADD
                   │                                     │
                   ▼                                     ▼
          downstream completes                 reactor: POLL CQE
          via set_value                         → drain udev_monitor_receive_device
          → drain_or_poll                          in a loop into __pending_
                                                  → drain_or_poll
```

`__op` owns:
- `udev*` and `udev_monitor*` (`std::unique_ptr` with `udev_unref` /
  `udev_monitor_unref` deleters)
- `__pending_`: `std::deque<device_event>` feeding both the initial
  replay and the live POLL drain
- in-flight `__poll_op_` / `__cancel_op_` / `__finalize_op_`
  (`std::optional<__io_task_facade<...>>`)
- `__next_op_` (currently in flight downstream child op)
- `__stop_cb_`
- `__pending_cqes_` counter (POLL + optional CANCEL + NOP), checked
  for zero on the NOP CQE before driving the unique completion path

`udev_context` owns only `std::atomic<__op_base*> __active_` —
matches `dax` / `velx` / `inx` / `fsx` / `rdcx::pool`.

## Initial replay

On `subscribe`:

1. `udev_monitor_enable_receiving` runs **before**
   `udev_enumerate_scan_devices` so any device that appears in the
   gap between the two is queued by the monitor (it would be missed
   if the order were reversed).
2. `udev_enumerate_scan_devices` returns currently-present
   subsystem-matching devices; the wrapper synthesizes one
   `device_event{kind = add}` per device into `__pending_`.
3. devtype is filtered explicitly during enumeration because
   `udev_enumerate_add_match_subsystem` does not also filter on
   devtype — without this, initial replay would include partitions
   even when the live filter excluded them.

**Caveat — duplicates.** A device that appears in the gap window
will surface twice (once from enumerate, once from monitor). The
wrapper does not dedupe because there is no universal device key
(syspath is only unique within `/sys`, but USB / network device
identity uses different keys). Caller should dedupe in
`transform_each` against whichever key matters for its application.
This is the same posture as `velx`'s `__seen_arrivals_` — except
`velx` has a universal key (`\\?\Volume{guid}`) and dedupes
in-wrapper. Linux does not.

## Backpressure

Continuation-style (matches `inotify_wrapper.hpp`): each
`device_event` flows downstream via `set_next`; the next event is
either popped from `__pending_` or comes from the next POLL_ADD,
**both gated on `next_receiver::set_value`**. While the downstream
is processing, no new POLL is in flight; libudev's internal buffer
queues incoming netlink messages.

Limit: libudev's monitor buffer defaults to `SO_RCVBUF` of the
netlink socket (kernel default `/proc/sys/net/core/rmem_default`,
typically 200–500 KiB on modern Linux). Sustained slow consumer
will eventually trigger netlink message drops; libudev signals this
on the next `udev_monitor_receive_device` call by returning NULL
without an EAGAIN underlying — but observable from userspace only
as gaps in the event stream. Hot-plug rates are bounded by physics,
so a stall this severe usually means the consumer is wedged.

If a downstream operation does **not** propagate `stop_token`, the
wrapper can stall in `__pending_` drain forever waiting for
`set_value`. Same gotcha as inotify / dax / velx.

## Cancellation

```
upstream stop_token ──► __on_stop_fn ──► __stop_requested_ = true
                                          │
                                          ▼
                              if poll in flight: submit IORING_OP_ASYNC_CANCEL
                                          │
                                          ▼
                              POLL CQE arrives with res = -ECANCELED
                                          │
                                          ▼
                              __request_finalize(stopped) → IORING_OP_NOP
                                          │
                                          ▼
                              NOP CQE on reactor frame:
                                __finalize_and_complete
                                  drop stop_cb / next_op / poll_op /
                                  cancel_op / finalize_op
                                  release udev_monitor_unref + udev_unref
                                  release __active_
                                  set_stopped(rcvr)
```

All "want to finish" triggers funnel through one
`__request_finalize`, CAS-gated on `__finalize_scheduled_`:
- Upstream `stop_token`
- POLL CQE with negative res (`-ECANCELED` or other errno)
- POLL CQE reporting `POLLERR | POLLHUP`
- Downstream `next_receiver::set_stopped` / `set_error`
- Exception during `connect` / `start` of the next sender
- Exception during the `udev_monitor_receive_device` drain loop

The unique safe site for `__finalize_and_complete` is the NOP CQE
handler — same trampoline trick as the inotify wrapper. By that
point any nested `set_next` chain has unwound, the POLL CQE's debt
is settled, and (if a cancel was submitted) the cancel CQE has
been observed.

## libudev / kernel quirks worth knowing

| Quirk | Detail |
|---|---|
| **Monitor source choice: `"udev"` not `"kernel"`** | `udev_monitor_new_from_netlink(udev, "udev")` sees events **after** systemd-udev rule processing — the same view that udisks2 / GNOME Disks / etc. see. `"kernel"` would surface raw uevents, including transient ones the rule processor would normalize. The wrapper hardcodes `"udev"` because alignment with desktop daemons matters more than seeing every kernel-side uevent. |
| **`enable_receiving` must come before `enumerate`** | If you reverse the order, devices that appear in the gap window are lost. The wrapper enables receiving in `__setup_udev` and only then runs the enumerate. |
| **Initial replay can duplicate live `add` events** | Documented above. Caller dedupes if needed; wrapper does not. |
| **`udev_monitor_receive_device` returns NULL non-fatally** | NULL means EAGAIN OR the message was filtered out by libudev's internal subsystem/devtype filter. The wrapper treats both identically: stop draining, re-arm POLL. |
| **`POLLERR` / `POLLHUP` on the netlink fd is fatal** | The netlink socket has been torn down (process namespace going away, etc.). The wrapper completes with `set_error`. |
| **Buffer sizing** | Default `SO_RCVBUF` on the libudev monitor netlink socket is the kernel default. To raise it for noisy environments, use `udev_monitor_set_receive_buffer_size` after `_new` — this wrapper does not expose it in v1 because device hot-plug rates are low; add a `watch_options::receive_buffer_size` field if you observe drops. |
| **`unknown` event kind is real** | `udev_device_get_action` can return strings the wrapper does not enumerate (e.g. future kernel additions). The wrapper surfaces these as `device_kind::unknown` rather than dropping; the original action string is recoverable from the underlying `udev_device*` (not exposed in v1). |
| **Cleanup must not race in-flight CQEs** | Same NOP-trampoline rule as the inotify wrapper. Tearing down `udev_monitor_unref` while a POLL CQE is mid-flight invalidates the fd; the wrapper waits for cancel + nop CQE before reset. |

## Things deliberately NOT done

- **Approval policy.** Structurally absent (see "Why no approval
  surface" above). The README and design doc enumerate the three
  rejected pseudo-approval paths so the next person who has the
  same idea can find the rationale.
- **Multi-subsystem filter** in `watch_options`.
  `udev_monitor_filter_add_match_subsystem_devtype` can be called
  multiple times to OR them, but a Linux developer who genuinely
  needs to monitor block + USB simultaneously is in unusual
  territory. v1 keeps one subsystem per `udev_context`; for two,
  open two contexts. Add a `std::vector<filter>` field if a real
  use case shows up.
- **Tag filter / sysattr filter.**
  `udev_monitor_filter_add_match_tag` (since udev 154) lets you
  filter on tags applied by udev rules. Useful only if you ship
  your own udev rules. Out of scope.
- **`"kernel"` source.** See the quirks table — the wrapper aligns
  with udisks2 / desktop daemons by using `"udev"`. If you genuinely
  need raw kernel uevents (rare), copy the wrapper and change the
  one string.
- **Wrapper-side dedup of initial replay vs live arrival.** No
  universal device key on Linux. Caller's responsibility.
- **`udisksx::` D-Bus wrapper.** That is the right home for
  approval-flavored Linux storage API; lives outside this wrapper's
  scope, on a different event source (D-Bus signals).
- **Multi-subscriber fan-out** on a single `udev_context`.
  `__active_` is single-shot CAS-guarded; matches DA / velx /
  inotify / fsevents / rdc::pool. Build a layer on top.
- **Structural unit tests under `test/exec/`.** Same posture as
  `da_wrapper` / `velx_wrapper`: the demo plus visual inspection
  on a real Linux box stands in for runtime coverage; static-only
  shape checks may follow.
