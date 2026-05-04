# udx × libudev × io_uring sequence sender — design

Status: design (not yet implemented)
Target: `examples/udx_wrapper.hpp` + `examples/udx.cpp` + `examples/udx_README.md`

## Motivation

填補 platform-event-senders 家族的最後一格。設備層三平台對照：

| | macOS (`dax`) | Windows (`velx`) | Linux (`udx`，本設計) |
|---|---|---|---|
| Reactor scheduler | `exec::libdispatch_queue` | `exec::windows_thread_pool` | `exec::io_uring_context` |
| Source primitive | `DARegisterDisk*Callback` | `CM_Register_Notification` (`GUID_DEVINTERFACE_VOLUME`) | `udev_monitor_new_from_netlink("udev")` + `IORING_OP_POLL_ADD` |
| Initial replay | DA daemon 自帶 | `CM_Get_Device_Interface_List_PRESENT` 合成 | `udev_enumerate_scan_devices` 合成 |
| Item shape | per-event `disk_event` | per-event `volume_event` | per-event `device_event` |
| Approval | mount/unmount/eject | query-remove | **無**（kernel uevent 廣播語意，無 veto path） |

跨平台不變量保持五個 wrapper 一致：
- `exec::sequence_sender_t`
- 單一 active subscription per context（CAS-guarded）
- `exec::sequence_with_scheduler` env 注入，compile-time 拒絕錯誤 scheduler
- 原生 cancellation 經 stop callback 路由
- 一個 `__op` 持有資源全 lifecycle，cleanup 與 in-flight callback 不能 race

## Decisions

### 1. 同樣的四個 invariant，跟 dax / velx / inx / fsx / rdcx 對齊

- 單一 active subscription per `udev_context`（`std::atomic<__op_base*> __active_` CAS slot）
- 不擁有 `io_uring_context`；scheduler 由使用者提供，透過 `exec::sequence_with_scheduler` 注入 receiver env
- `__op` 持有：`udev*`、`udev_monitor*`、stop callback、in-flight `__poll_op_` / `__cancel_op_` / `__finalize_op_`、pending queue、next op
- Cancellation 透過 `IORING_OP_ASYNC_CANCEL`，cleanup 透過 `IORING_OP_NOP` 反彈到 reactor frame（與 inotify 一致）

### 2. Scope：設備層，觀察 only

- 預設 subsystem 是 `"block"`，符合 dax/velx 的「儲存裝置 hot-plug」slot 語意
- v1 不支援 multi-subsystem。改 subsystem 等於開新訂閱；如果要監看 USB + block，請開兩個 `udev_context`
- 不暴露 approval policy（語意上不存在，見 §3）

### 3. **不**做 approval — 結構性而非懶惰

`libudev` 跟 kernel uevent 是 **broadcast-only**：當 userspace 收到 `add` / `remove` 時，kernel 已經完成動作。這不是設計取捨，是 OS 介面契約。

被檢視且**拒絕**的三條 pseudo-approval 路徑：

| 路徑 | 為何不採用 |
|---|---|
| **動態產生 udev rule + RUN+= IPC 回 wrapper** | (a) `RUN+=` 對 `add` 跑時 device 已加入；對 `remove` 跑時 device 已移除。沒有 veto 語意，最多延遲 udisks2 auto-mount。 (b) 需要 root 寫 `/etc/udev/rules.d/`，跟 user-space library 形狀錯位。 (c) wrapper crash 會洩漏持久化 system state，stale rule 卡住全機 udev 30s timeout。 (d) systemd-udev `RUN+=` 文件明文「不可長進程」。(e) 多 process race，rule 互相覆蓋。 |
| **同步 `pre_deliver` hook（先呼叫 user predicate 再 set_next）** | YAGNI。`transform_each(then(...))` 已經能跑 user code，差不到一個函數呼叫。命名也誤導使用者以為有 approval 語意。 |
| **包進 udisks2 D-Bus** | 是正確的 approval-flavored 出口（user-initiated mount/unmount path），但事件源跟 lifetime 跟 udev netlink 完全不同。屬於另一個 wrapper（`udisksx::`，未來工作），不在 `udx` scope 內。 |

README 必須把這節用「Why no approval surface」明寫，避免使用者期望落空。

### 4. Item shape：single `device_event` per delivery（DA/velx 對齊）

```cpp
struct device_event {
  enum class kind_t { add, remove, change, online, offline, bind, unbind, move, unknown };
  kind_t                                            kind;
  std::string                                       subsystem;   // e.g. "block"
  std::string                                       devtype;     // e.g. "disk", "partition"，可能空字串
  std::string                                       sysname;     // e.g. "sda1"
  std::optional<std::string>                        devnode;     // e.g. "/dev/sda1"，無 node 的設備 nullopt
  std::optional<std::string>                        syspath;     // e.g. "/sys/devices/.../sda/sda1"
  std::vector<std::pair<std::string, std::string>>  properties;  // opt-in，watch_options::want_properties
};
```

不採用「批次交付（`device_batch`）」雖然 io_uring 完成後可能有多個 device 可 drain — 一次 POLL_ADD 完成可能對應多個排隊的 netlink message。內部仍 drain 進 pending queue，但**對下游一次只交付一個** `device_event`，跟 dax/velx 形狀一致。設備層事件率本來就低（人類插拔速率），佇列深度通常個位數。

### 5. `watch_options{}` v1 的最小集

```cpp
struct watch_options {
  std::string                subsystem       = "block";   // udev_monitor_filter
  std::optional<std::string> devtype         = std::nullopt; // 不過濾 = 同時看 disk + partition
  bool                       initial_replay  = true;
  bool                       want_properties = false;     // 是否 populate device_event::properties
  std::vector<std::string>   property_keys;               // empty + want_properties=true → 全收
};
```

不放進 v1：
- 多 subsystem filter（v1 一個就夠，符合 dax/velx 對齊）
- Tag filter（`udev_monitor_filter_add_match_tag`）
- Custom monitor source（`"kernel"` vs `"udev"`：`"kernel"` 看的是 raw uevent，繞過 udev rule processing；wrapper 統一走 `"udev"` 跟 udisks2 等 daemon 看到一致的事件流）

### 6. 初始 replay 透過 `udev_enumerate_scan_devices`

`subscribe → enumerate → register monitor` 是錯的（gap window 漏事件）。
正確順序：
```
1. udev_monitor_new_from_netlink + filter_add_match + enable_receiving
2. udev_enumerate_new + add_match_subsystem + scan_devices
3. for each enumerated dev: synthesize add 推入 pending
4. drain pending → 後續 POLL_ADD live events
```

step 1 早於 step 2 確保「register 跟 enumerate 之間出現的新 device」會在 monitor 隊列裡而不是丟失。可能造成 duplicate（enumerate 跟 monitor 都拿到同一顆），由 caller 用 `transform_each` 自行去重 — wrapper 不暗自去重，因為 wrapper 不知道 caller 的 dedup key（sysname? devnode? syspath UUID?）。

velx 同樣的 ordering 問題用 `__seen_arrivals_` set 去重；那邊去重 key 是 `\\?\Volume{guid}` symbolic link，唯一。Linux 沒有同等普世的 device key（block device 用 syspath，但 USB device 用 sysattr，不一致），所以選擇不在 wrapper 內去重。

### 7. Reactor：`IORING_OP_POLL_ADD` on monitor netlink fd

不用 `IORING_OP_READ`。理由：
- libudev 自己 parse netlink message（CMSG ucred 檢查、subsystem filter、property dictionary build）。我們重做這些只是為了一行 syscall economy。
- POLL_ADD 是 level-triggered POLLIN，下一輪 POLL 會在還有未讀資料時立刻 fire。不需要 multishot。
- 每次 POLL CQE 後 loop `udev_monitor_receive_device` 直到 NULL（EAGAIN 或 filter 駁回），把當前可讀的全部 drain 到 pending。

```
on_poll_complete(cqe):
  loop:
    dev = udev_monitor_receive_device(mon)
    if !dev: break                                        # EAGAIN 或 filter 駁回
    pending.push_back(build_event(dev))
    udev_device_unref(dev)
  drain_or_poll()
```

### 8. Drainer 狀態機：pending queue 優先，否則 arm POLL

```
drain_or_poll():
  if pending.empty():
    arm_poll()                                            # IORING_OP_POLL_ADD
  else:
    event = pop_front(pending)
    set_next(rcvr, just(event))                           # 繼續 set_value 後再次 drain_or_poll
```

`pending` 同時容納：
- `start()` 時 enumerate 出來的初始 add 事件
- 每次 POLL CQE drain 出來的 live 事件

單一 deque 統一來源簡化狀態機。Memory 上限：一次 POLL_ADD 之間 kernel netlink 隊列累積的 events 量，加上初始 enumerate 全機 block device 數，個位數到二位數。

### 9. `start()` ordering（與 inotify 一致）

1. CAS 進駐 `__active_` slot（失敗 → `set_error`）
2. 建 `udev*` + `udev_monitor*` + filter + `enable_receiving`
3. enumerate 推入 `pending`
4. `drain_or_poll()`（馬上會交付第一個初始 event 或直接 arm POLL）
5. **最後**註冊 stop callback（避免 token 已 stop 時同步 fire 到尚未 ready 的 op）

### 10. Cleanup 透過 NOP trampoline

跟 inotify 一樣的 `IORING_OP_NOP` 反彈，確保 `__finalize_and_complete` 跑在 reactor 自己的 frame，不在 nested set_next chain 或 set_value callback 中。`__pending_cqes_` counter 統計 in-flight POLL/CANCEL/NOP CQE，全部歸零才 finalize。

Cleanup 序列：
```
drop stop_cb
udev_monitor_unref
udev_unref
release __active_
__rcvr_.set_stopped() / set_value() / set_error(__error_)
```

`udev_monitor_unref` 自動 close monitor fd，所以不必 explicit close。

## Public API

```cpp
namespace udx {
  enum class device_kind { add, remove, change, online, offline, bind, unbind, move, unknown };

  struct device_event {
    device_kind                                       kind;
    std::string                                       subsystem;
    std::string                                       devtype;
    std::string                                       sysname;
    std::optional<std::string>                        devnode;
    std::optional<std::string>                        syspath;
    std::vector<std::pair<std::string, std::string>>  properties;
  };

  struct watch_options {
    std::string                subsystem       = "block";
    std::optional<std::string> devtype         = std::nullopt;
    bool                       initial_replay  = true;
    bool                       want_properties = false;
    std::vector<std::string>   property_keys;
  };

  class udev_context {
   public:
    udev_context();
    ~udev_context();
    udev_context(udev_context const&)                    = delete;
    auto operator=(udev_context const&) -> udev_context& = delete;

    auto watch(watch_options opts = {}) -> __detail::__watch_sender;

   private:
    std::atomic<__detail::__op_base*> __active_{nullptr};
  };
}
```

使用：

```cpp
exec::io_uring_context ring;
std::thread driver{[&]{ ring.run_until_stopped(); }};

udx::udev_context ctx;
sync_wait(
    exec::sequence_with_scheduler(ring.get_scheduler(),
        ctx.watch({.subsystem = "block", .initial_replay = true}))
  | exec::transform_each(stdexec::then([](udx::device_event e) {
      std::printf("[%s] %s/%s %s\n",
                  to_str(e.kind), e.subsystem.c_str(),
                  e.devtype.c_str(), e.sysname.c_str());
    }))
  | exec::ignore_all_values());

ring.request_stop();
driver.join();
```

## Error handling

| 失敗點 | 行為 |
|---|---|
| `udev_new` 回 NULL | `set_error(make_exception_ptr(runtime_error))` 立即返回 |
| `udev_monitor_new_from_netlink` 回 NULL | 同上 |
| `udev_monitor_filter_add_match_subsystem_devtype` < 0 | 同上，附 errno 訊息 |
| `udev_monitor_enable_receiving` < 0 | 同上 |
| `udev_enumerate_scan_devices` < 0 | 視 `initial_replay` 而定：true → set_error；false → 不 reach |
| POLL_ADD CQE 回 -EINVAL / -ENOMEM | `set_error` 經 cleanup |
| POLL_ADD CQE 回 -ECANCELED | 正常取消路徑，cleanup 走 stopped |
| `udev_monitor_receive_device` 回 NULL（非 EAGAIN） | 視作該次 POLL drain 完，繼續 arm 下一次 POLL（libudev internal filter 把不符合的 device 過濾，回 NULL 是合法的） |
| `__active_` CAS 失敗 | `set_error("udev_context already has an active watch")` |
| Receiver 自己 set_error / set_stopped | cleanup 走對應 finish_kind |

## Testing strategy

### Demo (`examples/udx.cpp`)

30 秒觀察，搭 `when_any` + 計時器 stop。在 dev box 上用 loopback device (`losetup`) 觸發 add/remove：

```bash
sudo losetup -f /tmp/udx_test.img    # add 事件
sudo losetup -d /dev/loop7           # remove 事件
```

或者 `/dev/loop-control` 配 `LOOP_CTL_GET_FREE` ioctl 在 demo 內部跑（避免要 sudo）— 但 `losetup` 後續仍要 root，所以 README 直接寫「在 dev box 上手動跑這兩條指令觀察輸出」。

### Structural tests（沿用 dax/velx 路線）

`test/exec/test_udx_wrapper.cpp`：
- `udev_context` 預設可建構、move 不可、copy 不可
- `watch()` 回傳的東西滿足 `exec::sequence_sender_to<...>`
- compile-time scheduler enforcement（用 `inline_scheduler` 的 receiver 應該編譯失敗 — `static_assert` test）
- 第二次 `start` 立即 `set_error`（`__active_` CAS）

不寫 runtime 整合測試（需要 udev daemon + sudo 觸發 device events），保持與 dax/velx 同樣 posture。

## Out of scope (followups)

| 項目 | 為何延後 |
|---|---|
| Multi-subsystem filter | YAGNI；多開一個 `udev_context` 即可 |
| `udisksx::` D-Bus wrapper | approval-flavored API 的正確出口，但事件源不同（D-Bus vs netlink），lifetime 不同，獨立 wrapper |
| Tag filter / sysattr filter | v1 沒人需要 |
| `"kernel"` source（raw uevent） | 跟 udisks2 等 daemon 看到的事件流不一致，違背「跟現代 Linux desktop 一致」的預期 |
| Multi-subscriber fan-out | 跟 dax/velx/inotify 一樣，build a layer on top |
| 動態加 / 移 subsystem filter | 改 subsystem 等於改訂閱；重新 `watch()` |
