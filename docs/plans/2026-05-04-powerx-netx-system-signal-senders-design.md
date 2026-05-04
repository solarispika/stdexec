# powerx + netx system-signal sequence senders — design

Status: design (not yet implemented)
Target:
- `examples/powerx_wrapper.hpp` + `examples/powerx.cpp` + `examples/powerx_README.md`
- `examples/netx_wrapper.hpp` + `examples/netx.cpp` + `examples/netx_README.md`

## Motivation

OrangeDrive 客戶端目前手刻了兩個跨平台「系統訊號訂閱」抽象：

- **`PowerDetector`**（`include/detector/power-detector.h`、`lib/detector/{mac,windows}/power-detector.{h,cpp}`）：suspend / resume 通知。
- **`NetworkDetector`**（`include/detector/network-detector.h`、`lib/detector/{mac,windows}/network-detector.{h,cpp}`）：網路介面變更通知。

兩者形狀幾乎相同（callback vector + 自管 CFRunLoop / 隱藏 HWND + `std::jthread` + condvar），痛點也相同（callback 同步在 OS thread 上執行、callback vector 註冊有 mutex 但 invocation 沒有、lifetime 由 detector 自己用 `m_runloop` / `m_handle` nullness 表達），跟我們在 dax / velx 處理掉的問題一模一樣。

powerx + netx 把這兩個 detector 收斂到 platform-event-senders 家族裡，重用 fsx / dax / velx 已驗證的設計：dispatch queue / thread pool scheduler 注入、CAS-guarded single active subscription、structured cancellation。

跨平台不變量同 dax / velx / udx：
- `exec::sequence_sender_t`
- 單一 active subscription per context（CAS-guarded）
- `exec::sequence_with_scheduler` env 注入，compile-time 拒絕錯誤 scheduler
- 原生 cancellation 經 stop callback 路由
- 一個 `__op` 持有資源全 lifecycle，cleanup 與 in-flight callback 不能 race

## 為什麼是兩個 wrapper、不是一個

Power 事件（suspend/resume）跟 Network 事件（interface change）來自完全不同的 OS API，event shape 也不同。把它們合併會出現 union event type 跟 「watch_power=true / watch_network=true」的奇怪 option，反而難用。

兩個 wrapper 共用同一份設計骨架（同 scheduler、同 sequence_sender contract、同 cancellation 路由），但對外是兩個 context type、兩個 event struct。pipeline 層要組合的時候用 `exec::when_any` 或讓 user code 各跑一條 pipeline。

| | macOS | Windows | Linux |
|---|---|---|---|
| `powerx` source | `IORegisterForSystemPower` + `IONotificationPortSetDispatchQueue` | `RegisterSuspendResumeNotification(DEVICE_NOTIFY_CALLBACK)` | **out of scope v1**（systemd-logind D-Bus / sd_login PrepareForSleep） |
| `netx` source | `SCDynamicStoreCreate` + `SCDynamicStoreSetDispatchQueue` | `NotifyIpInterfaceChange` | **out of scope v1**（rtnetlink RTM_NEWLINK / NetworkManager D-Bus） |
| Scheduler | `exec::libdispatch_queue` | `exec::windows_thread_pool` | n/a |

OrangeDrive 目前也只有 mac + windows 兩支實作，所以 v1 對齊現況、不做 Linux。Linux 出口（rtnetlink / D-Bus）形狀跟 udx 比較像，未來作 `linuxx::power` / `linuxx::net` 另一個 wrapper。

## Decisions

### 1. macOS 不再自管 CFRunLoop — 改用 dispatch queue 直送

OrangeDrive 現況：兩個 detector 各自 spawn `std::jthread`，在 thread 裡 `CFRunLoopGetCurrent()` + `CFRunLoopAddSource()` + `CFRunLoopRun()`，stop 時用 `CFRunLoopStop()` + condvar 等 thread 退出（`lib/detector/mac/power-detector.cpp:111-147`、`lib/detector/mac/network-detector.cpp:110-153`）。

兩個 IOKit / SystemConfiguration API 都支援把 notification port 直接綁到 dispatch queue：

- `IONotificationPortSetDispatchQueue(port, queue)`（取代 `IONotificationPortGetRunLoopSource` + `CFRunLoopAddSource`）
- `SCDynamicStoreSetDispatchQueue(store, queue)`（取代 `SCDynamicStoreCreateRunLoopSource` + `CFRunLoopAddSource`）

切過去之後：
- 不需要自管 thread / runloop / condvar。
- callback 直接在 wrapper 配置的 serial dispatch queue 上跑，模式跟 fsx / dax 完全一致（per-op 私有 serial queue + user queue 當 target，per memory `libdispatch_sequence_sender_pattern`）。
- teardown 透過 `dispatch_async_f` 反彈到自己 queue，跟 in-flight callback 不會 race。

### 2. Windows 不再開隱藏 HWND — 改用 callback subscription

OrangeDrive 現況：`WindowsPowerDetector` 為了收 `WM_POWERBROADCAST`，整套刻 `RegisterClassExA` + `CreateWindowExA` + 自家 thread 跑 `GetMessage` 迴圈（`lib/detector/windows/power-detector.cpp:194-298`）。

從 Windows 8 起，`RegisterSuspendResumeNotification` 跟 `RegisterPowerSettingNotification` 接受 `DEVICE_NOTIFY_CALLBACK` flavour，傳一個 `DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS` 進去就好；callback 在系統 thread 上跑，不需要 HWND、不需要 message pump、不需要 window class registration。

`netx` 已經是 callback API（`NotifyIpInterfaceChange`，`PIPINTERFACE_CHANGE_CALLBACK`），不需要改設計，只是把現況的 `m_thread` + `WaitForSingleObject(shutdownEvent)` 那段卸掉，因為 callback subscription 本身就持有 lifetime，shutdown 直接 `CancelMibChangeNotify2`。

兩個 wrapper 都用 velx 同款 `exec::windows_thread_pool` MPSC queue + drainer 模式：OS callback push 進 lock-free queue，drainer work item 在 user pool 上 drain。

### 3. Item shape（與 dax / velx 對齊：per-event delivery）

```cpp
namespace powerx
{
  enum class power_event_kind
  {
    suspend,         // macOS: kIOMessageSystemWillSleep
                     // Windows: PBT_APMSUSPEND
    resume,          // macOS: kIOMessageSystemHasPoweredOn
                     // Windows: PBT_APMRESUMEAUTOMATIC
  };

  struct power_event
  {
    power_event_kind kind;
    // v1 故意不帶任何 payload。reason / battery state / display state 等
    // 屬於 future work（見 §6）。
  };
}

namespace netx
{
  struct interface_change_event
  {
    // v1：訊號通知，不帶 diff。語意上是「interface table 可能變了，請重查」。
    // 對齊 OrangeDrive 現況（PowerDetector 的 callback 是 void()）跟
    // SCDynamicStore / NotifyIpInterfaceChange 的 hint-only 性質。
  };
}
```

兩個 wrapper 都不批次交付 — 事件率低（人類插拔網路線、按下蓋子）。`netx` 的 debounce / checksum 比較邏輯（OrangeDrive Windows 端有：`lib/detector/windows/network-detector.cpp:42-119`）**不**進 wrapper layer；那是 application policy，留給下游 `transform_each(then(...))`。

### 4. `watch_options{}` 最小集

```cpp
namespace powerx {
  struct watch_options {
    bool watch_suspend = true;
    bool watch_resume  = true;
  };
}

namespace netx {
  struct watch_options {
    // v1：什麼都沒有。Windows 的 OrangeDrive 邏輯有「skip BeeDriveTap / BeeDriveTun
    // 假介面」這種 application-level filter，是業務邏輯，不該進 wrapper。
    //
    // address_family 過濾未來可加（IPv4 only / IPv6 only / both），但目前
    // SCDynamicStore 跟 NotifyIpInterfaceChange 都用 AF_UNSPEC，沒理由 v1 開洞。
  };
}
```

### 5. **不**做 approval surface — 結構性決定

#### powerx：suspend approval 是真的可以做、但 v1 不做

macOS 有 `kIOMessageCanSystemSleep`：app 可以 `IOCancelPowerChange` veto 進入 sleep（30 秒內）。Windows 的對應品 `PBT_APMQUERYSUSPEND` 自 Vista 起已被 deprecate，現代 Windows 不允許 app veto suspend。

OrangeDrive 現況：**沒有用 veto path**。`SystemPowerEventCallback`（`lib/detector/mac/power-detector.cpp:70-94`）只訂閱 `kIOMessageSystemWillSleep`（已決定要 sleep 的事後通知，不可 veto），且自動 `IOAllowPowerChange`。沒有業務需求要 veto。

powerx v1 對齊現況：只給 informational suspend / resume，不給 approval。這跟 udx 的「沒有 approval surface」是不同性質的決定（udx 是 OS 介面契約上不存在；powerx 是 macOS 可以做、Windows 不能做、目前無人需要）。**README 要明寫**這條是 v1 決定，未來如果有需求，approval 出口長這樣：

```cpp
// 未來擴充示意
opts.system_will_sleep_approval = approval::bounded<powerx::sleep_request>{
  .predicate = [](sleep_request const&, stdexec::inplace_stop_token tok) {
    return flush_caches_within(tok);
  },
  .timeout          = std::chrono::seconds{20}, // < kIOMessageCanSystemSleep 30s window
  .on_timeout_allow = true,
};
// Windows 端會編成 no-op（`if constexpr` 平台分支），語意上對齊 udx 的設計。
```

#### netx：沒有 approval

兩個 OS 的 network change API 都是廣播語意（事後通知），無 veto path。同 udx，這是介面契約決定。

### 6. Cancellation 路由

- macOS：stop callback fire → `dispatch_async` 到內部 serial queue → `IONotificationPortSetDispatchQueue(port, NULL)` / `SCDynamicStoreSetDispatchQueue(store, NULL)` → `IODeregisterForSystemPower` / 釋放 `SCDynamicStoreRef` → 在 queue 上 `set_stopped`。同 dax pattern。
- Windows：stop callback fire → push cancel sentinel 進 MPSC queue → drainer work item 看到 sentinel → `UnregisterSuspendResumeNotification` / `CancelMibChangeNotify2` → drainer 跑 final `set_stopped`。同 velx pattern。

OrangeDrive 現況的 `m_cv.wait(lock, [this] { return m_runloop == nullptr; })` 那種「Stop 同步等 runloop 結束」的形狀整個拿掉，receiver 收到 `set_stopped` 就是 lifecycle 結束的單一 source of truth。

### 7. 不暴露 `IOAllowPowerChange` 機制給使用者

macOS suspend 流程在收到 `kIOMessageSystemWillSleep` 後**必須**呼叫 `IOAllowPowerChange`（或 `IOCancelPowerChange`）才不會卡住系統 sleep（30 秒 timeout）。OrangeDrive 現況是 callback 跑完後**同步**呼叫 `IOAllowPowerChange`（`power-detector.cpp:87`），這隱含一個假設：所有 callback 都會即時返回。

powerx 的契約：wrapper 在 deliver `power_event{suspend}` 給 receiver **之前**就 `IOAllowPowerChange`，receiver 收到時 sleep 已經被允許、不可逆。這跟 udx 一樣是「事後通知」語意，不暴露 acknowledge API。如果未來真的要做 approval，那條路會走 `kIOMessageCanSystemSleep`（不是 `kIOMessageSystemWillSleep`）+ `approval::bounded`，wrapper 會在 predicate 結果出來後選 `IOAllowPowerChange` 或 `IOCancelPowerChange`。兩條路是分開的。

## API 預覽

```cpp
// powerx
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("oranged.power");
powerx::context ctx;

stdexec::sync_wait(
    exec::sequence_with_scheduler(pool.get_scheduler(),
                                  ctx.watch({.watch_suspend = true,
                                             .watch_resume  = true}))
  | exec::transform_each(stdexec::then([](powerx::power_event e){
      switch (e.kind) {
        case powerx::power_event_kind::suspend: pause_sync(); break;
        case powerx::power_event_kind::resume:  resume_sync(); break;
      }
    }))
  | exec::ignore_all_values());
```

```cpp
// netx
exec::libdispatch_queue pool = exec::libdispatch_queue::make_concurrent("oranged.net");
netx::context ctx;

stdexec::sync_wait(
    exec::sequence_with_scheduler(pool.get_scheduler(), ctx.watch({}))
  | exec::transform_each(stdexec::then([](netx::interface_change_event){
      poke_connection_manager();
    }))
  | exec::ignore_all_values());
```

OrangeDrive 端原本「`NetworkDetector` 在 ctor 裡 `m_power_detector.AddCustomCallback(...)` 來 auto-Stop on suspend / auto-Start on resume」（`lib/detector/network-detector.cpp:33-43`）會被改寫成 pipeline 組合：

```cpp
// 現況：耦合在 ctor，順序由 Mac/WindowsNetworkDetector 隱含
// 改寫後：在 application 層顯式組合
auto net_subscription_lifetime =
    powerx_ctx.watch({.watch_resume = true})
  | exec::transform_each(stdexec::then([&](powerx::power_event){
      // resume 之後重新訂閱 net；suspend 期間 stop_token 已 fire
    }));
```

實際 application-side 怎麼長視 OrangeDrive 那邊重構時的決定，wrapper 不強制特定組合方式。

## 為何不直接在 OrangeDrive 內部把 PowerDetector / NetworkDetector 抽成 sender

兩個原因：

1. **這個家族的 wrapper 本來就是 stdexec example 的價值定位** — 跟 fsx/dax/velx/udx/inx 一樣，是「示範如何把 callback-driven OS API 包成 sequence_sender」的標本。把它們收齊有 portfolio 完整性的價值。
2. **OrangeDrive 是 downstream consumer** — wrapper 在 stdexec 這邊長出來、穩定後，OrangeDrive 端做 detector 的 sender 化重構，引用 `examples/powerx_wrapper.hpp` 跟 `examples/netx_wrapper.hpp`。如果要 production-grade，再 fork 出 `synodrive::powerx::` 命名空間版本，按 OrangeDrive coding style 調整 — 但結構性決策已經在 stdexec 這邊驗證過。

## 範圍外（v1 不做）

- **Linux 實作**：systemd-logind 的 `PrepareForSleep` D-Bus signal、rtnetlink `RTM_NEWLINK/NEWADDR`。形狀跟 udx 比較接近（io_uring + netlink），未來作 `linuxx::power` / `linuxx::net`。
- **macOS approval path**（`kIOMessageCanSystemSleep` + `approval::bounded`）：見 §5。
- **更細的 power 事件**：`battery_low`、`display_state_changed`、`thermal_pressure_changed`。架構支援，但 OrangeDrive 沒用，不進 v1。
- **netx event payload**：interface diff、address list、specific interface up/down。OrangeDrive 現況也只用 hint-only 訊號，v1 對齊。
- **Multi-subscription per context**：同 dax / velx，每個 `context` 一個 active watch。要監看不同子集就開多個 context。

## Validation plan

兩個 wrapper 都要：

1. **手動冒煙測試**：
   - powerx mac：`pmset sleepnow` 觸發 suspend；闔筆電蓋；外接電源拔插（如果有訂閱 power source）。
   - powerx windows：`rundll32.exe powrprof.dll,SetSuspendState 0,1,0` 或開始選單 → 睡眠。
   - netx mac：`networksetup -setairportpower en0 off / on`、拔網路線。
   - netx windows：`netsh interface set interface "Ethernet" admin=disabled / enabled`。

2. **生命週期測試**（同 dax / velx 既有測試框架）：
   - 起 / 停 watch 100 次無 leak、無 use-after-free（ASAN）。
   - watch 中途 `request_stop()`：必須 deliver `set_stopped` 而非 `set_value`。
   - context 析構時 active subscription 已 stop（CAS slot 為 null）。

3. **平台行為差異 callout**：在 README 寫「macOS 收得到 `kIOMessageDeviceSignaledWakeup`，Windows 看不到」這類事實，避免使用者依賴 wrapper 抽象不到的平台特異性。

## Build gating

- powerx：mac + windows 平台分支；Linux build 為空 stub（編 `static_assert` 讓 user 看到「v1 not implemented on linux」訊息），跟 dax 在 non-mac 的處理對齊。
- netx：同上。
- CMake：`example.powerx` / `example.netx` target 只在對應平台 configure（同 `example.da` / `example.velx` 既有 pattern）。
