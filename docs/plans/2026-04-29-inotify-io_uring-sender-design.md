# inotify × io_uring sequence sender — design

Status: design (not yet implemented)
Target: `examples/inotify_wrapper.hpp` + `examples/inotify.cpp` + `examples/inotify_README.md`

## Goal

Linux 對應 `examples/fsevents_wrapper.hpp` (macOS) 和 `examples/rdc_pool_wrapper.hpp`
(Windows) 的「不擁有 thread、user-supplied scheduler、continuation-style
backpressure」形狀 —— 把 inotify 包成 `exec::sequence_sender_t`，IO 完成走 stdexec
內建的 `exec::io_uring_context`。

形狀對稱：

| 平台 | reactor scheduler | env adapter | source primitive |
|---|---|---|---|
| macOS | `exec::libdispatch_queue` | `fsx::on_queue` | `FSEventStreamCreate` + `FSEventStreamSetDispatchQueue` |
| Windows | `exec::windows_thread_pool` | `rdcx::pool::on_pool` | `ReadDirectoryChangesW` + `CreateThreadpoolIo` |
| Linux | `exec::io_uring_context` | `inx::on_ring` | `inotify_init1` + `IORING_OP_READ` |

## Scope

**In:**
- 多 path watch（建構時 + 動態 add/remove），thread-safe `wd → path` 對應。
- 單一 active subscription per `inotify_context`（CAS-guarded，跟 FSEvents/RDC 一致）。
- Continuation-style backpressure：CQE → `set_next` → 下游跑完 → `next_receiver::set_value` 重 submit READ。
- Cancellation 透過 `IORING_OP_ASYNC_CANCEL`，cleanup 在獨立 task slot（不嵌在 IO completion 中）。
- 原生 inotify 事件欄位 (`wd / mask / cookie / name`) 直接 surface。
- IN_Q_OVERFLOW 對應 `fs_batch::overflow=true`；IN_IGNORED 自動清 wd→path map。

**Out (deliberately NOT done — 詳見 README):**
- Recursive / subtree watching：caller 用 `add_watch` 自己組，README 寫 walk + race。
- fanotify backend：CAP_SYS_ADMIN 要求 + 不對稱 kernel feature surface 不適合 example。
- `IORING_OP_READ_MULTISHOT`：未來優化，需要 provided buffers ring，且打破 backpressure 對齊。
- 多訂閱者 fan-out：layer on top。
- 單元測試：與 `fsevents_wrapper.hpp` / `rdc_wrapper.hpp` 既有先例對齊。

## Architecture

Wrapper 不擁有 thread。User 提供 `exec::io_uring_context::scheduler`，透過
`inx::on_ring` 注入到 receiver env（`exec::__on_scheduler_t` 的實例化，跟 fsx /
rdcx::pool 用同一個 shared adapter）。

`__op` lifecycle:

1. `start()` 末尾：
   - 透過 `inotify_context::__activate_(this)` CAS 進駐單一 active slot。
   - 透過 `io_uring_context::submit(__task*)` 入口送出第一個 `IORING_OP_READ` (target =
     inotify_fd)。
   - 註冊 stop callback（**最後一步**：與 FSEvents/RDC 一致，避免在 ring/fd/SQE 尚未就緒時被同步 fire）。
2. CQE 在 io_uring reactor thread 上回呼：
   - `IN_Q_OVERFLOW` (wd=-1) → `batch.overflow=true`，event 從 span 過濾掉。
   - `IN_IGNORED` → 在 mutex 內從 `wd_to_path_` map erase；event 仍 surface 給下游（讓 caller 看見 watch 真的沒了）。
   - 其餘事件 push 進 staging vector。
   - `set_next(rcvr, just(fs_batch))` → start child op。
3. `next_receiver::set_value` → 若未 stop_requested，重新 submit READ；否則 schedule cleanup。
4. `next_receiver::set_stopped` / `set_error` → schedule cleanup with finish kind。
5. Cleanup task：
   - drop stop callback。
   - submit `IORING_OP_ASYNC_CANCEL` 確保任何 in-flight READ 收尾（只在 stop callback 路徑時實際有 in-flight READ；set_value/set_stopped/set_error 路徑此時無 SQE in flight，cancel 為 no-op）。
   - **不** close inotify_fd（fd 由 `inotify_context` 擁有，見「inotify_fd 的擁有權」一節）。
   - 釋出 `__active_` slot。
   - `set_stopped(rcvr)` 或 `set_error(rcvr, ep)`。

## Public API

```cpp
namespace inx {
  struct fs_event {
    int         wd;       // -1 表 IN_Q_OVERFLOW；其他都是 inotify_add_watch 回傳的 wd
    uint32_t    mask;     // 原生 inotify mask
    uint32_t    cookie;   // 用於配對 IN_MOVED_FROM ↔ IN_MOVED_TO
    std::string name;     // basename；watch 自身事件時為空
  };

  struct fs_batch {
    std::span<const fs_event> events;
    bool                      overflow;  // 本批前/中遭遇 IN_Q_OVERFLOW，需 rescan
  };

  struct watch_options {
    uint32_t mask = IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MODIFY
                  | IN_ATTRIB | IN_MOVED_FROM | IN_MOVED_TO | IN_MOVE_SELF
                  | IN_CLOSE_WRITE;
    std::size_t buffer_size = 64 * 1024;  // read-staging buffer
  };

  class inotify_context {
   public:
    // initial_paths 用 default_mask 一次 add 進 fd。default_mask 同時是
    // 後續 add_watch(path, nullopt) 的預設 mask。
    explicit inotify_context(std::vector<std::string> initial_paths,
                             uint32_t default_mask = watch_options{}.mask);
    ~inotify_context();

    inotify_context(const inotify_context&) = delete;
    auto operator=(const inotify_context&) -> inotify_context& = delete;

    // Thread-safe；可在 watch 進行中呼叫。
    // mask=nullopt → 用 ctor 傳入的 default_mask。
    // throws std::system_error。
    auto add_watch(std::string_view path,
                   std::optional<uint32_t> mask = std::nullopt) -> int;

    // Thread-safe；wd 不存在時 no-op 並回傳 false。
    auto remove_watch(int wd) noexcept -> bool;

    // Thread-safe；wd 不存在或已 IN_IGNORED 時回傳 nullopt。
    auto path_for(int wd) const -> std::optional<std::string>;

    auto watch(watch_options opts = {}) -> __detail::__watch_sender;
  };

  // env-injection adapter。三平台共用 exec::__on_scheduler_t。
  inline constexpr exec::__on_scheduler_t on_ring{};
}
```

`__watch_sender::subscribe` constraint:

```cpp
template <stdexec::receiver _Rcvr>
  requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                     exec::io_uring_context::scheduler>
auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>;
```

→ 沒透過 `inx::on_ring` 注入 io_uring scheduler 是 compile error，跟 fsx / rdcx::pool 一致。

## Cancellation

- Stop callback fire (`__on_stop_fn::operator()`)：
  - `__stop_requested_.store(true, release)`。
  - 若 fd 仍 valid，submit `IORING_OP_ASYNC_CANCEL`（user_data 指向 in-flight READ 的 task）。
- READ 完成回 `-ECANCELED`：completion path 走 cleanup with `__finish_stopped`。
- 跟 RDC pool / FSEvents 同樣警告：若下游不傳 stop_token，pipeline 會卡（沒新 SQE → 沒 CQE → continuation 不前進）。沿用 `sequence_sender_on_scheduler.md` 既有解釋。

## Backpressure

Continuation-style，跟 RDC pool 完全一致：

- CQE 進來後 `set_next`，set_value 才重 submit READ。
- READ 不在 flight 期間，kernel 的 inotify 內部 queue 累積（上限見
  `/proc/sys/fs/inotify/max_queued_events`，常見預設 16384）。
- Queue 撐爆 → kernel 發 `wd=-1, mask=IN_Q_OVERFLOW`。
- Wrapper 把 `overflow=true` 傳到下一個 batch，並把這個 synthetic event 從
  `events` span 過濾掉（保留真 event；對齊 FSEvents 把 drop notice 同 batch surface 的精神）。

## add_watch / remove_watch / path_for 的同步

- `wd_to_path_` 用 `std::unordered_map<int, std::string>`，`mutable std::mutex` 保護。
- `inotify_add_watch(2)` / `inotify_rm_watch(2)` 本身 kernel-side thread-safe；map mutation 在 mutex 內。
- CQE 路徑遇 `IN_IGNORED` → 同樣鎖 mutex 做 erase。`path_for(wd)` 在 user 看到 IN_IGNORED 那批 batch 之後查必為 `nullopt`（觀察順序：解析→更新 map→set_next 整段在 io_uring reactor thread 上序列化）。
- `path_for` 為查詢，回傳值是 `optional<std::string>`（copy 出來），呼叫端不持有 map 內部引用。

## Single-active subscription

`std::atomic<__op_base*> __active_`，CAS-guarded 進駐 / 釋出。並發 subscribe 第二份 → `set_error(std::runtime_error{"inotify_context already has an active watch"})`。

`add_watch` / `remove_watch` 不需 CAS — 它們不獨佔 active slot，只動 wd→path map + 呼 inotify syscall。允許在 `watch` active 期間呼叫。

## inotify_fd 的擁有權

- `inotify_context` 持有 fd（`int inotify_fd_ = -1`），在 ctor 開、dtor 關。
- 動機：`add_watch` / `remove_watch` 必須能在沒有 active subscription 時呼叫（例如 ctor 之後 immediately add 一堆，再 subscribe）。如果 fd 由 `__op` 擁有，這個流程不成立。
- `__op` 借用 fd 不擁有它。Cleanup 時 close 的不是 fd 本身，而是釋出 active slot。

## 與 io_uring_context 的整合

- `__op` 持有一個內嵌 `__task` 物件（符合 `exec::__io_task` concept），包含：
  - `submit(io_uring_sqe&)`：填 `IORING_OP_READ`、fd、buffer ptr/len、user_data。
  - `complete(const io_uring_cqe&)`：解析或處理 -ECANCELED。
  - `context() noexcept -> __context&`：回 user-supplied io_uring_context。
- Cancel SQE 用第二個內嵌 `__task`（避免 race：cancel SQE 跟 read SQE 不能共用 user_data）。
- io_uring_context 的 scheduler 從 receiver env 拿（`stdexec::get_scheduler(get_env(rcvr_))`），它的 native_handle / context 透過 scheduler 公開。

## Demo (`examples/inotify.cpp`)

仿 `rdc.cpp` / `fsevents.cpp`：

```cpp
exec::io_uring_context ring;
std::thread driver{[&]{ ring.run_until_stopped(); }};

inx::inotify_context ctx{{dir.string()}};

stdexec::sync_wait(exec::when_any(
    /* timer 3s 切斷 */,
    inx::on_ring(ring.get_scheduler(), ctx.watch())
      | exec::transform_each(stdexec::then([&](inx::fs_batch b) {
          if (b.overflow) std::printf("[overflow]\n");
          for (const auto& e : b.events) {
            // print mask + name + (optional) path_for(e.wd)
            if (e.mask & IN_CREATE && e.mask & IN_ISDIR) {
              auto p = ctx.path_for(e.wd);
              if (p) ctx.add_watch(*p + "/" + e.name);  // 動態擴展示範
            }
          }
        }))
      | exec::ignore_all_values()));

ring.request_stop();
driver.join();
```

關鍵示範點：
1. `inx::on_ring(ring.get_scheduler(), ctx.watch())` — env 注入 io_uring scheduler。
2. `transform_each` 處理 `fs_batch`。
3. `ctx.add_watch(...)` 在 watch 進行中呼叫 — inotify 獨有 surface。
4. `when_any(timer, watch)` cancellation 走 stop_token。

## CMakeLists

現有 `if (LINUX)` 已有 `example.io_uring` 走 `def_example` helper。新增同模式：

```cmake
if (LINUX)
  set(stdexec_examples ${stdexec_examples}
                    "example.io_uring : io_uring.cpp"
                    "example.inotify : inotify.cpp"
  )
endif ()
```

不需要額外 link flag — `exec/linux/io_uring_context.hpp` 已是 stdexec 的一等公民。

## README outline (`examples/inotify_README.md`)

仿 `rdc_README.md` 結構：

1. Files 表
2. Build 指令
3. API 範例
4. 平台對照表（fsx / rdcx::pool / inx 三欄）
5. Pool selection / scheduler — 解釋 `inx::on_ring` 跟 compile-time scheduler-type 約束
6. What happens under the hood — ASCII flow diagram
7. Backpressure — 表格 + IN_Q_OVERFLOW 對應
8. Cancellation — flow diagram
9. inotify quirks 表（IN_IGNORED 自動清 map / IN_Q_OVERFLOW / 路徑必須是 directory 才能看子檔事件 / 重複 add_watch 同 path 的 mask 行為 / 非遞迴 / inode-level 不是 path-level）
10. Recursive watching pattern — walk + race 警告
11. Variant comparison（單變體，但放跟 fsx / rdcx::pool 的對照）
12. Things deliberately NOT done

## 開放問題

無 — Q1（B：io_uring_context-driven）/ Q2（B：多 path 動態 add/remove）/ Q3（A：inotify only）已收斂。
