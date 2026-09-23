# 本地流式窗口引擎（Go）

用**本地产生的事件流**与**本地汇聚端点**模拟数据源 / 数据出口的流式数据处理引擎核心。
目标：在乱序、迟到、重启恢复与并发处理下，窗口统计结论**始终可预期、可复现**。

- 无外部依赖（仅标准库）；事件时间毫秒精度；滚动（tumbling）窗口。
- 代码位置：

| 包 | 职责 |
| --- | --- |
| `internal/event` | 事件定义、结构性输入校验（缺字段 / 非法时间戳 / NaN·Inf） |
| `internal/watermark` | 单调水位线（bounded out-of-orderness） |
| `internal/window` | 滚动窗口聚合、去重集合、版本化输出、聚合快照 |
| `internal/endpoint` | 本地内存数据源 `Source` 与并发安全汇聚出口 `Sink` |
| `internal/engine` | 分片并发执行、迟到/重复/内存上限处置、整体快照与恢复 |

## 1. 窗口与水位线语义

### 窗口划分
滚动窗口左闭右开：

```
start(t) = t - t % windowSize
窗口 = [start, start + windowSize)
```

### 水位线（Watermark）
采用周期性水位线策略，单分片内跟踪观察到的最大事件时间 `maxSeen`：

```
WM = max(startFloor, maxSeen - maxOutOfOrderness)
```

- **水位线只进不退**：乱序、迟到、重复事件都不可能让 WM 下降；只有当候选值严格
  大于当前值时 WM 才前进一步（`Watermark.Observe` 返回是否推进）。
- **触发条件**：当且仅当 `WM >= windowEnd` 时窗口关闭并输出一次 `FIRE`。
  WM 未到右端点的窗口**绝不提前输出**。
- 水位线按分片独立维护；同一 key 的事件经哈希稳定路由到同一分片，因此单 key 的
  事件时间序由该分片串行处理，跨分片互不影响。

### 同窗口重复触发的确定性
同一 `(key, window)` 的每次输出带单调递增 `Version`：

| Kind | 含义 | Version |
| --- | --- | --- |
| `FIRE` | WM 到达右端点时的首次关闭输出 | `0`，全局唯一 |
| `UPDATE` | 宽限期内迟到事件对已关闭窗口的修正 | `1, 2, 3, …` 严格递增 |

- 窗口一旦 `FIRE`，永远不会再次 `FIRE`；迟到修正只产生更高版本的 `UPDATE`。
- `Sink.FinalResults()` 折叠所有输出，每个 `(key, window)` 只保留 **Version 最大**
  的一条，即该窗口确定的最终结果；下游可用 Version 做幂等覆盖。
- 每个分片内部窗口按 `(windowStart, key)` 排序触发；相同输入独立运行两次，最终
  结果字节级一致（见 `TestDeterministicRepro`）。

## 2. 乱序、迟到事件与重复数据的处理约定

### 乱序（仍在水位线容忍内）
`maxOutOfOrderness`（`OutOfOrderMillis`）之内的乱序事件在所属窗口 `FIRE` 之前
到达，正常计入，对结果无任何特殊影响。

### 迟到事件（窗口已 FIRE）
设宽限期为 `AllowedLatenessMillis`：

- `WM <= windowEnd + AllowedLateness`（含边界）：事件被接受并修正窗口，
  输出一条 `UPDATE`（Version +1）。
- `WM > windowEnd + AllowedLateness`：窗口状态已被清理，事件**不会静默丢弃**，
  而是以原因 `LATE_BEYOND_TOLERANCE` 写入 `Sink.Drops()` 并计入
  `Stats.DroppedLate`。

注意判定基于**事件到达时刻**（该事件时间被观察之前）的水位线，推进 WM 的事件
不会把自己判成迟到。

### 重复数据
事件以 `ID` 唯一标识；每个窗口聚合维护已消费 ID 集合：

- 同一 ID 在同一 `(key, window)` 内第二次（含更多次）到达，**只计一次**，
  以 `DUPLICATE_EVENT` 写入 `Sink.Drops()`、计入 `Stats.DroppedDup`。
- 重复事件除幂等的水位线观察外**无任何副作用**：不计数、不产生 FIRE/UPDATE。

### 异常输入（不中断、不污染）
`event.Validate` 拒绝以下结构性非法输入，旁路记录到 `Sink.InvalidEvents()` 与
`Stats.ParseErrors`，`Submit` 不返回错误，其余事件与窗口不受影响：

- `MISSING_ID`（空 ID）、`MISSING_KEY`（空 key）
- `INVALID_TIMESTAMP`（负时间戳）
- `INVALID_VALUE`（`NaN` / `+Inf` / `-Inf`）

时间戳相对前序事件"倒退"不是非法输入——它是乱序 / 迟到，由水位线与窗口机制处理。

## 3. 快照恢复与重复消费的一致性保证

`Engine.SaveSnapshot()` 逐分片加锁，生成某一时刻的一致性快照（可 `json.Marshal`
落盘）：

- 每个分片的**水位线**、**全部窗口聚合状态（含已消费事件 ID 集合、closed 标志）**、
  **已输出 Version**，以及引擎计数器。
- 快照内窗口按 `(windowStart, key)` 排序，重复快照字节级确定，便于比对。

`Engine.Restore(snap)` 把以上状态全部还原到快照点。恢复后**重放快照点之前已消费
的数据**时：

1. 事件 ID 命中快照中的已消费集合 → 判为 `DUPLICATE_EVENT`，不重复计数；
2. 重复事件不触发任何窗口输出 → 输出不重复；
3. 重放同时幂等重建水位线（只进不退），后续新事件的窗口 / 迟到判定与快照点一致；
4. 计数器从快照值继续累加，统计口径连续。

即"中途快照 + 重放已消费数据"满足**不重不漏**，水位线与窗口状态与快照点一致
（见 `TestSnapshotRestoreReplay`、`TestSnapshotJSONRoundTrip`）。

## 4. 内存上限策略

- 活跃状态由 `windowStart → key → 聚合` 的 map 持有，窗口生命周期为
  **FIRE 后再保留一个宽限期**；当 `WM > windowEnd + AllowedLateness` 时窗口在下一
  次提交时被清理（先 FIRE、后清理，绝不回收尚未输出的窗口）。清理数计入
  `Stats.PurgedWindows`。
- `Config.MaxKeys`（>0 时）是**每个分片活跃 key 数的硬上限**。新窗口创建前若已
  达上限：先触发到点窗口并尝试回收过期窗口；回收后仍不足，则该事件以
  `MEMORY_LIMIT_REJECTED` 写入 `Sink.Drops()`、计入 `Stats.DroppedMemory`，
  内存不会无界增长。
- 该处置是**可观测且可区分**的（与迟到、重复原因互斥），且不阻断引擎，后续事件
  在容量恢复后照常处理（见 `TestMemoryLimitObservable`）。

## 5. 并发模型

- 按 key 的 FNV-1a 哈希分到 `ShardCount` 个分片；同 key 永远同分片，分片内一把
  互斥锁串行化水位线观察、聚合变更与输出版本分配；不同分片真正并行。
- `Source` / `Sink` 均为并发安全，多生产者可同时提交。
- 全量测试在 `-race` 下通过（见 `TestConcurrentInterleaving`、
  `TestConcurrentSameKeyContention`）。

## 6. 验证方法

```bash
# 全部单元测试（含快照恢复、乱序、迟到、异常输入、并发交错）
go test ./... -v

# 竞态检测
go test -race -count=1 ./...

# 端到端本地管线示例（本地源 -> 引擎 -> 本地汇）
go test . -run Example_localPipeline -v
```

测试中可注入 `engine.WithLogger(...)`（如 `t.Logf`），日志逐条打印：
**事件时间、所属窗口 `[start,end)`、观察前后水位线、迟到判定、FIRE/UPDATE/DROP
原因及 Version**，便于区分失败原因。关键用例：

| 用例 | 覆盖点 |
| --- | --- |
| `TestOutOfOrderWatermarkFiring` | 乱序计入、WM 未到不提前输出、满足条件才 FIRE |
| `TestLateWithinToleranceUpdateAndBeyondDrop` | 宽限内 UPDATE 递增、超宽限 `LATE_BEYOND_TOLERANCE`、FIRE 唯一 |
| `TestDuplicateEventDedup` | 重复 ID 不双计、`DUPLICATE_EVENT` 可观测 |
| `TestInvalidInputDoesNotKillPipeline` | 四类非法原因、不中断、不污染其他窗口 |
| `TestSnapshotRestoreReplay` | 中途快照恢复 + 重放不重不漏、水位线/状态一致 |
| `TestSnapshotJSONRoundTrip` | 快照 JSON 落盘往返后重放语义不变 |
| `TestConcurrentInterleaving` / `…SameKeyContention` | 乱序并发交错、同 key 竞争下计数精确 |
| `TestMemoryLimitObservable` | 硬上限拒绝、过期回收后容量恢复 |
| `TestDeterministicRepro` | 相同输入两次独立运行结果一致 |

## 7. 最小用法

```go
cfg := engine.Config{
    ShardCount:            4,
    WindowSizeMillis:      1000,
    OutOfOrderMillis:      500,
    AllowedLatenessMillis: 1000,
    MaxKeys:               100000,
}
sink := endpoint.NewSink()
eng := engine.New(cfg, sink)
defer eng.Close()

_ = eng.Submit(ctx, event.Event{ID: "1", Key: "userA", Value: 10, EventTime: 100})

snap := eng.SaveSnapshot()      // 一致性快照（可 json.Marshal 落盘）
raw, _ := json.Marshal(snap)

for _, r := range sink.FinalResults() { // Version 折叠后的确定最终结果
    // r.Sum / r.Count ...
}
```
