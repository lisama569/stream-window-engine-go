# 本地流式窗口引擎（Go）

用**本地产生的事件流**与**本地汇聚端点**模拟数据源 / 数据出口的流式数据处理引擎核心。
目标：在乱序、迟到、重启恢复与并发处理下，窗口统计结论**始终可预期、可复现**。

- 无外部依赖（仅标准库）；事件时间毫秒精度；滚动（tumbling）窗口与滑动（sliding / hopping）窗口。
- 代码位置：

| 包 | 职责 |
| --- | --- |
| `internal/event` | 事件定义、结构性输入校验（缺字段 / 非法时间戳 / NaN·Inf） |
| `internal/watermark` | 单调水位线（bounded out-of-orderness） |
| `internal/window` | 滚动/滑动窗口划分、聚合、按窗口去重集合、版本化输出、聚合快照 |
| `internal/endpoint` | 本地内存数据源 `Source` 与并发安全汇聚出口 `Sink` |
| `internal/engine` | 分片并发执行、多窗口路由、迟到/重复/内存上限处置、整体快照与恢复 |

## 1. 窗口与水位线语义

### 窗口划分（滚动与滑动）
所有窗口左闭右开、按纪元（时间 0）对齐：

```
滚动（tumbling）：start(t) = t - t % windowSize；窗口 = [start, start+windowSize)
滑动（sliding）：起点网格 = slide 的整数倍（…,0,slide,2slide,…）；
                事件时间 t 同时属于所有满足 start <= t < start+windowSize 的窗口
```

- `Config.SlideMillis` 配滑动步长：
  - **缺省（0）或等于 `WindowSizeMillis`**：滚动窗口，每个事件恰好命中一个窗口，
    与历史行为逐结果一致（滑动是滚动的严格泛化，滚动是 `slide=size` 的退化）；
  - `0 < slide < size`：滑动窗口，同一事件同时进入覆盖它的多个窗口，典型如
    **"每 1 分钟往前看 5 分钟"** = `WindowSizeMillis=300000, SlideMillis=60000`，
    任一事件最多同时打开 `ceil(size/slide)` 个窗口；
  - `slide > size` 无意义（会留下覆盖空隙），构造时规范化为 `size`。
- 事件时间域为 `t >= 0`；纪元前起点（负 start）的窗口不创建，早期事件命中的
  窗口数可以少于理论重叠度，之后稳定为 `ceil(size/slide)`。
- 各窗口**各自独立聚合、独立按事件时间水位线关闭输出、独立去重与迟到判定**，
  互不串味：一条事件在某个窗口被拒绝（过期/重复/容量），绝不影响它在其他窗口
  的正常计入。

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
- `Sink.FinalResults()` 折叠所有输出，每个 `(key, windowStart)` 只保留 **Version 最大**
  的一条，即该窗口确定的最终结果；下游可用 Version 做幂等覆盖。
- 每个分片内部窗口按 `(windowStart, key)` 排序触发；一次提交同时让多个窗口到点时，
  本事件的迟到修正（窗口起点升序）排在前、到点 FIRE（按 `(start,key)`）排在后。
  相同输入独立运行两次，**逐条原始输出序列**即字节级一致（见
  `TestSlidingMultiWindowFireOrder`、`TestDeterministicRepro`）。
- 跨分片输出顺序由调度决定、不作全局排序保证；确定性断言以单分片或
  `FinalResults()` 折叠视图为准。

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

注意判定基于**事件到达时刻**（该事件时间被观察之后）的水位线，判定顺序为
"观察时间推进水位线 → 先 FIRE 到点窗口 → 清理过宽限期窗口 → 事件再逐窗口路由"，
因此推进 WM 的事件不会把自己判成迟到。

### 重叠窗口下的迟到与重复（滑动窗口关键语义）
一条事件同时命中多个窗口时，迟到/重复/容量判定都是**逐窗口独立**进行的：

- **只修正真正受影响的窗口**：迟到事件只对"已 FIRE 且仍在宽限期"的覆盖窗口产出
  `UPDATE`；还没 FIRE 的窗口静默计入、不产生输出；已过宽限期被清理的窗口各记一条
  `LATE_BEYOND_TOLERANCE`。同一条事件因此可能在部分窗口 UPDATE、在另一些窗口被丢，
  两种结局各自带窗口坐标 `[window_start, window_end)`，互不混淆。
- **同一事件 ID 在同一个窗口里只算一次**：去重集合是每个 `(key, window)` 一份。
  同一条事件进入 5 个窗口，就会在 5 个窗口各计一次（这是滑动统计的正确语义）；
  但该事件重放时，会在它命中的**每一个**窗口分别识别为 `DUPLICATE_EVENT`
  （`Stats.DroppedDup` 因此按窗口条目计数），任何窗口都不会双计。
- **原因不串味**：`LATE_BEYOND_TOLERANCE`、`DUPLICATE_EVENT`、
  `MEMORY_LIMIT_REJECTED` 三类原因在每条 `Drop` 上互斥且携带窗口坐标，一种原因
  绝不混成另一种；一条坏事件只影响它自己进不去的窗口，其他窗口结果不被带偏。

### 重复数据
事件以 `ID` 唯一标识；每个窗口聚合维护各自独立的已消费 ID 集合：

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

`Engine.Restore(snap)` 把以上状态全部还原到快照点。恢复时会校验快照与引擎的
**窗口划分（大小/步长）一致**，不一致直接返回错误（不同划分下同一 windowStart
语义不同，混用会造成窗口错位；旧版本无步长字段的快照按滚动窗口兼容）。恢复后
**重放快照点之前已消费的数据**时：

1. 事件 ID 在它命中的**每个窗口**的已消费集合中命中 → 各窗口判为
   `DUPLICATE_EVENT`，不重复计数；滑动窗口下一条重放事件因此会产生多条重复记录
   （每个命中窗口一条），这正是按窗口去重的预期口径；
2. 重复事件不触发任何窗口输出 → 输出不重复；
3. 重放同时幂等重建水位线（只进不退），后续新事件的窗口 / 迟到判定与快照点一致；
4. 计数器从快照值继续累加，统计口径连续。

即"中途快照 + 重放已消费数据"满足**不重不漏**，水位线与窗口状态与快照点一致
（见 `TestSlidingSnapshotRestoreReplay`、`TestSnapshotRestoreReplay`、
`TestSnapshotJSONRoundTrip`）。快照内每个窗口的去重 ID 集合排序导出，同一状态
连续两次快照字节级一致（`TestSlidingSnapshotDeterministicBytes`）。

## 4. 内存上限策略

- 活跃状态由 `windowStart → key → 聚合` 的 map 持有，窗口生命周期为
  **FIRE 后再保留一个宽限期**；当 `WM > windowEnd + AllowedLateness` 时窗口在下一
  次提交时被清理（先 FIRE、后清理，绝不回收尚未输出的窗口）。清理数计入
  `Stats.PurgedWindows`。
- **滑动窗口下状态是按重叠度放大的**：同一份数据会同时撑开到至多
  `ceil(size/slide)` 份窗口状态（每分钟看 5 分钟即 5 份）。但由于过期窗口过宽限期
  即真正释放，单调事件时间流下活跃窗口数稳态恒为重叠度量级，**不随历史长度增长**；
  每次提交先做一轮 FIRE+清理再路由新事件，因此正常推进的流不会误触上限。
- 两道**每个分片**的硬上限（<=0 表示不限），满足其一即拒绝新窗口创建：
  - `Config.MaxKeys`：活跃的不同 key 数（沿用滚动窗口语义）；
  - `Config.MaxWindows`：活跃 `(key,window)` 聚合条目总数，给滑动窗口的重叠状态
    兜底。容量规划可按 `MaxWindows ≈ 活跃 key 数 × ceil(size/slide)` 估算。
- 达到上限且当轮回收后仍不足时，**仅拒绝本次事件真正装不下的那个窗口**，以
  `MEMORY_LIMIT_REJECTED`（携带窗口坐标）写入 `Sink.Drops()`、计入
  `Stats.DroppedMemory`；该事件命中的其他仍有容量的窗口照常计入。引擎不返回错误、
  不被拖死，容量随过期回收恢复后后续事件照常处理（见
  `TestSlidingCapacityAndStateReclaim`、`TestMemoryLimitObservable`）。

## 5. 并发模型

- 按 key 的 FNV-1a 哈希分到 `ShardCount` 个分片；同 key 永远同分片，分片内一把
  互斥锁串行化水位线观察、聚合变更与输出版本分配；不同分片真正并行。
- `Source` / `Sink` 均为并发安全，多生产者可同时提交。
- 全量测试在 `-race` 下通过（见 `TestConcurrentInterleaving`、
  `TestConcurrentSameKeyContention`、`TestSlidingConcurrentSameKey`）。

## 6. 验证方法

```bash
# 全部单元测试（含滚动/滑动窗口、快照恢复、乱序、迟到、异常输入、并发交错）
go test ./... -v

# 竞态检测
go test -race -count=1 ./...

# 端到端本地管线示例（本地源 -> 引擎 -> 本地汇）
go test . -run Example_localPipeline -v
```

测试中可注入 `engine.WithLogger(...)`（如 `t.Logf`），日志逐条打印：
**事件时间、当前水位线、事件命中的全部窗口 `[start,end)` 列表（命中窗口数）**、
**FIRE/UPDATE/LATE-WITHIN-TOLERANCE/PURGE/DROP 触发原因、窗口坐标及 Version**，
便于区分失败原因。关键用例：

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
| `TestSpecSlidingStarts` / `…NonNegativeDomain` | 滑动窗口划分、边界、纪元对齐、负起点截断 |
| `TestSlidingOverlappingAggregation` | 同一事件进入多个窗口、各窗口独立聚合 |
| `TestSlidingBoundaryEvents` | 窗口交界处事件的左闭右开归属 |
| `TestSlidingMultiWindowFireOrder` | 多窗口同时到点的确定输出顺序、两次运行逐条一致 |
| `TestSlidingLateUpdateAndDedupPerWindow` | 重叠窗口下逐窗口迟到修正/去重、原因互斥不串味 |
| `TestSlidingCapacityAndStateReclaim` | 容量硬上限可观测拒绝、长流下状态有界与过期释放 |
| `TestSlidingMaxKeysStillEnforced` | 滑动下 MaxKeys 与 MaxWindows 双上限独立生效 |
| `TestSlidingSnapshotRestoreReplay` | 滑动中途快照 + JSON 往返 + 重放不重不漏，终态等价 |
| `TestSlidingSnapshotDeterministicBytes` | 同一滑动状态两次快照字节一致 |
| `TestSlidingRestoreRejectsDifferentSpec` | 窗口划分不一致的恢复被显式拒绝 |
| `TestSlidingDegeneratesToTumbling` | slide=size 与滚动窗口逐输出/丢弃/计数完全等价 |
| `TestSlidingConcurrentSameKey` | 滑动窗口同 key 并发计数精确、全窗口计数守恒 |

## 7. 最小用法

```go
// 滚动窗口（老配置，行为不变；SlideMillis 留空或等于 WindowSizeMillis 即可）
cfg := engine.Config{
    ShardCount:            4,
    WindowSizeMillis:      1000,
    OutOfOrderMillis:      500,
    AllowedLatenessMillis: 1000,
    MaxKeys:               100000,
}

// 滑动窗口：每 1 分钟（slide）往前看 5 分钟（size）
slidingCfg := engine.Config{
    ShardCount:            4,
    WindowSizeMillis:      5 * 60 * 1000, // 300000：窗口大小 5 分钟
    SlideMillis:           1 * 60 * 1000, // 60000：步长 1 分钟
    OutOfOrderMillis:      500,
    AllowedLatenessMillis: 1000,
    MaxKeys:               100000,
    MaxWindows:            500000, // ≈ 活跃 key 数 × ceil(size/slide)
}
sink := endpoint.NewSink()
eng := engine.New(slidingCfg, sink)
defer eng.Close()

_ = eng.Submit(ctx, event.Event{ID: "1", Key: "userA", Value: 10, EventTime: 100})
// 该事件会同时计入 5 个覆盖它的 5 分钟窗口，各窗口独立 FIRE/UPDATE/去重。

snap := eng.SaveSnapshot()      // 一致性快照（可 json.Marshal 落盘）
raw, _ := json.Marshal(snap)

for _, d := range sink.Drops() { // 每条丢弃带原因与所属窗口坐标
    // d.Reason / d.WindowStart / d.WindowEnd ...
}
for _, r := range sink.FinalResults() { // Version 折叠后的确定最终结果
    // r.Sum / r.Count ...
}
```
