# stream-window-engine-go

一套**本地**流式数据处理引擎的核心执行能力：本地产生事件流、本地汇聚端点
（`MemorySink`）模拟数据源与数据出口，专注在乱序、迟到、重启恢复与并发处理
下让统计结论**始终可预期、可复现**。不依赖任何外部消息队列或数据库。

- 事件时间（event-time）滚动窗口 + 单调推进的水位线
- 乱序事件按事件时间归入正确窗口；迟到事件在容忍期内触发**带标记的修正结果**
- 迟到超界、重复事件、非法输入均**按原因区分、可观测**，绝不静默丢弃
- 一致性快照 / 恢复 / 重放：水位线、窗口状态、去重集合、触发标记完全一致
- 并行处理采用「按 key 分片 + 水位线屏障」，最终状态与顺序执行逐字节一致
- 内存占用有明确硬上限（存活窗口状态数、单窗口事件数、拒绝记录缓冲）

## 快速开始

```bash
go run ./cmd/demo          # 端到端演示（乱序/迟到/重复/异常/快照恢复/并行）
go test ./...              # 全部单测
go test ./... -race -v     # 竞态检测 + 详细日志（事件时间/水位线/窗口判定）
```

## 1. 窗口与水位线语义

### 窗口

- 采用**滚动窗口（tumbling window）**，窗口大小 `Config.WindowSizeMillis`，
  相对 Unix epoch 对齐：窗口起点 `start = floor(ts / size) * size`。
- 窗口为**半开区间 `[start, end)`**：`ts == end` 属于下一个窗口；时间戳 0
  （epoch 零点）合法，属于第一个窗口。
- 聚合维度为 `(窗口, key)`，默认聚合为求和（`SumAggregate`），也可用
  `CountAggregate` 或自定义 `Aggregate`。

### 水位线

- 水位线是一条**单调非递减**的事件时间断言，初始值为 `-1`（尚无事件时间信息）。
- 水位线**只有严格增大时才前进**：`AdvanceWatermark(target)` 仅当
  `target > current` 生效；相等或倒退的标点（punctuation）会被拒绝，返回错误并
  以 `BACKWARD_WATERMARK` 原因上报，**不改变任何状态**。
- **触发条件（唯一且确定）**：当且仅当 `watermark >= windowEnd` 时，窗口输出
  一次初始结果。`watermark < end` 时，无论已到多少事件都**绝不提前输出**。
- 同一窗口的初始结果**只触发一次**：重复的水位线推进不会重复输出；窗口上有
  `fired` 标记，快照中持久化，恢复后同样不会重复触发。
- 可选的**有界乱序启发式**：`MaxOutOfOrderMillis > 0` 时，每接受一个事件后水位线
  可前进到 `max(已观测事件时间戳) - delay`。关键顺序约定是——**事件先按当前
  水位线完成迟到/重复分类并被接受，之后启发式水位线才允许前进**，因此事件绝不会
  被自己携带的时间戳「判死」。显式标点与启发式取较大者，水位线仍永不倒退。
- 初始触发的输出顺序确定：按 `(窗口起点, key)` 排序输出。

`WindowResult.Revised` 区分首次输出与迟到修正：`false` = 首次，`true` = 容忍期内
迟到事件导致的修正输出；`Watermark` 字段记录输出时刻的水位线。

## 2. 迟到事件与重复数据处理约定

设窗口结束为 `end`，容忍迟到为 `AllowedLatenessMillis`，则
`horizon = end + AllowedLateness`。

| 情形 | 判定条件 | 处理 | 可观测原因 |
|---|---|---|---|
| 正常/乱序 | `wm < end` | 计入窗口，不立即输出 | — |
| 容忍内迟到 | 已触发且 `wm < horizon` | 计入窗口，输出一条 `Revised=true` 的修正结果 | — |
| 迟到超界 | `wm >= horizon`（状态已清除） | 拒绝，窗口最终结果冻结，不再变化 | `TOO_LATE` |
| 重复事件 | 同一存活窗口状态内出现相同 `ID` | 拒绝，聚合不变 | `DUPLICATE` |

约定要点：

- **修正语义确定且前后一致**：容忍期内每接受一个迟到事件，就基于窗口最新完整
  状态输出一条全量修正值（累计和/计数），下游以「同一 `(窗口,key)` 的最后一条
  输出」为准即为最终值。
- 到达 `horizon` 后窗口状态被**清除（purge）**，这是内存有界的根本机制；此后
  再来的事件其 ID 已无从记忆，按 `TOO_LATE` 处理（迟到超界判定优先于重复判定）。
- 去重以 `(窗口, key)` 状态为作用域；不同 key 或不同窗口使用相同 ID 互不影响。
- 重复消费不是静默丢弃：处理函数返回 `ErrDuplicate` / `ErrTooLate` /
  `ErrMemoryLimit`（可用 `errors.Is` 区分），同时经 `DropReporter` 上报一条带
  事件 ID、key、时间戳、当前水位线与详情的 `DropRecord`。

### 异常输入隔离

事件先过 `Event.Validate()`，按固定顺序检查（缺失字段 → 时间戳 → 数值），
保证拒绝原因确定：

- `MISSING_FIELD`：`ID` 或 `Key` 为空；
- `INVALID_TIMESTAMP`：时间戳为负数；
- `INVALID_VALUE`：数值为 `NaN` 或 `±Inf`。

非法事件**不触碰任何窗口状态**，因此不会污染其他窗口，也不会中断流——
`Source.Push` 会跳过它继续处理后续事件（单测 `TestInvalidInputsIsolated` 验证
合法窗口结果不被污染）。

## 3. 快照恢复与重复消费的一致性保证

`Engine.Snapshot()` 在内部互斥锁保护下取得**一致性深拷贝**（JSON 可往返），包含：

- `watermark`：快照点水位线；
- `max_seen_ts`：启发式水位线所需的最大已观测事件时间；
- 每个存活 `(窗口,key)` 的聚合值、计数、**已接受事件 ID 集合 `seen`**、
  接受顺序的事件值列表 `values`，以及 **`fired` 触发标记**；
- `seq`：已接受事件序号。

恢复（`RestoreEngine`）时：水位线与启发式簿记原样恢复；聚合通过按序重放
`values` 确定性重建；`seen` 集合与 `fired` 标记原样恢复。因此：

1. **恢复点状态与快照点逐字节一致**（`TestSnapshotRoundTripAndStateEquality`）。
2. **崩溃恢复协议**：在检查点（通常是水位线标点）做快照；输出端回滚到该检查点
   （本项目用一个新的 `MemorySink` 模拟）；恢复后**只重放检查点之后的后缀**。
   检查点前的结果来自旧 sink，检查点后的结果来自恢复引擎，拼接后与一次无故障
   运行完全相等——**不重复、不漏计**（`TestSnapshotReplayNoDoubleCount`）。
3. **重复消费的幂等可观测性**：即使重放位置算错、把快照已包含的事件再次送入，
   这些事件也会被 `seen` 集合拦截为 `DUPLICATE`（容忍期内）或 `TOO_LATE`（已
   purge），**绝不二次计数**（`TestSnapshotReplayDuplicateSuffix`）。换言之，去重
   集合把「至少一次投递」收敛为确定的一次生效。

> 说明：快照只负责引擎内部状态；外部 sink 的「提交/回滚」由使用方与检查点对齐。
> 引擎侧保证恢复后不会因 `fired` 标记而重复发出窗口初始结果。

## 4. 内存上限策略

内存增长来自三处，均有明确上限与**可区分的可观测处置**（不做无界增长、
不静默丢弃）：

1. **存活窗口状态数 `MaxLiveWindows`**：当前存活 `(窗口,key)` 状态数达到上限时，
   新建状态的事件以 `MEMORY_LIMIT` 拒绝并计数；已有状态继续可更新。正常情况下，
   水位线越过 `horizon` 会自动 purge 旧窗口状态，为新状态腾出配额。
2. **单窗口事件数 `MaxEventsPerWindow`**：单窗口接受事件达到上限后，更多事件以
   `MEMORY_LIMIT` 拒绝，已形成的聚合不受影响。
3. **拒绝记录缓冲 `NewMemoryDropReporter(capacity)`**：超出容量后记录不再保留，
   但 `OverflowCount()` 与按原因计数 `CountByReason()` 仍继续增长，可观测性不丢。

设为 0/负数表示该项不限（拒绝记录器有默认容量 1024）。`Stats` 提供
Accepted / Invalid / TooLate / Duplicate / MemoryLimited / Emitted / Revised /
BackwardWMMark 计数，且满足「每个输入恰好归入一类」的会计恒等式
（见 `TestConcurrentSnapshotsAndProcess`）。

## 5. 并行与确定性

`Source.Push(engine, parallelism)`：

- `parallelism <= 1`：严格按切片顺序处理。
- `parallelism > 1`：事件按 `DeterministicShard(key)`（FNV-1a 取模）分片到固定
  worker，**同一 key 总在同一 worker、顺序保持**；水位线标点是**屏障**——先排空
  屏障前的所有事件，再由协调者统一推进水位线并触发窗口，然后才放行下一批。
- 引擎内部所有状态变更由一把互斥锁串行化。结合 key 分片与水位线屏障，**任意
  并行度下的最终状态、输出条数与输出顺序都与顺序执行一致**
  （`TestParallelPushMatchesSequential`），且重复运行 20 次结果完全相同
  （`TestParallelPushDeterminismAcrossRuns`）。`go test -race` 干净。

## 6. 代码结构与验证方法

| 文件 | 职责 |
|---|---|
| `engine/event.go` | 事件模型与输入校验 |
| `engine/window.go` | 滚动窗口数学、聚合接口与内建聚合 |
| `engine/watermark.go` | 单调水位线追踪器 |
| `engine/drops.go` | 拒绝原因枚举与有界可观测收集器 |
| `engine/config.go` | 配置与校验 |
| `engine/engine.go` | 核心引擎：分类、触发/修正、purge、快照/恢复、内存上限 |
| `engine/snapshot.go` | 快照结构与 JSON 序列化 |
| `engine/pipeline.go` | 本地数据源、分片并行推送、`MemorySink` |
| `cmd/demo/main.go` | 端到端可运行演示 |

测试覆盖与定位（失败原因可区分）：

| 测试 | 覆盖场景 |
|---|---|
| `TestWindowFiresOnlyAtWatermark` | 不提前输出、到点恰好触发一次、重复触发语义、倒退水位线 |
| `TestOutOfOrderEvents` | 窗口内乱序到达的正确聚合 |
| `TestLateEventWithinLateness` | 容忍期内修正输出、超界冻结 |
| `TestHeuristicDoesNotSelfExpire` | 事件不被自身时间戳误判迟到 |
| `TestDuplicateEvent` | 重复 ID 拒绝且不污染聚合 |
| `TestInvalidInputsIsolated` | 缺失字段/非法时间戳/NaN/Inf 隔离与可区分原因 |
| `TestMemoryLimitLiveWindows` / `TestMemoryLimitPerWindow` | 两类内存硬上限 |
| `TestSnapshotRoundTripAndStateEquality` | 快照 JSON 往返、状态等价、后缀行为一致 |
| `TestSnapshotReplayNoDoubleCount` | 检查点崩溃恢复、后缀重放不重不漏 |
| `TestSnapshotReplayDuplicateSuffix` | 误重放已消费事件被幂等去重 |
| `TestParallelPushMatchesSequential` | 多并行度与顺序执行结果/顺序一致 |
| `TestParallelPushDeterminismAcrossRuns` | 多次并行运行结果完全相同 |
| `TestConcurrentSnapshotsAndProcess` | 并发交错下快照自洽与会计恒等式（`-race`） |
| `TestPushToleratesBackwardMarkAndBadEvents` | 异常标点/异常事件不中断流 |

所有测试在判定处通过 `t.Logf` 打印**事件时间、当前水位线与窗口判定依据**
（`[start,end)`、容忍边界 horizon、接受/拒绝原因），`go test -v` 即可查看。
