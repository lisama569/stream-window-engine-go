// Package engine 组装水位线、窗口状态、本地源/汇，提供并发安全的流式执行核心。
// 支持滚动窗口（WindowSlideMillis 缺省或等于窗口大小）与重叠滑动窗口
// （WindowSlideMillis < WindowSizeMillis）：同一条事件同时路由到覆盖它的全部窗口，
// 各窗口独立聚合、独立按水位线关闭/修正/清理。
//
// 并发模型：按 key 的 FNV 哈希把 key 空间划分到固定数量的 shard；同一 key 的事件
// 始终进入同一 shard，shard 内串行处理，因此该 key 的水位线与窗口状态天然一致；
// 不同 shard 并行推进。输出汇聚到线程安全的 endpoint.Sink，按接收顺序保留。
//
// 确定性：窗口触发顺序在每个 shard 内按 (windowStart, key) 排序；一次提交让多个
// 重叠窗口同时到点时同样按窗口起点升序输出，同批输入每次运行输出流完全一致。
// 同一 (key,window) 的输出携带单调递增 Version（Fire=0，迟到修正为 UPDATE 1..n），
// 任何重放/重启后最大 Version 即确定结果；重复事件在每个窗口各自去重，不会产生
// 第二次有效输出。Processed/DroppedDup/DroppedLate/DroppedMemory 均按"事件×窗口
// 归属"计数（滚动窗口每事件恰 1 个归属，与历史口径一致）。
package engine

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// 丢弃原因（可观测、可区分；重复原因复用 window.ReasonDuplicate）。
const (
	DropLate        = "LATE_BEYOND_TOLERANCE" // 窗口已过水位线宽限期，迟到事件不参与
	DropMemoryLimit = "MEMORY_LIMIT_REJECTED" // 活跃 key 数达硬上限，且无过期窗口可回收
)

// ErrEngineClosed 在引擎 Close 后再提交事件时返回。
var ErrEngineClosed = errors.New("engine already closed")

// Config 是引擎配置。
type Config struct {
	// ShardCount 并发分片数（按 key 哈希）；<=0 时取 1。
	ShardCount int
	// WindowSizeMillis 窗口大小（毫秒，必须 >0）。
	WindowSizeMillis int64
	// WindowSlideMillis 滑动步长（毫秒）。<=0 或 >= WindowSizeMillis 时退化为
	// WindowSizeMillis，即与历史完全一致的滚动（tumbling）窗口；
	// 0 < 值 < WindowSizeMillis 时为重叠滑动窗口，一份事件同时进入覆盖它的每个窗口。
	WindowSlideMillis int64
	// OutOfOrderMillis 水位线乱序容忍：WM = maxSeen - OutOfOrderMillis。
	OutOfOrderMillis int64
	// AllowedLatenessMillis 窗口 Fire 后仍接受迟到修正的宽限期；
	// watermark >= end+AllowedLateness 后窗口清理，再到的事件判迟到丢弃。
	AllowedLatenessMillis int64
	// MaxKeys 每个 shard 活跃 key 数硬上限（内存边界）；<=0 表示不限。
	// 滑动窗口下已存在 key 的窗口扇出（每 key 至多 ceil(size/slide) 个）不占用新 key
	// 名额，只有全新 key 建窗才受此上限约束。
	MaxKeys int
}

func (c Config) normalized() Config {
	if c.ShardCount <= 0 {
		c.ShardCount = 1
	}
	if c.WindowSizeMillis <= 0 {
		c.WindowSizeMillis = 1
	}
	if c.WindowSlideMillis <= 0 || c.WindowSlideMillis >= c.WindowSizeMillis {
		// 步长缺省或不小于窗口大小：退化为滚动窗口，历史行为保持不变。
		c.WindowSlideMillis = c.WindowSizeMillis
	}
	if c.OutOfOrderMillis < 0 {
		c.OutOfOrderMillis = 0
	}
	if c.AllowedLatenessMillis < 0 {
		c.AllowedLatenessMillis = 0
	}
	return c
}

// Stats 暴露可观测计数器（快照值）。
type Stats struct {
	Processed      int64 `json:"processed"`       // 通过结构校验并被路由的事件数
	ResultsEmitted int64 `json:"results_emitted"` // 输出窗口结果条数（含 UPDATE 版本）
	DroppedLate    int64 `json:"dropped_late"`    // 超容忍迟到
	DroppedDup     int64 `json:"dropped_dup"`     // 重复事件
	DroppedMemory  int64 `json:"dropped_memory"`  // 内存上限拒绝
	ParseErrors    int64 `json:"parse_errors"`    // 结构性非法输入
	PurgedWindows  int64 `json:"purged_windows"`  // 宽限期满被清理的窗口数
	ActiveWindows  int   `json:"active_windows"`  // 当前存活窗口数
}

// Logger 是引擎诊断日志接口（测试中可接 t.Logf，打印事件时间/水位线/窗口判定）。
type Logger interface {
	Logf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Logf(string, ...any) {}

// Engine 是流处理引擎。
type Engine struct {
	cfg    Config
	sink   *endpoint.Sink
	shards []*shard

	closeMu sync.RWMutex
	closed  bool

	processed      atomic.Int64
	resultsEmitted atomic.Int64
	droppedLate    atomic.Int64
	droppedDup     atomic.Int64
	droppedMemory  atomic.Int64
	parseErrors    atomic.Int64
	purgedWindows  atomic.Int64
}

// Option 配置引擎构造。
type Option func(*Engine)

// WithLogger 注入诊断日志器。
func WithLogger(l Logger) Option {
	return func(e *Engine) {
		for _, s := range e.shards {
			s.logf = l.Logf
		}
	}
}

// New 创建引擎。
func New(cfg Config, sink *endpoint.Sink, opts ...Option) *Engine {
	cfg = cfg.normalized()
	e := &Engine{cfg: cfg, sink: sink}
	e.shards = make([]*shard, cfg.ShardCount)
	for i := range e.shards {
		e.shards[i] = newShard(i, cfg, nil)
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// shardOf 按 key 稳定哈希选择分片。
func (e *Engine) shardOf(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return e.shards[int(h.Sum32())%e.cfg.ShardCount]
}

// Submit 提交一条事件（并发安全）。
// 结构性非法输入（缺字段/非法时间戳/NaN 等）不会返回错误中断流程，而是被旁路记录到
// sink 并计入 ParseErrors，且不触碰任何窗口状态；调用方仅在引擎已关闭时收到错误。
func (e *Engine) Submit(_ context.Context, ev event.Event) error {
	if err := event.Validate(ev); err != nil {
		ie, _ := event.AsInvalidEvent(err)
		e.sink.CollectInvalid(ie)
		e.parseErrors.Add(1)
		return nil
	}

	e.closeMu.RLock()
	if e.closed {
		e.closeMu.RUnlock()
		return ErrEngineClosed
	}
	s := e.shardOf(ev.Key)
	e.closeMu.RUnlock()

	out := s.submit(ev)
	for _, r := range out.results {
		e.sink.CollectResult(r)
		e.resultsEmitted.Add(1)
	}
	if out.purged > 0 {
		e.purgedWindows.Add(int64(out.purged))
	}
	// 计数器与 Drop 信号均按"窗口归属"计：滚动窗口每事件至多 1 个归属（与历史口径
	// 一致）；滑动窗口下同一事件在它命中的每个窗口里各自独立结算，互不串味。
	e.processed.Add(int64(out.accepted))
	for _, d := range out.dups {
		e.droppedDup.Add(1)
		e.sink.CollectDrop(window.Drop{Ev: ev, Reason: window.ReasonDuplicate, WindowStart: d.start, WindowEnd: d.end})
	}
	for _, d := range out.lates {
		e.droppedLate.Add(1)
		e.sink.CollectDrop(window.Drop{Ev: ev, Reason: DropLate, WindowStart: d.start, WindowEnd: d.end})
	}
	for _, d := range out.memory {
		e.droppedMemory.Add(1)
		e.sink.CollectDrop(window.Drop{Ev: ev, Reason: DropMemoryLimit, WindowStart: d.start, WindowEnd: d.end})
	}
	return nil
}

// Drain 让各分片基于已观察事件触发到点窗口。本实现中窗口在每次 Submit 观察到水位线
// 推进后即已判定输出，Drain 提供显式同步点（典型用于无新事件时断言）。
func (e *Engine) Drain() {
	for _, s := range e.shards {
		rs := s.drain()
		for _, r := range rs {
			e.sink.CollectResult(r)
			e.resultsEmitted.Add(1)
		}
	}
}

// Close 冲刷所有未到点但仍存活的窗口（有界停机输出），之后拒绝新提交。
// 已 Fire 的窗口不会重复 Fire。
func (e *Engine) Close() error {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	for _, s := range e.shards {
		for _, r := range s.flushAll() {
			e.sink.CollectResult(r)
			e.resultsEmitted.Add(1)
		}
	}
	return nil
}

// Stats 返回计数器与存活窗口数的快照。
func (e *Engine) Stats() Stats {
	active := 0
	for _, s := range e.shards {
		active += s.activeWindows()
	}
	return Stats{
		Processed:      e.processed.Load(),
		ResultsEmitted: e.resultsEmitted.Load(),
		DroppedLate:    e.droppedLate.Load(),
		DroppedDup:     e.droppedDup.Load(),
		DroppedMemory:  e.droppedMemory.Load(),
		ParseErrors:    e.parseErrors.Load(),
		PurgedWindows:  e.purgedWindows.Load(),
		ActiveWindows:  active,
	}
}
