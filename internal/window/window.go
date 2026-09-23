// Package window 实现滚动（tumbling）事件时间窗口的聚合状态。
//
// 窗口划分（左闭右开）：窗口 [start, end)，start = t - t%size，end = start + size。
// 关闭/触发判定由引擎结合水位线完成：当 watermark >= end 时窗口被关闭并输出 Fire；
// 关闭后、allowedLateness 宽限期内到达的迟到事件仍可修正该窗口，并输出 Version 递增
// 的 Update；宽限期过后窗口状态被清理（引擎通过 PurgedWindows 计数器可观测），之后
// 到达的事件由引擎按 LATE_BEYOND_TOLERANCE 观测为丢弃。
//
// 重复语义：同一事件 ID 在同一 (key, window) 内只生效一次，重复事件返回
// DUPLICATE_EVENT，绝不静默丢弃，也绝不重复计数。
package window

import (
	"sync"

	"github.com/lisama569/stream-window-engine-go/internal/event"
)

// 丢弃原因（可观测、可区分）。
const (
	ReasonDuplicate = "DUPLICATE_EVENT" // 同一事件 ID 在同一窗口重复到达
)

// Kind 标识一次窗口输出的触发语义。
type Kind string

const (
	Fire   Kind = "FIRE"   // 水位线关闭窗口时的首次（且唯一一次）最终窗口输出
	Update Kind = "UPDATE" // 宽限期内迟到事件对已关闭窗口的确定性修正输出
)

// Spec 描述滚动窗口划分。
type Spec struct {
	SizeMillis int64
}

// Start 返回事件时间所属窗口的起始时间（毫秒）。
func (s Spec) Start(t int64) int64 {
	if s.SizeMillis <= 0 || t < 0 {
		return 0
	}
	return t - (t % s.SizeMillis)
}

// End 返回给定窗口起点对应的结束时间（左闭右开）。
func (s Spec) End(start int64) int64 { return start + s.SizeMillis }

// Result 是一次窗口输出。Version 从 0（Fire）起，每次迟到修正严格递增，
// 下游据此识别同一窗口的前后版本：最大 Version 即为该窗口确定结果。
type Result struct {
	Key         string  `json:"key"`
	WindowStart int64   `json:"window_start"`
	WindowEnd   int64   `json:"window_end"`
	Sum         float64 `json:"sum"`
	Count       int64   `json:"count"`
	Kind        Kind    `json:"kind"`
	Version     int64   `json:"version"`
}

// Drop 记录一条未进入聚合的事件及其可区分原因。
type Drop struct {
	Ev     event.Event `json:"event"`
	Reason string      `json:"reason"`
}

// Agg 是单个 (key, window) 的聚合状态。所有方法并发安全。
type Agg struct {
	mu sync.Mutex

	key   string
	start int64
	end   int64

	sum   float64
	count int64
	seen  map[string]struct{}

	closed bool
}

// NewAgg 创建聚合状态。
func NewAgg(key string, start, end int64) *Agg {
	return &Agg{key: key, start: start, end: end, seen: make(map[string]struct{})}
}

// Key/Start/End 访问器。
func (a *Agg) Key() string  { return a.key }
func (a *Agg) Start() int64 { return a.start }
func (a *Agg) End() int64   { return a.end }

// Add 尝试把事件计入聚合。
// 重复 ID 返回 (false, ReasonDuplicate)；窗口是否关闭不影响 Add——是否把事件路由到
// 已过宽限期的窗口由引擎在更上层判定。
func (a *Agg) Add(e event.Event) (accepted bool, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[e.ID]; ok {
		return false, ReasonDuplicate
	}
	a.seen[e.ID] = struct{}{}
	a.sum += e.Value
	a.count++
	return true, ""
}

// Close 标记窗口关闭。重复 Close 是幂等的。
func (a *Agg) Close() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
}

// Closed 报告窗口是否已被水位线关闭。
func (a *Agg) Closed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

// ToResult 生成指定 Kind/Version 的确定性输出快照。
func (a *Agg) ToResult(kind Kind, version int64) Result {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Result{
		Key:         a.key,
		WindowStart: a.start,
		WindowEnd:   a.end,
		Sum:         a.sum,
		Count:       a.count,
		Kind:        kind,
		Version:     version,
	}
}

// AggSnapshot 是聚合状态的可序列化快照。
type AggSnapshot struct {
	Key    string   `json:"key"`
	Start  int64    `json:"start"`
	End    int64    `json:"end"`
	Sum    float64  `json:"sum"`
	Count  int64    `json:"count"`
	IDs    []string `json:"ids"`
	Closed bool     `json:"closed"`
}

// Snapshot 导出状态（拷贝，含去重 ID 集合）。
func (a *Agg) Snapshot() AggSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]string, 0, len(a.seen))
	for id := range a.seen {
		ids = append(ids, id)
	}
	return AggSnapshot{Key: a.key, Start: a.start, End: a.end, Sum: a.sum, Count: a.count, IDs: ids, Closed: a.closed}
}

// RestoreAgg 从快照恢复聚合状态。
func RestoreAgg(s AggSnapshot) *Agg {
	seen := make(map[string]struct{}, len(s.IDs))
	for _, id := range s.IDs {
		seen[id] = struct{}{}
	}
	return &Agg{
		key: s.Key, start: s.Start, end: s.End,
		sum: s.Sum, count: s.Count, seen: seen, closed: s.Closed,
	}
}
