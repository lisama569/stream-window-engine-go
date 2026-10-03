// Package window 实现事件时间窗口的划分与聚合状态，支持滚动（tumbling）与
// 滑动（sliding，亦称 hopping）两种划分，二者左闭右开、按纪元对齐：
//
//   - 滚动：窗口 [start, end)，start = t - t%size，end = start + size。
//   - 滑动：窗口起点网格为 slide 的整数倍（…, -slide, 0, slide, 2slide, …），
//     任一事件时间 t 同时落入所有满足 start <= t < start+size 的窗口；
//     窗口大小 size 与步长 slide 都可配（要求 0 < slide <= size）。
//     slide == size 时每个事件只命中一个窗口，划分结果与滚动窗口逐点相同，
//     即滚动是滑动在步长等于窗口大小时的退化情形。
//
// 关闭/触发判定由引擎结合水位线完成：当 watermark >= end 时窗口被关闭并输出 Fire；
// 关闭后、allowedLateness 宽限期内到达的迟到事件仍可修正该窗口，并输出 Version 递增
// 的 Update；宽限期过后窗口状态被清理（引擎通过 PurgedWindows 计数器可观测），之后
// 到达的事件由引擎按 LATE_BEYOND_TOLERANCE 观测为丢弃。
//
// 重复语义：同一事件 ID 在同一 (key, window) 内只生效一次，重复事件返回
// DUPLICATE_EVENT，绝不静默丢弃，也绝不重复计数。
package window

import (
	"sort"
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

// Spec 描述窗口划分：SizeMillis 为窗口大小，SlideMillis 为滑动步长。
// SlideMillis <= 0 或 == SizeMillis 时即滚动窗口（每个事件恰好命中一个窗口）。
type Spec struct {
	SizeMillis  int64
	SlideMillis int64
}

// slide 返回规范化后的有效步长：非正时退化为 size（滚动窗口）。
func (s Spec) slide() int64 {
	if s.SlideMillis <= 0 {
		return s.SizeMillis
	}
	return s.SlideMillis
}

// Start 返回事件时间在滚动划分（步长=大小）下所属窗口的起始时间（毫秒）。
// 为兼容滚动窗口语义保留；滑动划分请用 Starts。
func (s Spec) Start(t int64) int64 {
	if s.SizeMillis <= 0 || t < 0 {
		return 0
	}
	return t - (t % s.SizeMillis)
}

// Starts 返回覆盖事件时间 t 的全部窗口起点（左闭右开），按起点升序排列。
// 窗口起点是 slide 的整数倍（按纪元对齐）；由于事件时间域为 t >= 0，负起点
// 窗口（其右端 <= 0、不覆盖任何合法事件）不会出现在结果中。
// 当 slide == size（滚动退化）时结果恒为单元素，且等于 Start(t)。
func (s Spec) Starts(t int64) []int64 {
	if s.SizeMillis <= 0 || t < 0 {
		return nil
	}
	slide := s.slide()
	if slide > s.SizeMillis {
		slide = s.SizeMillis // 防御：引擎构造时已规范化，这里保证不越界
	}
	// 满足 start <= t 的最大网格点。
	latest := t - modNonNegative(t, slide)
	// 满足 start + size > t（严格：右端点不含 t）的最小网格点：
	// 先取 <= t-size 的最大网格点，再前进一格（t-size 自身对齐时也要跳过）。
	earliest := t - s.SizeMillis - modNonNegative(t-s.SizeMillis, slide) + slide
	n := (latest-earliest)/slide + 1
	if n <= 0 {
		return nil
	}
	starts := make([]int64, 0, n)
	for st := earliest; st <= latest; st += slide {
		if st >= 0 { // 事件时间域 t>=0：不创建纪元前起点的冗余窗口
			starts = append(starts, st)
		}
	}
	return starts
}

// modNonNegative 返回 Euclid 意义下的非负余数（被除数可能为负）。
func modNonNegative(a, m int64) int64 {
	r := a % m
	if r < 0 {
		r += m
	}
	return r
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

// Drop 记录一条未进入某窗口聚合的事件及其可区分原因。
// 滑动窗口下同一条事件可能同时命中多个窗口、各窗口处置不同，因此除事件外还携带
// 做出该处置的窗口坐标 [WindowStart, WindowEnd)；滚动窗口下即事件唯一所属窗口。
type Drop struct {
	Ev          event.Event `json:"event"`
	Reason      string      `json:"reason"`
	WindowStart int64       `json:"window_start"`
	WindowEnd   int64       `json:"window_end"`
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

// Snapshot 导出状态（拷贝，含去重 ID 集合）。ID 列表排序导出，保证同一状态
// 连续两次快照字节级一致（重叠窗口下每窗口各自一份集合，互不影响）。
func (a *Agg) Snapshot() AggSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]string, 0, len(a.seen))
	for id := range a.seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
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
