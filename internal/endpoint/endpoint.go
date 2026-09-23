// Package endpoint 提供本地内存事件数据源（模拟外部数据源）与本地汇聚出口
// （模拟数据出口）。两者均为并发安全：Source 可被多生产者写入、被引擎顺序消费；
// Sink 可被多个引擎分片并发写入，输出按接收顺序保留，保证结果可复现。
package endpoint

import (
	"sync"

	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// Source 是本地内存事件源。
type Source struct {
	mu     sync.Mutex
	events []event.Event
	closed bool
}

// NewSource 创建数据源。
func NewSource() *Source { return &Source{} }

// Emit 投入一条事件（线程安全）。
func (s *Source) Emit(e event.Event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

// Drain 取出并清空当前缓冲的全部事件（线程安全）。
func (s *Source) Drain() []event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.events
	s.events = nil
	return out
}

// Len 返回缓冲事件数。
func (s *Source) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// Sink 是本地汇聚出口，并发安全地收集确定性输出与可区分的异常信号。
type Sink struct {
	mu      sync.Mutex
	results []window.Result
	drops   []window.Drop
	invalid []*event.InvalidEvent
}

// NewSink 创建汇聚出口。
func NewSink() *Sink { return &Sink{} }

// CollectResult 收集一次窗口输出。
func (s *Sink) CollectResult(r window.Result) {
	s.mu.Lock()
	s.results = append(s.results, r)
	s.mu.Unlock()
}

// CollectDrop 收集一条被窗口处理拒绝的事件（迟到/重复/内存上限）。
func (s *Sink) CollectDrop(d window.Drop) {
	s.mu.Lock()
	s.drops = append(s.drops, d)
	s.mu.Unlock()
}

// CollectInvalid 收集一条结构性非法输入。
func (s *Sink) CollectInvalid(p *event.InvalidEvent) {
	s.mu.Lock()
	s.invalid = append(s.invalid, p)
	s.mu.Unlock()
}

// Results 返回按接收顺序排列的输出副本。
func (s *Sink) Results() []window.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]window.Result, len(s.results))
	copy(out, s.results)
	return out
}

// Drops 返回丢弃记录副本。
func (s *Sink) Drops() []window.Drop {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]window.Drop, len(s.drops))
	copy(out, s.drops)
	return out
}

// InvalidEvents 返回非法输入记录副本。
func (s *Sink) InvalidEvents() []*event.InvalidEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*event.InvalidEvent, len(s.invalid))
	copy(out, s.invalid)
	return out
}

// FinalResults 折叠版本化输出：对每个 (key, windowStart) 仅保留 Version 最大的
// 一条结果（迟到修正会产生更高版本），按窗口起点、key 排序，得到确定的最终视图。
func (s *Sink) FinalResults() []window.Result {
	all := s.Results()
	type kw struct {
		key   string
		start int64
	}
	latest := make(map[kw]window.Result, len(all))
	order := make([]kw, 0, len(all))
	for _, r := range all {
		k := kw{r.Key, r.WindowStart}
		if prev, ok := latest[k]; !ok {
			order = append(order, k)
			latest[k] = r
		} else if r.Version > prev.Version {
			latest[k] = r
		}
	}
	out := make([]window.Result, 0, len(order))
	for _, k := range order {
		out = append(out, latest[k])
	}
	return out
}
