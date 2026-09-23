package engine

import (
	"sort"
	"sync"

	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/watermark"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// shard 是按 key 哈希划分的串行执行单元。同一 key 的所有事件进入同一 shard，
// shard 内用一把锁串行化，保证该 key 上水位线、窗口状态与输出版本的确定性；
// 不同 shard 之间互不阻塞，实现并行处理下的并发安全。
type shard struct {
	id   int
	cfg  Config
	spec window.Spec

	mu       sync.Mutex
	wm       *watermark.Watermark
	windows  map[int64]map[string]*window.Agg // windowStart -> key -> agg
	versions map[string]map[int64]int64       // key -> windowStart -> 已输出最大版本
	logf     func(format string, args ...any)
}

func newShard(id int, cfg Config, logf func(string, ...any)) *shard {
	return &shard{
		id:       id,
		cfg:      cfg,
		spec:     window.Spec{SizeMillis: cfg.WindowSizeMillis},
		wm:       watermark.New(cfg.OutOfOrderMillis, 0),
		windows:  make(map[int64]map[string]*window.Agg),
		versions: make(map[string]map[int64]int64),
		logf:     logf,
	}
}

func (s *shard) log(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
	}
}

// nextVersion 返回某 (key,window) 下一个输出版本号。
// 调用方必须持有 s.mu。
func (s *shard) nextVersion(key string, start int64) int64 {
	vm, ok := s.versions[key]
	if !ok {
		vm = make(map[int64]int64)
		s.versions[key] = vm
	}
	v := vm[start]
	vm[start] = v + 1
	return v
}

func (s *shard) version(key string, start int64) int64 {
	if vm, ok := s.versions[key]; ok {
		return vm[start]
	}
	return 0
}

// getOrCreate 在持锁状态下取/建聚合；受内存上限约束。
// 返回 (agg, rejected)：触发内存上限时 rejected=true。
func (s *shard) getOrCreate(key string, start int64) (*window.Agg, bool) {
	ks, ok := s.windows[start]
	if !ok {
		ks = make(map[string]*window.Agg)
		s.windows[start] = ks
	}
	if a, ok := ks[key]; ok {
		return a, false
	}
	if s.cfg.MaxKeys > 0 && s.activeKeysLocked() >= s.cfg.MaxKeys {
		return nil, true
	}
	end := s.spec.End(start)
	a := window.NewAgg(key, start, end)
	ks[key] = a
	return a, false
}

// activeKeysLocked 统计当前分片持有的不同 key 数。调用方持锁。
func (s *shard) activeKeysLocked() int {
	seen := make(map[string]struct{})
	for _, ks := range s.windows {
		for k := range ks {
			seen[k] = struct{}{}
		}
	}
	return len(seen)
}

// emitLocked 输出一个窗口结果。调用方持锁。
func (s *shard) emitLocked(a *window.Agg, kind window.Kind) window.Result {
	v := s.nextVersion(a.Key(), a.Start())
	r := a.ToResult(kind, v)
	s.log("[shard=%d] OUTPUT kind=%s key=%s window=[%d,%d) version=%d sum=%v count=%d watermark=%d",
		s.id, kind, r.Key, r.WindowStart, r.WindowEnd, v, r.Sum, r.Count, s.wm.Current())
	return r
}

// purgeExpiredLocked 清理宽限期已过（WM > end+AllowedLateness）的窗口。
// 调用方持锁。能满足该条件的窗口必然早已 Fire（Fire 只需 WM >= end），因此清理
// 永远不会吞掉尚未输出的窗口；清理紧跟在每次 fireDueLocked 之后执行。
func (s *shard) purgeExpiredLocked() []purgeRef {
	wm := s.wm.Current()
	var purged []purgeRef
	for start, ks := range s.windows {
		end := s.spec.End(start)
		if wm <= end+s.cfg.AllowedLatenessMillis {
			continue // 仍在宽限期内（含边界），保留以备迟到修正
		}
		for key := range ks {
			s.log("[shard=%d] PURGE key=%s window=[%d,%d) watermark=%d 已过宽限期(%dms)",
				s.id, key, start, end, wm, s.cfg.AllowedLatenessMillis)
			purged = append(purged, purgeRef{key: key, start: start})
			delete(ks, key)
		}
		if len(ks) == 0 {
			delete(s.windows, start)
		}
	}
	return purged
}

type purgeRef struct {
	key   string
	start int64
}

// fireDueLocked 关闭并输出所有 end <= watermark 且尚未关闭的窗口（按 start、key 确定顺序）。
// 调用方持锁。
func (s *shard) fireDueLocked() []window.Result {
	wm := s.wm.Current()
	starts := make([]int64, 0, len(s.windows))
	for st := range s.windows {
		starts = append(starts, st)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	var out []window.Result
	for _, st := range starts {
		end := s.spec.End(st)
		if wm < end {
			continue // 水位线未到窗口右端：不得提前输出
		}
		ks := s.windows[st]
		keys := make([]string, 0, len(ks))
		for k := range ks {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			a := ks[k]
			if a.Closed() {
				continue // 已触发过 Fire 的窗口绝不重复 Fire
			}
			a.Close()
			out = append(out, s.emitLocked(a, window.Fire))
		}
	}
	return out
}

// submitOutcome 是一次事件处理的确定性产物。
type submitOutcome struct {
	results []window.Result
	reason  string // 空字符串表示事件被计入
	purged  int    // 本次顺带清理的过宽限期窗口数
}

// submit 处理一条已通过结构校验的事件。
func (s *shard) submit(ev event.Event) submitOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()

	start := s.spec.Start(ev.EventTime)
	end := s.spec.End(start)
	lateByWM := s.wm.IsLate(ev.EventTime)

	s.log("[shard=%d] INPUT id=%s key=%s eventTime=%d -> window=[%d,%d) watermark=%d lateByWM=%v",
		s.id, ev.ID, ev.Key, ev.EventTime, start, end, s.wm.Current(), lateByWM)

	// 1) 观察事件时间，推进单调水位线（只进不退）。
	s.wm.Observe(ev.EventTime)

	// 2) 若目标窗口已超过宽限期（含到点未输出的极端情形不可能发生：到点窗口会在
	//    上一步推进后立刻 Fire），迟到丢弃。
	//    先把所有到点窗口 Fire，保证"先触发、后清理"的严格次序。
	if s.wm.Current() > end+s.cfg.AllowedLatenessMillis {
		out := s.fireDueLocked()
		purged := s.purgeExpiredLocked()
		s.log("[shard=%d] DROP id=%s reason=%s eventTime=%d watermark=%d (window=[%d,%d) 宽限至 %d)",
			s.id, ev.ID, DropLate, ev.EventTime, s.wm.Current(), start, end, end+s.cfg.AllowedLatenessMillis)
		return submitOutcome{results: out, reason: DropLate, purged: len(purged)}
	}

	// 2.5) 若该事件 ID 已计入目标窗口（典型场景：快照恢复后重放已消费数据），
	// 它是重复事件：第 1 步已幂等观察其时间戳（WM 只进不退），此后不得再有任何
	// 副作用——不重复计数、不触发 Fire/Update。这样重放历史数据既能把水位线重建
	// 到快照点，又保证输出不重不漏。
	if a0 := s.lookupAgg(ev.Key, start); a0 != nil {
		if _, dupReason := a0.Add(ev); dupReason != "" {
			s.log("[shard=%d] DROP id=%s reason=%s key=%s window=[%d,%d) 重复事件：水位线已幂等观察=%d，不计数/不输出",
				s.id, ev.ID, dupReason, ev.Key, start, end, s.wm.Current())
			return submitOutcome{reason: dupReason}
		}
		// 非重复：Add 已生效，a0 即目标聚合，后续走正常关闭/触发路径。
		return s.routeAccepted(ev, a0, start, end)
	}

	// 3) 目标窗口尚不存在：受 MaxKeys 内存上限约束（先 Fire 到点窗口，再尽力回收）。
	a, rejected := s.getOrCreate(ev.Key, start)
	if rejected {
		out := s.fireDueLocked()
		purged := s.purgeExpiredLocked()
		// 回收后若腾出位置则放行本次事件，否则才按内存上限拒绝（可观测、可区分）。
		a2, rej2 := s.getOrCreate(ev.Key, start)
		if rej2 {
			s.log("[shard=%d] DROP id=%s reason=%s activeKeys 已达上限 %d (回收 %d 窗口后仍不足)",
				s.id, ev.ID, DropMemoryLimit, s.cfg.MaxKeys, len(purged))
			return submitOutcome{results: out, reason: DropMemoryLimit, purged: len(purged)}
		}
		a = a2
	}
	// 新建窗口中没有历史 ID，Add 必被接受。
	if ok, reason := a.Add(ev); !ok {
		return submitOutcome{reason: reason} // 理论不可达，保守处理且不产生输出
	}
	return s.routeAccepted(ev, a, start, end)
}

// routeAccepted 处理一条已成功计入聚合的事件：已关闭窗口 -> 递增 UPDATE；
// 随后按确定顺序 Fire 到点窗口，最后清理过宽限期窗口。调用方持锁。
func (s *shard) routeAccepted(ev event.Event, a *window.Agg, start, end int64) submitOutcome {
	var lateUpdate *window.Result
	if a.Closed() {
		s.log("[shard=%d] LATE-WITHIN-TOLERANCE id=%s eventTime=%d 修正窗口 [%d,%d)",
			s.id, ev.ID, ev.EventTime, start, end)
		r := s.emitLocked(a, window.Update)
		lateUpdate = &r
	}
	out := s.fireDueLocked()
	if lateUpdate != nil {
		out = append([]window.Result{*lateUpdate}, out...)
	}
	purged := s.purgeExpiredLocked()
	return submitOutcome{results: out, reason: "", purged: len(purged)}
}

// lookupAgg 在不创建窗口的前提下查找聚合；不存在返回 nil。调用方持锁。
func (s *shard) lookupAgg(key string, start int64) *window.Agg {
	if ks, ok := s.windows[start]; ok {
		return ks[key]
	}
	return nil
}

// flushAll 关闭引擎时冲刷：把所有仍存活（含尚未到点）的窗口输出一次。
// 已 Fire 的窗口只可能产出其宽限期内迟到修正；未关闭窗口在此统一 Fire。
func (s *shard) flushAll() []window.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []window.Result
	starts := make([]int64, 0, len(s.windows))
	for st := range s.windows {
		starts = append(starts, st)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for _, st := range starts {
		ks := s.windows[st]
		keys := make([]string, 0, len(ks))
		for k := range ks {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			a := ks[k]
			if !a.Closed() {
				a.Close()
				out = append(out, s.emitLocked(a, window.Fire))
			}
		}
	}
	return out
}

// drain 显式触发所有到点但尚未关闭的窗口（不依赖新事件）。
func (s *shard) drain() []window.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fireDueLocked()
}

// activeWindows 返回当前存活窗口总数（跨 key 去重前的 (window,key) 条目数）。
func (s *shard) activeWindows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, ks := range s.windows {
		n += len(ks)
	}
	return n
}
