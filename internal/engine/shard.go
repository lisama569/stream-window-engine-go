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
		spec:     window.Spec{SizeMillis: cfg.WindowSizeMillis, SlideMillis: cfg.SlideMillis},
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
// 返回 (agg, rejected)：触发内存上限（MaxKeys 或 MaxWindows）时 rejected=true。
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
	if s.cfg.MaxWindows > 0 && s.activeWindowsLocked() >= s.cfg.MaxWindows {
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
	results   []window.Result // 本次产生的全部输出（迟到 UPDATE + 到点 FIRE，确定顺序）
	drops     []window.Drop   // 按窗口分别记录的未计入处置（原因互斥可区分）
	purged    int             // 本次顺带清理的过宽限期窗口数
	acceptedN int             // 本次事件真正计入的窗口数（>=1 即 processed）
}

// submit 处理一条已通过结构校验的事件。
func (s *shard) submit(ev event.Event) submitOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()

	targets := s.spec.Starts(ev.EventTime)

	s.log("[shard=%d] INPUT id=%s key=%s eventTime=%d watermark=%d 命中窗口数=%d %v",
		s.id, ev.ID, ev.Key, ev.EventTime, s.wm.Current(), len(targets), s.bounds(targets))

	// 1) 观察事件时间，推进单调水位线（只进不退）。
	s.wm.Observe(ev.EventTime)

	// 2) 先触发到点窗口、再清理过宽限期窗口（严格"先触发、后清理"）。
	//    滚动窗口下与原行为一致：迟到判定时旧窗口已被 Fire/PURGE；容量判定时也已尽力回收。
	fired := s.fireDueLocked()
	purged := s.purgeExpiredLocked()

	// 3) 事件逐个进入覆盖它的窗口；各窗口独立聚合、独立迟到/重复/容量判定，互不串味。
	var updates []window.Result
	var drops []window.Drop
	acceptedN := 0
	wm := s.wm.Current()
	for _, start := range targets {
		end := s.spec.End(start)
		// 3a) 过宽限期：该窗口在第 2 步已被清理（或从未创建），绝不重建。
		//     同事件的其他覆盖窗口不受影响，各自照常处理。
		if wm > end+s.cfg.AllowedLatenessMillis {
			s.log("[shard=%d] DROP id=%s reason=%s eventTime=%d watermark=%d (window=[%d,%d) 宽限至 %d)",
				s.id, ev.ID, DropLate, ev.EventTime, wm, start, end, end+s.cfg.AllowedLatenessMillis)
			drops = append(drops, window.Drop{Ev: ev, Reason: DropLate, WindowStart: start, WindowEnd: end})
			continue
		}
		// 3b) 取既有聚合或新建（受容量上限约束；第 2 步已尽力回收，这里仍不足才拒绝本窗口）。
		a := s.lookupAgg(ev.Key, start)
		if a == nil {
			a2, rejected := s.getOrCreate(ev.Key, start)
			if rejected {
				s.log("[shard=%d] DROP id=%s reason=%s key=%s window=[%d,%d) activeKeys/activeWindows 已达上限",
					s.id, ev.ID, DropMemoryLimit, ev.Key, start, end)
				drops = append(drops, window.Drop{Ev: ev, Reason: DropMemoryLimit, WindowStart: start, WindowEnd: end})
				continue
			}
			a = a2
		}
		// 3c) 同一事件 ID 在同一 (key,window) 内只计一次；重复除幂等水位线观察外无副作用：
		//     不计数、不产生 FIRE/UPDATE。
		if ok, reason := a.Add(ev); !ok {
			s.log("[shard=%d] DROP id=%s reason=%s key=%s window=[%d,%d) 重复事件：不计数/不输出",
				s.id, ev.ID, reason, ev.Key, start, end)
			drops = append(drops, window.Drop{Ev: ev, Reason: reason, WindowStart: start, WindowEnd: end})
			continue
		}
		// 3d) 已 Fire 窗口在宽限期内被修正 -> 该窗口独立产出一条递增 UPDATE。
		if a.Closed() {
			s.log("[shard=%d] LATE-WITHIN-TOLERANCE id=%s eventTime=%d 修正窗口 [%d,%d)",
				s.id, ev.ID, ev.EventTime, start, end)
			updates = append(updates, s.emitLocked(a, window.Update))
		}
		acceptedN++
	}

	// 输出顺序确定性：本事件的迟到修正（窗口起点升序）在前，到点 FIRE
	// （按 (start,key) 排序，见 fireDueLocked）在后。滚动窗口下至多一条 UPDATE
	// 且其窗口即原目标窗口，与老行为逐字节一致。
	out := append(updates, fired...)
	return submitOutcome{results: out, drops: drops, purged: len(purged), acceptedN: acceptedN}
}

// bounds 生成日志用的窗口边界描述。
func (s *shard) bounds(starts []int64) [][2]int64 {
	out := make([][2]int64, 0, len(starts))
	for _, st := range starts {
		out = append(out, [2]int64{st, s.spec.End(st)})
	}
	return out
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

// activeWindows 返回当前存活窗口总数（跨 key 的 (window,key) 条目数）。
func (s *shard) activeWindows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeWindowsLocked()
}

// activeWindowsLocked 统计存活 (window,key) 条目数。调用方持锁。
func (s *shard) activeWindowsLocked() int {
	n := 0
	for _, ks := range s.windows {
		n += len(ks)
	}
	return n
}
