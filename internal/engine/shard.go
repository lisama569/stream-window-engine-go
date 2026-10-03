package engine

import (
	"fmt"
	"sort"
	"strings"
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
		spec:     window.Spec{SizeMillis: cfg.WindowSizeMillis, SlideMillis: cfg.WindowSlideMillis},
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

// getOrCreate 在持锁状态下取/建聚合。容量上限不由本函数判定：滑动窗口下一个
// 已存在的 key 扇出到新窗口不应占用新 key 名额，统一由 submit 的容量闸门在
// "全新 key 且需要新建窗口"时判定。调用方负责先完成容量校验。
func (s *shard) getOrCreate(key string, start int64) (*window.Agg, bool) {
	ks, ok := s.windows[start]
	if !ok {
		ks = make(map[string]*window.Agg)
		s.windows[start] = ks
	}
	if a, ok := ks[key]; ok {
		return a, false
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

// keyExistsLocked 报告 key 当前是否持有任意存活窗口。调用方持锁。
func (s *shard) keyExistsLocked(key string) bool {
	for _, ks := range s.windows {
		if _, ok := ks[key]; ok {
			return true
		}
	}
	return false
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

// dropRef 记录一次按窗口归属的丢弃处置。
type dropRef struct {
	start int64
	end   int64
}

// submitOutcome 是一次事件处理的确定性产物。
// 滑动窗口下一条事件同时归属多个窗口，各窗口独立处置：
// accepted/dup/late/memory 均按"窗口归属"计数（滚动窗口下每事件至多 1 个归属，
// 与历史口径完全一致）。
type submitOutcome struct {
	results  []window.Result // 本次产生的窗口输出（顺序确定）
	accepted int             // 被窗口接受的归属数
	dups     []dropRef       // 重复归属
	lates    []dropRef       // 超宽限期迟到归属
	memory   []dropRef       // 容量上限拒绝归属
	purged   int             // 本次顺带清理的过宽限期窗口数
}

// submit 处理一条已通过结构校验的事件。
func (s *shard) submit(ev event.Event) submitOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()

	starts := s.spec.StartsFor(ev.EventTime)
	wmBefore := s.wm.Current()
	lateLine := func(end int64) bool { return wmBefore > end+s.cfg.AllowedLatenessMillis }

	winDesc := func() string {
		parts := make([]string, len(starts))
		for i, st := range starts {
			parts[i] = fmt.Sprintf("[%d,%d)", st, s.spec.End(st))
		}
		return strings.Join(parts, ",")
	}
	s.log("[shard=%d] INPUT id=%s key=%s eventTime=%d -> windows=%s watermark=%d lateByWM=%v",
		s.id, ev.ID, ev.Key, ev.EventTime, winDesc(), wmBefore, wmBefore > ev.EventTime)

	// 1) 观察事件时间，推进单调水位线（只进不退；幂等）。
	s.wm.Observe(ev.EventTime)

	var out submitOutcome

	// 2) 窗口级处置。先挑出已过宽限期的归属：这类归属对应的聚合（若存在）在本事件
	//    之前的上一次提交时就应被清理；防御性地不触碰任何状态。若存在此类归属
	//    （典型为单窗口滚动场景），沿用历史严格次序：先触发到点窗口、后清理，再丢弃，
	//    且重复检查在迟到判定之后——保证滚动窗口行为与历史版本逐字节一致。
	var live []int64
	for _, st := range starts {
		end := s.spec.End(st)
		if lateLine(end) {
			out.lates = append(out.lates, dropRef{start: st, end: end})
		} else {
			live = append(live, st)
		}
	}
	if len(out.lates) > 0 {
		out.results = append(out.results, s.fireDueLocked()...)
		out.purged += len(s.purgeExpiredLocked())
	}

	// 3) 已有聚合的归属：尝试计入，重复 ID 只在该窗口内判重（各窗口去重集合独立）。
	var needCreate []int64
	var acceptedAggs []*window.Agg
	for _, st := range live {
		a := s.lookupAgg(ev.Key, st)
		if a == nil {
			needCreate = append(needCreate, st)
			continue
		}
		if ok, _ := a.Add(ev); !ok {
			out.dups = append(out.dups, dropRef{start: st, end: s.spec.End(st)})
			s.log("[shard=%d] DROP id=%s reason=%s key=%s window=[%d,%d) 重复事件：水位线幂等观察=%d，不计数/不输出",
				s.id, ev.ID, window.ReasonDuplicate, ev.Key, st, s.spec.End(st), s.wm.Current())
			continue
		}
		acceptedAggs = append(acceptedAggs, a)
	}

	// 4) 需要新建聚合的归属：统一受 MaxKeys 内存上限约束。
	//    全部需要的新归属都属于同一个 key，只要该 key 尚未存在，腾出 1 个名额即可
	//    全部放下；先触发到点窗口并回收过期窗口，仍不足才拒绝（可观测、可区分），
	//    拒绝发生在任何 Add 之前，绝不半计入。
	if len(needCreate) > 0 && s.cfg.MaxKeys > 0 && !s.keyExistsLocked(ev.Key) && s.activeKeysLocked() >= s.cfg.MaxKeys {
		fired := s.fireDueLocked()
		purged := s.purgeExpiredLocked()
		out.results = append(out.results, fired...)
		out.purged += len(purged)
		if s.activeKeysLocked() >= s.cfg.MaxKeys {
			for _, st := range needCreate {
				out.memory = append(out.memory, dropRef{start: st, end: s.spec.End(st)})
			}
			s.log("[shard=%d] DROP id=%s reason=%s activeKeys 已达上限 %d (回收 %d 窗口后仍不足) windows=%s",
				s.id, ev.ID, DropMemoryLimit, s.cfg.MaxKeys, out.purged, winDesc())
			needCreate = nil
		}
	}
	for _, st := range needCreate {
		a, _ := s.getOrCreate(ev.Key, st)
		if ok, _ := a.Add(ev); !ok {
			out.dups = append(out.dups, dropRef{start: st, end: s.spec.End(st)})
			continue
		}
		acceptedAggs = append(acceptedAggs, a)
	}

	// 5) 被接受的归属：已关闭窗口产出 UPDATE（宽限期内迟到修正），各窗口独立。
	//    starts 升序，同一事件触发的 UPDATE 按窗口起点从小到大输出。
	for _, st := range starts {
		for _, a := range acceptedAggs {
			if a.Start() != st {
				continue
			}
			if a.Closed() {
				s.log("[shard=%d] LATE-WITHIN-TOLERANCE id=%s eventTime=%d 修正窗口 [%d,%d)",
					s.id, ev.ID, ev.EventTime, st, s.spec.End(st))
				out.results = append(out.results, s.emitLocked(a, window.Update))
			}
		}
	}

	// 6) 触发所有到点窗口（按起点、key 确定顺序），随后清理过宽限期窗口。
	//    重叠窗口下一次水位线推进可能让多个窗口同时到点，输出顺序因此完全确定。
	//    第 2/4 步若已为特定处置路径执行过 fire/purge，这里不重复执行（Close/Purge 幂等，
	//    但输出顺序必须与滚动窗口历史路径一致：迟到丢弃分支早已 fire 过）。
	fired := len(out.lates) > 0
	purged := fired
	if len(out.memory) > 0 {
		fired = true
		purged = true
	}
	if !fired {
		out.results = append(out.results, s.fireDueLocked()...)
	}
	if !purged {
		out.purged += len(s.purgeExpiredLocked())
	}

	for _, d := range out.lates {
		s.log("[shard=%d] DROP id=%s reason=%s eventTime=%d watermark=%d (window=[%d,%d) 宽限至 %d)",
			s.id, ev.ID, DropLate, ev.EventTime, s.wm.Current(), d.start, d.end, d.end+s.cfg.AllowedLatenessMillis)
	}

	out.accepted = len(acceptedAggs)
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
