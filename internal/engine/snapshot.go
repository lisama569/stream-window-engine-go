package engine

import (
	"fmt"
	"sort"

	"github.com/lisama569/stream-window-engine-go/internal/watermark"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// shardSnapshot 是单个分片在某一时刻的一致性状态。
type shardSnapshot struct {
	ID        int                        `json:"id"`
	Watermark watermark.Snapshot         `json:"watermark"`
	Windows   []window.AggSnapshot       `json:"windows"`  // 窗口聚合（含去重 ID 集合）
	Versions  map[string]map[int64]int64 `json:"versions"` // key -> windowStart -> 已输出版本
}

// Snapshot 是引擎整体的可序列化状态快照。
type Snapshot struct {
	Config Config          `json:"config"`
	Shards []shardSnapshot `json:"shards"`

	// 计数器一并持久化：重放恢复后计数口径与快照点保持一致。
	Stats Stats `json:"stats"`
}

// SaveSnapshot 生成一致性快照。
// 逐分片加锁拷贝，保证快照内水位线与窗口状态相互对应；并发提交不会读到撕裂状态。
func (e *Engine) SaveSnapshot() *Snapshot {
	e.closeMu.RLock()
	defer e.closeMu.RUnlock()

	snap := &Snapshot{Config: e.cfg, Shards: make([]shardSnapshot, len(e.shards)), Stats: e.Stats()}
	for i, s := range e.shards {
		s.mu.Lock()
		aggs := make([]window.AggSnapshot, 0)
		for _, ks := range s.windows {
			for _, a := range ks {
				aggs = append(aggs, a.Snapshot())
			}
		}
		// 排序使快照字节级确定，便于比对/复现。
		sort.Slice(aggs, func(i, j int) bool {
			if aggs[i].Start != aggs[j].Start {
				return aggs[i].Start < aggs[j].Start
			}
			return aggs[i].Key < aggs[j].Key
		})
		versions := make(map[string]map[int64]int64, len(s.versions))
		for k, vm := range s.versions {
			cp := make(map[int64]int64, len(vm))
			for st, v := range vm {
				cp[st] = v
			}
			versions[k] = cp
		}
		snap.Shards[i] = shardSnapshot{
			ID:        s.id,
			Watermark: s.wm.Snapshot(),
			Windows:   aggs,
			Versions:  versions,
		}
		s.mu.Unlock()
	}
	return snap
}

// Restore 从快照恢复引擎：水位线、窗口状态（含已消费事件 ID 集合）、输出版本号与
// 计数器全部回到快照点。恢复后重放快照点之前已消费的数据时，这些事件会被识别为
// DUPLICATE_EVENT 而不会再次计数，也不会产生重复输出，因此"不重不漏"。
//
// 窗口划分（大小/步长）必须与生成快照时一致：不同划分下同一 windowStart 语义不同，
// 混用会造成窗口错位，故此处显式拒绝（分片数、容忍参数等可观测口径以引擎当前配置为准）。
func (e *Engine) Restore(snap *Snapshot) error {
	if snap == nil {
		return fmt.Errorf("snapshot is nil")
	}
	snapSlide := snap.Config.SlideMillis
	if snapSlide == 0 {
		snapSlide = snap.Config.WindowSizeMillis // 旧版本快照无 slide 字段：零值即滚动
	}
	if snap.Config.WindowSizeMillis != e.cfg.WindowSizeMillis || snapSlide != e.cfg.SlideMillis {
		return fmt.Errorf("window spec mismatch: snapshot size=%d slide=%d, engine size=%d slide=%d",
			snap.Config.WindowSizeMillis, snapSlide, e.cfg.WindowSizeMillis, e.cfg.SlideMillis)
	}
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if len(snap.Shards) != len(e.shards) {
		return fmt.Errorf("shard count mismatch: snapshot=%d engine=%d", len(snap.Shards), len(e.shards))
	}
	for i, ss := range snap.Shards {
		s := e.shards[i]
		s.mu.Lock()
		s.wm = watermark.Restore(ss.Watermark)
		s.windows = make(map[int64]map[string]*window.Agg)
		for _, as := range ss.Windows {
			ks, ok := s.windows[as.Start]
			if !ok {
				ks = make(map[string]*window.Agg)
				s.windows[as.Start] = ks
			}
			ks[as.Key] = window.RestoreAgg(as)
		}
		s.versions = make(map[string]map[int64]int64, len(ss.Versions))
		for k, vm := range ss.Versions {
			cp := make(map[int64]int64, len(vm))
			for st, v := range vm {
				cp[st] = v
			}
			s.versions[k] = cp
		}
		s.mu.Unlock()
	}

	e.processed.Store(snap.Stats.Processed)
	e.resultsEmitted.Store(snap.Stats.ResultsEmitted)
	e.droppedLate.Store(snap.Stats.DroppedLate)
	e.droppedDup.Store(snap.Stats.DroppedDup)
	e.droppedMemory.Store(snap.Stats.DroppedMemory)
	e.parseErrors.Store(snap.Stats.ParseErrors)
	e.purgedWindows.Store(snap.Stats.PurgedWindows)
	return nil
}
