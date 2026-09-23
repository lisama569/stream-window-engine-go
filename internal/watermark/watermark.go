// Package watermark 实现基于事件时间的周期性水位线（bounded out-of-orderness）。
//
// 语义约定（毫秒）：
//   - 观察到的最大事件时间记为 maxSeen；水位线 WM = maxSeen - maxOutOfOrderness，
//     且以 startMillis 为下限（避免无事件时水位线无意义）。
//   - 水位线【只进不退】：任何乱序/迟到事件都不会让 WM 倒退；只有当新事件时间
//     推高 maxSeen、使 WM 候选值严格大于当前值时，水位线才前进一步（Observe 返回
//     true），重复或更旧的时间戳返回 false。
//   - IsLate(t)：事件时间 t < 当前 WM，即该事件在水位线越过其时间戳后才到达。
//     是否最终丢弃由 window 层结合 allowedLateness 决定，本包只给出判定。
package watermark

// Watermark 跟踪单个分区上看到的最大事件时间并推导单调水位线。
type Watermark struct {
	maxSeen     int64
	outOfOrder  int64 // 允许的乱序容忍量（毫秒，>=0）
	current     int64 // 当前水位线（毫秒）
	startMillis int64 // 水位线下限
	initialized bool
}

// New 创建水位线。outOfOrderMillis 为乱序容忍；startMillis 为无事件/早期的水位线下限。
func New(outOfOrderMillis int64, startMillis int64) *Watermark {
	if outOfOrderMillis < 0 {
		outOfOrderMillis = 0
	}
	return &Watermark{
		outOfOrder:  outOfOrderMillis,
		current:     startMillis,
		startMillis: startMillis,
	}
}

// Observe 观察一个新事件时间。
// 仅当它使水位线严格前进时返回 true；乱序/重复/更旧的时间戳返回 false。
func (w *Watermark) Observe(eventTime int64) bool {
	if !w.initialized || eventTime > w.maxSeen {
		if !w.initialized {
			w.maxSeen = eventTime
			w.initialized = true
		} else {
			w.maxSeen = eventTime
		}
	}
	candidate := w.maxSeen - w.outOfOrder
	if candidate < w.startMillis {
		candidate = w.startMillis
	}
	if candidate > w.current {
		w.current = candidate
		return true
	}
	return false
}

// Current 返回当前水位线（毫秒）。
func (w *Watermark) Current() int64 { return w.current }

// MaxSeen 返回观察到的最大事件时间；未观察到任何事件时返回 startMillis。
func (w *Watermark) MaxSeen() int64 {
	if !w.initialized {
		return w.startMillis
	}
	return w.maxSeen
}

// Initialized 报告是否已观察到至少一个事件。
func (w *Watermark) Initialized() bool { return w.initialized }

// IsLate 判断事件时间是否已落后于当前水位线（t < WM）。
func (w *Watermark) IsLate(eventTime int64) bool { return eventTime < w.current }

// Snapshot 是水位线状态的可序列化快照。
type Snapshot struct {
	MaxSeen     int64
	OutOfOrder  int64
	Current     int64
	StartMillis int64
	Initialized bool
}

func (w *Watermark) Snapshot() Snapshot {
	return Snapshot{w.maxSeen, w.outOfOrder, w.current, w.startMillis, w.initialized}
}

// Restore 从快照恢复水位线。
func Restore(s Snapshot) *Watermark {
	return &Watermark{
		maxSeen:     s.MaxSeen,
		outOfOrder:  s.OutOfOrder,
		current:     s.Current,
		startMillis: s.StartMillis,
		initialized: s.Initialized,
	}
}
