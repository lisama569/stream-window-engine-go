package engine

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// WindowResult is one emitted (or revised) window aggregation result.
type WindowResult struct {
	Window Window  `json:"window"`
	Key    string  `json:"key"`
	Value  float64 `json:"value"`
	Count  int64   `json:"count"`
	// Revised is false for the first emission of a window and true for every
	// later emission caused by an accepted late event.
	Revised bool `json:"revised"`
	// Watermark is the engine watermark at the moment of emission.
	Watermark int64 `json:"watermark"`
}

// Sink is the local convergence endpoint. Emit must be safe for concurrent
// use.
type Sink interface {
	Emit(r WindowResult)
}

// Stats summarizes engine counters (every processed input is accounted for
// in exactly one category).
type Stats struct {
	Accepted       int64
	Invalid        int64
	TooLate        int64
	Duplicate      int64
	MemoryLimited  int64
	Emitted        int64
	Revised        int64
	BackwardWMMark int64
}

type windowState struct {
	start int64
	key   string
	agg   Aggregate
	count int64
	seen  map[string]struct{}
	// values keeps accepted values in acceptance order for exact snapshot
	// rebuild. It is bounded by Config.MaxEventsPerWindow.
	values []float64
	// fired records whether the initial result was already emitted.
	fired bool
}

// ErrTooLate is returned (and reported) when an event is beyond the allowed
// lateness horizon. ErrDuplicate indicates a repeated ID. ErrMemoryLimit
// indicates a configured hard bound was hit. All three are observable rather
// than silently ignored.
var (
	ErrTooLate     = errors.New("event is beyond allowed lateness")
	ErrDuplicate   = errors.New("duplicate event id")
	ErrMemoryLimit = errors.New("memory limit exceeded")
)

// Engine is the core stream processing engine. All methods are safe for
// concurrent use; state mutations are serialized under one lock so the state
// evolution is identical to a sequential run of the same input prefix.
type Engine struct {
	mu        sync.Mutex
	cfg       Config
	wm        *WatermarkTracker
	windows   map[int64]map[string]*windowState
	maxSeenTS int64
	seq       int64
	sink      Sink
	stats     Stats
}

// NewEngine builds an engine from configuration. The watermark starts at -1,
// meaning "no event time observed yet"; it never moves backwards.
func NewEngine(cfg Config, sink Sink) (*Engine, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Aggregate == nil {
		cfg.Aggregate = func() Aggregate { return &SumAggregate{} }
	}
	if cfg.Reporter == nil {
		cfg.Reporter = NewMemoryDropReporter(1024)
	}
	if sink == nil {
		return nil, errors.New("sink is required")
	}
	return &Engine{
		cfg:     cfg,
		wm:      NewWatermarkTracker(-1),
		windows: make(map[int64]map[string]*windowState),
		sink:    sink,
	}, nil
}

// report records an observable, reason-tagged rejection.
func (e *Engine) report(rec DropRecord) {
	if e.cfg.Reporter != nil {
		e.cfg.Reporter.Report(rec)
	}
}

// ProcessEvent validates and incorporates one event, applies the
// duplicate / lateness / memory-limit policies, and emits any resulting
// window outputs. An invalid event is reported and returned as an error but
// never interrupts processing of subsequent events or touches other windows.
func (e *Engine) ProcessEvent(ev Event) error {
	if verr := ev.Validate(); verr != nil {
		e.mu.Lock()
		e.stats.Invalid++
		e.report(DropRecord{
			Reason: verr.Reason, EventID: ev.ID, Key: ev.Key,
			Timestamp: ev.Timestamp, Watermark: e.wm.Current(),
			Detail: verr.Detail,
		})
		e.mu.Unlock()
		return verr
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if ev.Timestamp > e.maxSeenTS {
		e.maxSeenTS = ev.Timestamp
	}

	w := TumblingWindow(ev.Timestamp, e.cfg.WindowSizeMillis)
	horizon := w.End + e.cfg.AllowedLatenessMillis
	if e.wm.Current() >= horizon {
		// Window state was purged: the event is unrecoverably too late. This
		// takes precedence over duplicate classification, since a purged
		// event's ID can no longer be remembered.
		e.stats.TooLate++
		e.report(DropRecord{
			Reason: DropTooLate, EventID: ev.ID, Key: ev.Key,
			Timestamp: ev.Timestamp, Watermark: e.wm.Current(),
			Detail: fmt.Sprintf("window [%d,%d) purged at watermark %d (horizon %d)",
				w.Start, w.End, e.wm.Current(), horizon),
		})
		return fmt.Errorf("%w: event %s ts=%d", ErrTooLate, ev.ID, ev.Timestamp)
	}

	bucket := e.windows[w.Start]
	var st *windowState
	if bucket != nil {
		st = bucket[ev.Key]
	}
	if st != nil {
		if _, dup := st.seen[ev.ID]; dup {
			e.stats.Duplicate++
			e.report(DropRecord{
				Reason: DropDuplicate, EventID: ev.ID, Key: ev.Key,
				Timestamp: ev.Timestamp, Watermark: e.wm.Current(),
				Detail: "event id already accepted into window",
			})
			return fmt.Errorf("%w: event %s", ErrDuplicate, ev.ID)
		}
	}

	// Hard memory bounds. New (window,key) states count toward the live-state
	// bound; a full per-window footprint rejects the event. Either way the
	// rejection is observable and the existing state stays untouched.
	if e.cfg.MaxEventsPerWindow > 0 && st != nil && int(st.count) >= e.cfg.MaxEventsPerWindow {
		e.stats.MemoryLimited++
		e.report(DropRecord{
			Reason: DropMemoryLimit, EventID: ev.ID, Key: ev.Key,
			Timestamp: ev.Timestamp, Watermark: e.wm.Current(),
			Detail: fmt.Sprintf("per-window event cap %d reached", e.cfg.MaxEventsPerWindow),
		})
		return fmt.Errorf("%w: per-window cap for key %s", ErrMemoryLimit, ev.Key)
	}
	if st == nil && e.cfg.MaxLiveWindows > 0 && e.countLiveStatesLocked() >= e.cfg.MaxLiveWindows {
		e.stats.MemoryLimited++
		e.report(DropRecord{
			Reason: DropMemoryLimit, EventID: ev.ID, Key: ev.Key,
			Timestamp: ev.Timestamp, Watermark: e.wm.Current(),
			Detail: fmt.Sprintf("live-state cap %d reached", e.cfg.MaxLiveWindows),
		})
		return fmt.Errorf("%w: live-state cap reached", ErrMemoryLimit)
	}

	if st == nil {
		st = &windowState{
			start: w.Start,
			key:   ev.Key,
			agg:   e.cfg.Aggregate(),
			seen:  make(map[string]struct{}),
		}
		if e.windows[w.Start] == nil {
			e.windows[w.Start] = make(map[string]*windowState)
		}
		e.windows[w.Start][ev.Key] = st
	}
	st.agg.Add(ev.Value)
	st.count++
	st.seen[ev.ID] = struct{}{}
	st.values = append(st.values, ev.Value)
	e.stats.Accepted++
	e.seq++

	var pending []WindowResult
	// Late event into an already-fired window: emit a deterministic revision.
	if st.fired {
		pending = append(pending, e.resultForLocked(st, true))
		e.stats.Revised++
	}
	// Only after a valid event is accepted may the bounded-delay heuristic
	// move the watermark (to maxSeen - delay), so the event itself is always
	// classified against the watermark as it was before arrival.
	e.advanceHeuristicLocked()
	// An event may close windows (it carries the maximum observed time).
	pending = append(pending, e.closeWindowsLocked()...)
	e.emitUnlocked(pending)
	return nil
}

// advanceHeuristicLocked moves the watermark to maxSeenTS - delay when the
// bounded-delay heuristic is enabled and the target is ahead.
func (e *Engine) advanceHeuristicLocked() {
	if e.cfg.MaxOutOfOrderMillis <= 0 || e.maxSeenTS <= 0 {
		return
	}
	target := e.maxSeenTS - e.cfg.MaxOutOfOrderMillis
	if target > e.wm.Current() {
		e.wm.Advance(target)
	}
}

// closeWindowsLocked fires due windows (wm >= End) once, then purges windows
// whose lateness horizon has passed (wm >= End + AllowedLateness). Results
// are returned in a deterministic (window start, key) order.
func (e *Engine) closeWindowsLocked() []WindowResult {
	var pending []WindowResult
	for start, bucket := range e.windows {
		end := start + e.cfg.WindowSizeMillis
		if e.wm.Current() >= end {
			keys := make([]string, 0, len(bucket))
			for k := range bucket {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				st := bucket[k]
				if !st.fired {
					st.fired = true
					pending = append(pending, e.resultForLocked(st, false))
				}
			}
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].Window.Start != pending[j].Window.Start {
			return pending[i].Window.Start < pending[j].Window.Start
		}
		return pending[i].Key < pending[j].Key
	})

	for start := range e.windows {
		end := start + e.cfg.WindowSizeMillis
		if e.wm.Current() >= end+e.cfg.AllowedLatenessMillis {
			delete(e.windows, start)
		}
	}
	return pending
}

func (e *Engine) resultForLocked(st *windowState, revised bool) WindowResult {
	e.stats.Emitted++
	return WindowResult{
		Window:    Window{Start: st.start, End: st.start + e.cfg.WindowSizeMillis},
		Key:       st.key,
		Value:     st.agg.Value(),
		Count:     st.agg.Count(),
		Revised:   revised,
		Watermark: e.wm.Current(),
	}
}

func (e *Engine) emitUnlocked(rs []WindowResult) {
	// Sink calls happen without the engine lock to avoid user code holding up
	// state; order within one call is preserved.
	for _, r := range rs {
		e.sink.Emit(r)
	}
}

// AdvanceWatermark explicitly punctuates the event-time watermark and emits
// results of windows closed by the move. The watermark advances only when the
// target is strictly greater; equal/backward punctuations are reported as
// BACKWARD_WATERMARK and returned as an error, without moving state.
func (e *Engine) AdvanceWatermark(target int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	cur := e.wm.Current()
	if target <= cur {
		e.stats.BackwardWMMark++
		e.report(DropRecord{
			Reason: DropBackwardWatermark, Timestamp: target, Watermark: cur,
			Detail: "watermark punctuations must be strictly increasing",
		})
		return fmt.Errorf("watermark cannot move from %d to %d", cur, target)
	}
	e.wm.Advance(target)
	pending := e.closeWindowsLocked()
	e.emitUnlocked(pending)
	return nil
}

// Watermark returns the current event-time watermark.
func (e *Engine) Watermark() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wm.Current()
}

func (e *Engine) countLiveStatesLocked() int {
	n := 0
	for _, bucket := range e.windows {
		n += len(bucket)
	}
	return n
}

// Snapshot returns a consistent deep copy of the engine state, taken while
// mutation is quiesced. The returned bytes round-trip through JSON.
func (e *Engine) Snapshot() (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap := Snapshot{
		Watermark: e.wm.Current(),
		MaxSeenTS: e.maxSeenTS,
		Seq:       e.seq,
		Windows:   make([]WindowSnapshot, 0, e.countLiveStatesLocked()),
	}
	starts := make([]int64, 0, len(e.windows))
	for s := range e.windows {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for _, s := range starts {
		bucket := e.windows[s]
		keys := make([]string, 0, len(bucket))
		for k := range bucket {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			st := bucket[k]
			seen := make([]string, 0, len(st.seen))
			for id := range st.seen {
				seen = append(seen, id)
			}
			sort.Strings(seen)
			values := make([]float64, len(st.values))
			copy(values, st.values)
			snap.Windows = append(snap.Windows, WindowSnapshot{
				Start: s, Key: k, Value: st.agg.Value(), Count: st.count,
				Seen: seen, Values: values, Fired: st.fired,
			})
		}
	}
	return snap, nil
}

// RestoreEngine builds an engine whose state is exactly snap: same watermark,
// same heuristic bookkeeping, same live windows with the same dedup sets and
// emission flags. Aggregates are rebuilt deterministically by replaying the
// accepted values in snapshot order, so a replayed input prefix yields the
// exact same subsequent behavior with no double counting.
func RestoreEngine(cfg Config, snap Snapshot, sink Sink) (*Engine, error) {
	eng, err := NewEngine(cfg, sink)
	if err != nil {
		return nil, err
	}
	eng.wm = NewWatermarkTracker(snap.Watermark)
	eng.maxSeenTS = snap.MaxSeenTS
	eng.seq = snap.Seq
	for _, ws := range snap.Windows {
		st := &windowState{
			start: ws.Start,
			key:   ws.Key,
			agg:   eng.cfg.Aggregate(),
			seen:  make(map[string]struct{}, len(ws.Seen)),
			fired: ws.Fired,
			count: ws.Count,
		}
		// Values is the exact acceptance-ordered value stream; replaying it
		// rebuilds the aggregate deterministically. Seen is a set used only
		// for dedup, so its (sorted) serialization order is irrelevant.
		for _, v := range ws.Values {
			st.agg.Add(v)
		}
		st.values = append([]float64(nil), ws.Values...)
		for _, id := range ws.Seen {
			st.seen[id] = struct{}{}
		}
		if eng.windows[ws.Start] == nil {
			eng.windows[ws.Start] = make(map[string]*windowState)
		}
		eng.windows[ws.Start][ws.Key] = st
	}
	return eng, nil
}

// Stats returns a point-in-time copy of the engine counters.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// Close releases engine resources.
func (e *Engine) Close() error { return nil }
