package engine

import (
	"errors"
	"math"
	"testing"
)

// TestInvalidInputsIsolated: malformed events must not interrupt processing
// and must not pollute other windows. Every rejection is observable with a
// distinguishable reason.
func TestInvalidInputsIsolated(t *testing.T) {
	sink := &MemorySink{}
	rep := NewMemoryDropReporter(64)
	cfg := testConfig()
	cfg.Reporter = rep
	e, _ := NewEngine(cfg, sink)
	defer e.Close()

	good1 := ev("g1", "a", 1, 100)
	_ = e.ProcessEvent(good1)

	inputs := []Event{
		{ID: "", Key: "a", Timestamp: 1, Value: 1},            // missing id
		{ID: "x", Key: "", Timestamp: 1, Value: 1},            // missing key
		{ID: "x", Key: "a", Timestamp: -3, Value: 1},          // bad ts
		{ID: "x", Key: "a", Timestamp: 1, Value: math.NaN()},  // bad value
		{ID: "x", Key: "a", Timestamp: 1, Value: math.Inf(1)}, // bad value
	}
	for i, in := range inputs {
		err := e.ProcessEvent(in)
		if err == nil {
			t.Fatalf("case %d should be rejected", i)
		}
		var verr *ValidationError
		if !errors.As(err, &verr) {
			t.Fatalf("case %d err is not ValidationError: %v", i, err)
		}
		t.Logf("invalid input #%d id=%q key=%q ts=%d reason=%s rejected; pipeline continues",
			i, in.ID, in.Key, in.Timestamp, verr.Reason)
	}

	good2 := ev("g2", "b", 12, 5)
	if err := e.ProcessEvent(good2); err != nil {
		t.Fatalf("pipeline broken after invalid inputs: %v", err)
	}
	logDecision(t, "after-invalid", good2, e.Watermark(), TumblingWindow(good2.Timestamp, 10),
		"processing continues, lands in window [10,20)")

	_ = e.AdvanceWatermark(20)
	got := sink.Results()
	// Window [0,10) key a must be exactly 100; [10,20) key b exactly 5.
	by := map[string]WindowResult{}
	for _, r := range got {
		by[keyOf(r)] = r
	}
	if by["0/a"].Value != 100 || by["0/a"].Count != 1 {
		t.Fatalf("window a polluted: %+v", by["0/a"])
	}
	if by["10/b"].Value != 5 || by["10/b"].Count != 1 {
		t.Fatalf("window b wrong: %+v", by["10/b"])
	}
	cnt := rep.CountByReason()
	if cnt[DropMissingField] != 2 || cnt[DropInvalidTimestamp] != 1 || cnt[DropInvalidValue] != 2 {
		t.Fatalf("rejection counts wrong: %v", cnt)
	}
	t.Logf("eventTime/window basis logged; final distinct rejection reasons: %v", cnt)
}

func keyOf(r WindowResult) string {
	return itoa(r.Window.Start) + "/" + r.Key
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// TestMemoryLimitLiveWindows: the hard live-state bound rejects the overflow
// event as MEMORY_LIMIT (observable), leaving existing state intact and
// processing still alive.
func TestMemoryLimitLiveWindows(t *testing.T) {
	sink := &MemorySink{}
	rep := NewMemoryDropReporter(64)
	cfg := testConfig()
	cfg.MaxLiveWindows = 2
	cfg.Reporter = rep
	e, _ := NewEngine(cfg, sink)
	defer e.Close()

	// Create two distinct (window,key) live states.
	_ = e.ProcessEvent(ev("1", "a", 1, 1))
	_ = e.ProcessEvent(ev("2", "b", 1, 1))
	// Third live state must be rejected.
	err := e.ProcessEvent(ev("3", "c", 1, 1))
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("want ErrMemoryLimit, got %v", err)
	}
	t.Logf("memory cap event ts=1 wm=%d window=[0,10): MEMORY_LIMIT rejected (cap=2 live states)",
		e.Watermark())
	// Same key as an existing state is still accepted (no new state).
	if err := e.ProcessEvent(ev("4", "a", 2, 2)); err != nil {
		t.Fatalf("same-key event rejected: %v", err)
	}
	// Closing windows does not help the cap until purge (horizon). Move wm to
	// 15 (>=10+5): states purge, a new window state is then admissible.
	if err := e.AdvanceWatermark(15); err != nil {
		t.Fatal(err)
	}
	if err := e.ProcessEvent(ev("5", "c", 16, 9)); err != nil {
		t.Fatalf("after purge event should be accepted: %v", err)
	}
	_ = e.AdvanceWatermark(25)
	if rep.CountByReason()[DropMemoryLimit] != 1 {
		t.Fatalf("memory limit not reported once: %v", rep.CountByReason())
	}
	// Original window a = 1+2.
	for _, r := range sink.Results() {
		if r.Key == "a" && r.Window.Start == 0 && r.Value != 3 {
			t.Fatalf("existing state changed by memory rejection: %+v", r)
		}
	}
}

// TestMemoryLimitPerWindow: the per-window accepted-event footprint is
// bounded too.
func TestMemoryLimitPerWindow(t *testing.T) {
	sink := &MemorySink{}
	rep := NewMemoryDropReporter(64)
	cfg := testConfig()
	cfg.MaxEventsPerWindow = 2
	cfg.Reporter = rep
	e, _ := NewEngine(cfg, sink)
	defer e.Close()

	if err := e.ProcessEvent(ev("1", "k", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := e.ProcessEvent(ev("2", "k", 2, 2)); err != nil {
		t.Fatal(err)
	}
	err := e.ProcessEvent(ev("3", "k", 3, 4))
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("want ErrMemoryLimit, got %v", err)
	}
	_ = e.AdvanceWatermark(10)
	got := sink.Results()
	if len(got) != 1 || got[0].Value != 3 || got[0].Count != 2 {
		t.Fatalf("per-window cap leaked into aggregate: %+v", got)
	}
	if rep.CountByReason()[DropMemoryLimit] != 1 {
		t.Fatalf("memory limit not counted: %v", rep.CountByReason())
	}
}

// TestHeuristicWatermark: with MaxOutOfOrderMillis the observed event stream
// itself advances the watermark to maxSeen-delay and closes windows; it still
// never moves backwards.
func TestHeuristicWatermark(t *testing.T) {
	sink := &MemorySink{}
	cfg := testConfig()
	cfg.MaxOutOfOrderMillis = 2
	e, _ := NewEngine(cfg, sink)
	defer e.Close()

	_ = e.ProcessEvent(ev("1", "k", 5, 10)) // heuristic wm -> 3, window open
	if e.Watermark() != 3 {
		t.Fatalf("heuristic wm=%d want 3", e.Watermark())
	}
	if len(sink.Results()) != 0 {
		t.Fatal("window fired early under heuristic")
	}
	_ = e.ProcessEvent(ev("2", "k", 12, 1)) // heuristic wm -> 10, fires [0,10)
	logWatermark(t, "heuristic", 10, e.Watermark(), true, "maxSeen=12 delay=2 -> wm=10 fires window")
	got := sink.Results()
	if len(got) != 1 || got[0].Value != 10 {
		t.Fatalf("heuristic close wrong: %+v", got)
	}
	// Older event may not roll the watermark back.
	_ = e.ProcessEvent(ev("3", "k", 6, 1)) // too late anyway (horizon 15? wm10, window [0,10), horizon 15 -> late accepted revision)
	if e.Watermark() != 10 {
		t.Fatalf("watermark moved backwards to %d", e.Watermark())
	}
}

// TestHeuristicDoesNotSelfExpire: an event must be classified against the
// watermark *before* its own timestamp is allowed to advance the heuristic
// watermark, otherwise a first event could expire its own window.
func TestHeuristicDoesNotSelfExpire(t *testing.T) {
	sink := &MemorySink{}
	cfg := testConfig()
	cfg.MaxOutOfOrderMillis = 0 // purely explicit watermark; check baseline ordering
	cfg.WindowSizeMillis = 10
	cfg.AllowedLatenessMillis = 0
	e, _ := NewEngine(cfg, sink)
	defer e.Close()

	// wm at window end already, event exactly at boundary end is in the next
	// window; an event at ts=9 after wm=10 is too late (no lateness).
	_ = e.ProcessEvent(ev("early", "k", 1, 1))
	if err := e.AdvanceWatermark(10); err != nil {
		t.Fatal(err)
	}
	err := e.ProcessEvent(ev("late", "k", 9, 1))
	if !errors.Is(err, ErrTooLate) {
		t.Fatalf("strictly late event should be TOO_LATE, got %v", err)
	}

	// With allowed lateness > 0 the same event must be accepted as a revision.
	sink2 := &MemorySink{}
	cfg2 := testConfig()
	cfg2.AllowedLatenessMillis = 5
	e2, _ := NewEngine(cfg2, sink2)
	defer e2.Close()
	_ = e2.ProcessEvent(ev("early", "k", 1, 1))
	_ = e2.AdvanceWatermark(10)
	if err := e2.ProcessEvent(ev("late", "k", 9, 1)); err != nil {
		t.Fatalf("event within allowed lateness rejected: %v", err)
	}
	last := sink2.Results()[len(sink2.Results())-1]
	if !last.Revised || last.Value != 2 {
		t.Fatalf("expected revision value=2, got %+v", last)
	}
	t.Logf("event late ts=9 wm=10 window=[0,10) horizon=15: accepted as revision (lateness ordering verified)")
}
