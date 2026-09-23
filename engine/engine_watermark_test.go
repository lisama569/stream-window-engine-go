package engine

import (
	"errors"
	"testing"
)

func testConfig() Config {
	return Config{
		WindowSizeMillis:      10,
		AllowedLatenessMillis: 5,
		Aggregate:             func() Aggregate { return &SumAggregate{} },
	}
}

// TestWindowFiresOnlyAtWatermark: an unclosed window must not emit early;
// the first result appears exactly when wm reaches window end.
func TestWindowFiresOnlyAtWatermark(t *testing.T) {
	sink := &MemorySink{}
	rep := NewMemoryDropReporter(64)
	cfg := testConfig()
	cfg.Reporter = rep
	e, err := NewEngine(cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e1 := ev("e1", "k", 1, 3)
	if err := e.ProcessEvent(e1); err != nil {
		t.Fatal(err)
	}
	logDecision(t, "accept", e1, e.Watermark(), TumblingWindow(e1.Timestamp, 10),
		"window [0,10) open; wm<10 so NOT emitted")
	if len(sink.Results()) != 0 {
		t.Fatalf("unclosed window emitted early: %v", sink.Results())
	}

	e2 := ev("e2", "k", 9, 7)
	_ = e.ProcessEvent(e2)
	logDecision(t, "accept", e2, e.Watermark(), TumblingWindow(e2.Timestamp, 10),
		"still wm<10, no output")
	if len(sink.Results()) != 0 {
		t.Fatalf("window emitted before close: %v", sink.Results())
	}

	if err := e.AdvanceWatermark(9); err != nil {
		t.Fatal(err)
	}
	logWatermark(t, "mark", 9, e.Watermark(), false, "wm=9 < end=10, still no output")
	if len(sink.Results()) != 0 {
		t.Fatalf("window emitted at wm=9 (end is 10): %v", sink.Results())
	}

	if err := e.AdvanceWatermark(10); err != nil {
		t.Fatal(err)
	}
	logWatermark(t, "mark", 10, e.Watermark(), true, "wm>=10 fires window [0,10) once")
	got := sink.Results()
	if len(got) != 1 || got[0].Value != 10 || got[0].Count != 2 || got[0].Revised {
		t.Fatalf("unexpected emission: %+v", got)
	}
	if got[0].Watermark != 10 {
		t.Fatalf("emission watermark = %d", got[0].Watermark)
	}

	// Repeated watermark moves must not re-fire the same window.
	if err := e.AdvanceWatermark(11); err != nil {
		t.Fatal(err)
	}
	if len(sink.Results()) != 1 {
		t.Fatalf("window re-fired: %v", sink.Results())
	}
	// Equal/backward punctuation is rejected and observable.
	if err := e.AdvanceWatermark(10); err == nil {
		t.Fatal("backward watermark accepted")
	} else {
		t.Logf("backward watermark correctly rejected: %v", err)
	}
	if rep.CountByReason()[DropBackwardWatermark] != 1 {
		t.Fatalf("backward wm not reported: %v", rep.CountByReason())
	}
	if len(sink.Results()) != 1 {
		t.Fatalf("window output changed after rejected wm: %v", sink.Results())
	}
}

// TestOutOfOrderEvents: events arriving in reverse timestamp order within one
// window still produce the correct aggregate at fire time.
func TestOutOfOrderEvents(t *testing.T) {
	sink := &MemorySink{}
	e, _ := NewEngine(testConfig(), sink)
	defer e.Close()

	events := []Event{
		ev("a", "k", 9, 1),
		ev("b", "k", 3, 2),
		ev("c", "k", 7, 4),
		ev("d", "k", 0, 8),
	}
	for _, x := range events {
		if err := e.ProcessEvent(x); err != nil {
			t.Fatal(err)
		}
		logDecision(t, "out-of-order", x, e.Watermark(), TumblingWindow(x.Timestamp, 10),
			"accepted into open window; no output before close")
	}
	if err := e.AdvanceWatermark(10); err != nil {
		t.Fatal(err)
	}
	got := sink.Results()
	if len(got) != 1 {
		t.Fatalf("emissions=%d", len(got))
	}
	if got[0].Value != 15 || got[0].Count != 4 {
		t.Fatalf("out-of-order aggregate = %v count=%d, want 15/4", got[0].Value, got[0].Count)
	}
}

// TestLateEventWithinLateness: after firing, events up to end+allowedLateness
// update the window and emit a flagged revision; beyond that they are
// TOO_LATE and observable, and the finalized result never changes again.
func TestLateEventWithinLateness(t *testing.T) {
	sink := &MemorySink{}
	rep := NewMemoryDropReporter(64)
	cfg := testConfig()
	cfg.Reporter = rep
	e, _ := NewEngine(cfg, sink)
	defer e.Close()

	first := ev("a", "k", 2, 10)
	_ = e.ProcessEvent(first)
	logDecision(t, "late-scenario", first, e.Watermark(), Window{0, 10}, "window open")
	if err := e.AdvanceWatermark(10); err != nil {
		t.Fatal(err)
	}
	late1 := ev("b", "k", 5, 7) // within allowed lateness (horizon 15)
	if err := e.ProcessEvent(late1); err != nil {
		t.Fatalf("late event within tolerance rejected: %v", err)
	}
	logDecision(t, "late-scenario", late1, e.Watermark(), Window{0, 10},
		"wm=10 < horizon=15: accepted, revised emission value=17")
	got := sink.Results()
	if len(got) != 2 || !got[1].Revised || got[1].Value != 17 || got[1].Count != 2 {
		t.Fatalf("revision emission wrong: %+v", got)
	}

	late2 := ev("c", "k", 1, 3)
	if err := e.ProcessEvent(late2); err != nil {
		t.Fatalf("second in-lateness late event rejected: %v", err)
	}
	got = sink.Results()
	if len(got) != 3 || !got[2].Revised || got[2].Value != 20 {
		t.Fatalf("second revision wrong: %+v", got)
	}

	// Watermark reaches the horizon: state is purged. Further late events are
	// rejected as TOO_LATE (not silently dropped) and cannot change results.
	if err := e.AdvanceWatermark(15); err != nil {
		t.Fatal(err)
	}
	late3 := ev("d", "k", 0, 100)
	err := e.ProcessEvent(late3)
	if !errors.Is(err, ErrTooLate) {
		t.Fatalf("want ErrTooLate, got %v", err)
	}
	logDecision(t, "late-scenario", late3, e.Watermark(), Window{0, 10},
		"wm=15 >= horizon=15: TOO_LATE, state purged, result frozen at 20")
	if rep.CountByReason()[DropTooLate] != 1 {
		t.Fatalf("TOO_LATE not reported: %v", rep.CountByReason())
	}
	got = sink.Results()
	if len(got) != 3 || got[len(got)-1].Value != 20 {
		t.Fatalf("finalized window changed after purge: %+v", got)
	}
}

// TestDuplicateEvent: a repeated ID for a live window is DUPLICATE, counted
// once, and observable; it must not change the aggregate.
func TestDuplicateEvent(t *testing.T) {
	sink := &MemorySink{}
	rep := NewMemoryDropReporter(64)
	cfg := testConfig()
	cfg.Reporter = rep
	e, _ := NewEngine(cfg, sink)
	defer e.Close()

	x := ev("dup", "k", 2, 5)
	if err := e.ProcessEvent(x); err != nil {
		t.Fatal(err)
	}
	err := e.ProcessEvent(x)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	// Different key same ID is a different window state -> accepted (IDs are
	// scoped per window/key state).
	if err := e.ProcessEvent(ev("dup", "other", 2, 9)); err != nil {
		t.Fatalf("same id on another key rejected: %v", err)
	}
	_ = e.AdvanceWatermark(10)
	got := sink.Results()
	if len(got) != 2 {
		t.Fatalf("emissions=%d want 2 (two keys)", len(got))
	}
	byKey := map[string]WindowResult{}
	for _, r := range got {
		byKey[r.Key] = r
	}
	if byKey["k"].Value != 5 || byKey["k"].Count != 1 {
		t.Fatalf("duplicate polluted aggregate: %+v", byKey["k"])
	}
	if byKey["other"].Value != 9 {
		t.Fatalf("other-key aggregate wrong: %+v", byKey["other"])
	}
	if rep.CountByReason()[DropDuplicate] != 1 {
		t.Fatalf("duplicate not reported: %v", rep.CountByReason())
	}
	t.Logf("duplicate event ts=%d wm=%d window=[0,10): reported DUPLICATE, aggregate unchanged",
		x.Timestamp, e.Watermark())
}
