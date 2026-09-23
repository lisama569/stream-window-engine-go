// Command demo runs a fully local, deterministic demonstration of the stream
// window engine: out-of-order events, event-time watermarks, allowed
// lateness with revised emissions, too-late / duplicate / invalid rejection
// observability, crash-recovery replay from a snapshot, and parallel pushes.
package main

import (
	"fmt"

	"github.com/lisama569/stream-window-engine-go/engine"
)

type loggingSink struct{ tag string }

func (s loggingSink) Emit(r engine.WindowResult) {
	kind := "FIRE   "
	if r.Revised {
		kind = "REVISE "
	}
	fmt.Printf("  [%s] %s window=[%d,%d) key=%s value=%.0f count=%d wm=%d\n",
		s.tag, kind, r.Window.Start, r.Window.End, r.Key, r.Value, r.Count, r.Watermark)
}

type loggingReporter struct{}

func (loggingReporter) Report(rec engine.DropRecord) {
	fmt.Printf("  [drop] reason=%-18s event=%s key=%s ts=%d wm=%d %s\n",
		rec.Reason, rec.EventID, rec.Key, rec.Timestamp, rec.Watermark, rec.Detail)
}

func section(title string) {
	fmt.Printf("\n=== %s ===\n", title)
}

func mustEngine(tag string) (*engine.Engine, engine.Config) {
	cfg := engine.Config{
		WindowSizeMillis:      10,
		AllowedLatenessMillis: 5,
		Aggregate:             func() engine.Aggregate { return &engine.SumAggregate{} },
		Reporter:              loggingReporter{},
	}
	e, err := engine.NewEngine(cfg, loggingSink{tag: tag})
	if err != nil {
		panic(err)
	}
	return e, cfg
}

func main() {
	// 1) Out-of-order events: no output until the watermark closes the window.
	section("1. out-of-order windowing (size=10, aligned to epoch)")
	e, _ := mustEngine("wm")
	feed := func(id, key string, ts int64, v float64) {
		w := engine.TumblingWindow(ts, 10)
		fmt.Printf("  event=%s key=%s eventTime=%-3d -> window=[%d,%d)\n", id, key, ts, w.Start, w.End)
		_ = e.ProcessEvent(engine.Event{ID: id, Key: key, Timestamp: ts, Value: v})
	}
	feed("e1", "a", 9, 1)
	feed("e2", "a", 3, 2)
	feed("e3", "a", 7, 4)
	fmt.Printf("  watermark -> 9 (window end is 10): expect no output, wm=%d\n", e.Watermark())
	_ = e.AdvanceWatermark(9)
	fmt.Println("  watermark -> 10: window fires exactly once")
	_ = e.AdvanceWatermark(10)
	_ = e.AdvanceWatermark(11) // repeated trigger does not re-fire

	// 2) Allowed lateness and beyond-lateness.
	section("2. late events: revised within tolerance, rejected past horizon")
	feed("L1", "a", 5, 7)      // wm=10 < 10+5 -> revised
	feed("L2", "a", 1, 3)      // still in tolerance -> revised again
	_ = e.AdvanceWatermark(15) // horizon reached, state purged
	feed("L3", "a", 0, 100)    // TOO_LATE, result frozen

	// 3) Duplicate and invalid inputs are observable and isolated.
	section("3. duplicate and invalid inputs")
	e3, _ := mustEngine("dup")
	feed3 := func(id, key string, ts int64, v float64) {
		w := engine.TumblingWindow(ts, 10)
		fmt.Printf("  event=%s key=%s eventTime=%-3d -> window=[%d,%d)\n", id, key, ts, w.Start, w.End)
		_ = e3.ProcessEvent(engine.Event{ID: id, Key: key, Timestamp: ts, Value: v})
	}
	feed3("D1", "b", 2, 5)
	feed3("D1", "b", 2, 5)                                                        // DUPLICATE
	_ = e3.ProcessEvent(engine.Event{ID: "", Key: "b", Timestamp: 2, Value: 1})   // MISSING_FIELD
	_ = e3.ProcessEvent(engine.Event{ID: "x", Key: "b", Timestamp: -1, Value: 1}) // INVALID_TIMESTAMP
	feed3("D2", "b", 12, 9)                                                       // stream keeps flowing into another window
	_ = e3.AdvanceWatermark(20)

	// 4) Snapshot / restore / replay: suffix only, no double count.
	section("4. crash recovery: snapshot at checkpoint, replay suffix")
	cfg := engine.Config{
		WindowSizeMillis:      10,
		AllowedLatenessMillis: 5,
		Aggregate:             func() engine.Aggregate { return &engine.SumAggregate{} },
		Reporter:              loggingReporter{},
	}
	full := []engine.GeneratedEvent{
		{Event: engine.Event{ID: "1", Key: "a", Timestamp: 1, Value: 10}},
		{Event: engine.Event{ID: "2", Key: "a", Timestamp: 9, Value: 5}},
		{WatermarkMark: 10, HasWatermarkMark: true},
		{Event: engine.Event{ID: "3", Key: "a", Timestamp: 11, Value: 3}},
		{Event: engine.Event{ID: "4", Key: "a", Timestamp: 14, Value: 4}},
		{WatermarkMark: 20, HasWatermarkMark: true},
	}
	chkEng, _ := engine.NewEngine(cfg, loggingSink{tag: "pre-crash"})
	_ = engine.NewSource(full[:3]).Push(chkEng, 1)
	snap, _ := chkEng.Snapshot()
	data, _ := snap.Marshal()
	fmt.Printf("  checkpoint snapshot (wm=%d): %s\n", snap.Watermark, string(data))
	recEng, _ := engine.RestoreEngine(cfg, snap, loggingSink{tag: "recovered"})
	_ = engine.NewSource(full[3:]).Push(recEng, 1)
	fmt.Println("  combined pre-crash + recovered output equals a failure-free run")

	// 5) Parallel push produces identical results to a sequential push.
	section("5. parallel push (key-sharded workers, watermark barriers)")
	stream := []engine.GeneratedEvent{
		{Event: engine.Event{ID: "p1", Key: "x", Timestamp: 9, Value: 1}},
		{Event: engine.Event{ID: "p2", Key: "y", Timestamp: 2, Value: 1}},
		{Event: engine.Event{ID: "p3", Key: "x", Timestamp: 3, Value: 1}},
		{WatermarkMark: 10, HasWatermarkMark: true},
		{Event: engine.Event{ID: "p4", Key: "y", Timestamp: 12, Value: 1}},
		{WatermarkMark: 20, HasWatermarkMark: true},
	}
	parEng, _ := engine.NewEngine(cfg, loggingSink{tag: "parallel"})
	_ = engine.NewSource(stream).Push(parEng, 4)

	st := recEng.Stats()
	fmt.Printf("\nstats after recovery demo: accepted=%d emitted=%d revised=%d\n",
		st.Accepted, st.Emitted, st.Revised)
	fmt.Println("demo complete: all outcomes deterministic and reason-tagged")
}
