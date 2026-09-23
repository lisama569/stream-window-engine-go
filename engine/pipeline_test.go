package engine

import (
	"fmt"
	"sync"
	"testing"
)

// buildStream creates a deterministic multi-key, multi-window, out-of-order
// stream with periodic watermark barriers.
func buildStream(nKeys, windows int) []GeneratedEvent {
	var out []GeneratedEvent
	id := 0
	for w := 0; w < windows; w++ {
		base := int64(w * 10)
		// Insert events in reverse timestamp order to exercise out-of-order.
		for t := int64(9); t >= 0; t-- {
			k := fmt.Sprintf("k%d", id%nKeys)
			id++
			out = append(out, ge(fmt.Sprintf("e%d", id), k, base+t, 1))
		}
		out = append(out, mark(base+10))
	}
	return out
}

func aggregateFinal(sink *MemorySink) map[string]float64 {
	// Last emission per (window,key) is the authoritative final value.
	final := map[string]float64{}
	for _, r := range sink.Results() {
		final[keyOf(r)] = r.Value
	}
	return final
}

// TestParallelPushMatchesSequential: any parallelism level must produce the
// same final aggregates and the same number of emissions as parallelism=1.
func TestParallelPushMatchesSequential(t *testing.T) {
	cfg := testConfig() // MaxOutOfOrder=0: windows fire only at wm barriers
	stream := buildStream(4, 4)

	seqSink := &MemorySink{}
	seqEng, _ := NewEngine(cfg, seqSink)
	if err := NewSource(stream).Push(seqEng, 1); err != nil {
		t.Fatal(err)
	}
	seqFinal := aggregateFinal(seqSink)

	for _, p := range []int{2, 3, 4, 8} {
		sink := &MemorySink{}
		eng, _ := NewEngine(cfg, sink)
		if err := NewSource(stream).Push(eng, p); err != nil {
			t.Fatal(err)
		}
		gotFinal := aggregateFinal(sink)
		if len(gotFinal) != len(seqFinal) {
			t.Fatalf("p=%d final state size %d want %d", p, len(gotFinal), len(seqFinal))
		}
		for k, v := range seqFinal {
			if gotFinal[k] != v {
				t.Fatalf("p=%d key=%s got=%v want=%v", p, k, gotFinal[k], v)
			}
		}
		// Every window fires exactly once (no revisions in this stream): the
		// barrier must prevent a pre-mark event from racing the close.
		var revised int
		for _, r := range sink.Results() {
			if r.Revised {
				revised++
			}
		}
		if revised != 0 {
			t.Fatalf("p=%d unexpected revisions=%d (barrier race?)", p, revised)
		}
		// Emission ordering is deterministic as well: initial firings only
		// happen at barriers on the coordinator in (start,key) order.
		if len(sink.Results()) != len(seqSink.Results()) {
			t.Fatalf("p=%d emission count %d want %d", p, len(sink.Results()), len(seqSink.Results()))
		}
		compareResults(t, seqSink.Results(), sink.Results(), fmt.Sprintf("p=%d ordered emissions", p))
		t.Logf("parallelism=%d matches sequential: %d emissions, %d final aggregates",
			p, len(sink.Results()), len(gotFinal))
	}
}

// TestParallelPushDeterminismAcrossRuns: run the parallel push many times;
// results must be identical on every run regardless of goroutine scheduling.
func TestParallelPushDeterminismAcrossRuns(t *testing.T) {
	cfg := testConfig()
	stream := buildStream(5, 5)
	var reference []WindowResult
	for run := 0; run < 20; run++ {
		sink := &MemorySink{}
		eng, _ := NewEngine(cfg, sink)
		if err := NewSource(stream).Push(eng, 6); err != nil {
			t.Fatal(err)
		}
		if reference == nil {
			reference = sink.Results()
			continue
		}
		compareResults(t, reference, sink.Results(), fmt.Sprintf("run=%d", run))
	}
}

// TestConcurrentSnapshotsAndProcess: snapshots taken during concurrent
// processing must be self-consistent (counts match seen-set sizes, values
// match replayed values) and must never corrupt live state.
func TestConcurrentSnapshotsAndProcess(t *testing.T) {
	cfg := testConfig()
	cfg.MaxOutOfOrderMillis = 0
	rep := NewMemoryDropReporter(4096)
	cfg.Reporter = rep
	sink := &MemorySink{}
	eng, _ := NewEngine(cfg, sink)

	var writers sync.WaitGroup
	stop := make(chan struct{})

	// 6 writer goroutines hammer events for distinct key ranges.
	for w := 0; w < 6; w++ {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			for i := 0; i < 301; i++ {
				key := fmt.Sprintf("w%d-k%d", worker, i%7)
				ts := int64((i % 6) * 10) // windows 0..50
				_ = eng.ProcessEvent(ev(fmt.Sprintf("w%d-e%d", worker, i), key, ts+1, 1))
			}
		}(w)
	}

	// Watermark advancer on another goroutine (strictly increasing marks).
	writers.Add(1)
	go func() {
		defer writers.Done()
		for wm := int64(10); wm <= 60; wm += 10 {
			_ = eng.AdvanceWatermark(wm)
		}
	}()

	// Snapshotter goroutine validates every snapshot's internal consistency.
	var snapWG sync.WaitGroup
	snapWG.Add(1)
	go func() {
		defer snapWG.Done()
		snapshots := 0
		for snapshots < 2000 {
			select {
			case <-stop:
				return
			default:
			}
			snap, err := eng.Snapshot()
			if err != nil {
				return
			}
			for _, ws := range snap.Windows {
				if int(ws.Count) != len(ws.Seen) || len(ws.Seen) != len(ws.Values) {
					t.Errorf("inconsistent snapshot window %+v", ws)
					return
				}
			}
			snapshots++
		}
	}()

	// Once all writers and the watermarker finish, release the snapshotter.
	writers.Wait()
	close(stop)
	snapWG.Wait()

	// After all writers finish, advance wm fully and verify accounting.
	_ = eng.AdvanceWatermark(1000)
	stats := eng.Stats()
	t.Logf("concurrent run: accepted=%d invalid=%d duplicate=%d toolate=%d mem=%d emitted=%d",
		stats.Accepted, stats.Invalid, stats.Duplicate, stats.TooLate, stats.MemoryLimited, stats.Emitted)
	// Every attempted event is booked exactly once across the categories.
	if got := stats.Accepted + stats.Duplicate + stats.TooLate +
		stats.MemoryLimited + stats.Invalid; got != 6*301 {
		t.Fatalf("accounting mismatch: classified=%d attempted=%d stats=%+v",
			got, 6*301, stats)
	}
}

// TestPushToleratesBackwardMarkAndBadEvents: a stream containing invalid
// events and a backward watermark punctuation must still drain completely;
// all anomalies are observable and the valid data is aggregated correctly.
func TestPushToleratesBackwardMarkAndBadEvents(t *testing.T) {
	for _, p := range []int{1, 4} {
		cfg := testConfig()
		rep := NewMemoryDropReporter(64)
		cfg.Reporter = rep
		sink := &MemorySink{}
		eng, _ := NewEngine(cfg, sink)

		stream := []GeneratedEvent{
			ge("1", "k", 1, 3),
			{Event: Event{ID: "", Key: "k", Timestamp: 1, Value: 1}}, // invalid
			mark(10), // fires [0,10)
			mark(7),  // backward: reported, ignored
			ge("2", "k", 11, 4),
			mark(20), // fires [10,20)
		}
		if err := NewSource(stream).Push(eng, p); err != nil {
			t.Fatalf("p=%d push returned error: %v", p, err)
		}
		final := aggregateFinal(sink)
		if final["0/k"] != 3 {
			t.Fatalf("p=%d window0 = %v want 3", p, final["0/k"])
		}
		if final["10/k"] != 4 {
			t.Fatalf("p=%d window1 = %v want 4", p, final["10/k"])
		}
		cnt := rep.CountByReason()
		if cnt[DropBackwardWatermark] != 1 || cnt[DropMissingField] != 1 {
			t.Fatalf("p=%d anomaly counts wrong: %v", p, cnt)
		}
		t.Logf("p=%d survived invalid event + backward mark; wm=%d", p, eng.Watermark())
	}
}
