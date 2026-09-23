package engine

import (
	"bytes"
	"testing"
)

// assertSnapshotsEqual compares the observable state of two engines via
// JSON-serialized snapshots.
func assertSnapshotsEqual(t *testing.T, a, b *Engine, label string) {
	t.Helper()
	sa, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sb, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ja, _ := sa.Marshal()
	jb, _ := sb.Marshal()
	if !bytes.Equal(ja, jb) {
		t.Fatalf("%s: state diverged after restore\nA=%s\nB=%s", label, ja, jb)
	}
}

// TestSnapshotRoundTripAndStateEquality: an engine restored from a
// mid-stream snapshot (serialized through JSON) is identical to the original
// at the snapshot point and behaves identically on the remaining prefix.
func TestSnapshotRoundTripAndStateEquality(t *testing.T) {
	cfg := testConfig()

	build := func() (*Engine, *MemorySink) {
		s := &MemorySink{}
		e, _ := NewEngine(cfg, s)
		return e, s
	}

	orig, origSink := build()
	prefix := []Event{
		ev("1", "a", 1, 10),
		ev("2", "b", 2, 1),
		ev("3", "a", 11, 5), // next window
		ev("4", "a", 9, 2),  // out of order, still [0,10)
	}
	for _, x := range prefix {
		if err := orig.ProcessEvent(x); err != nil {
			t.Fatal(err)
		}
	}
	if err := orig.AdvanceWatermark(10); err != nil { // fires [0,10) a & b
		t.Fatal(err)
	}
	snap, err := orig.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	data, err := snap.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot at wm=%d: %s", orig.Watermark(), string(data))

	restoredSnap, err := UnmarshalSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	restoredSink := &MemorySink{}
	restored, err := RestoreEngine(cfg, restoredSnap, restoredSink)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotsEqual(t, orig, restored, "immediately after restore")

	// Continue both engines with the SAME remaining prefix, sequentially.
	rest := []Event{
		ev("5", "a", 15, 7),
		ev("6", "b", 12, 3),
	}
	for _, x := range rest {
		if err := orig.ProcessEvent(x); err != nil {
			t.Fatal(err)
		}
		if err := restored.ProcessEvent(x); err != nil {
			t.Fatalf("restored rejected %s: %v", x.ID, err)
		}
	}
	if err := orig.AdvanceWatermark(20); err != nil {
		t.Fatal(err)
	}
	if err := restored.AdvanceWatermark(20); err != nil {
		t.Fatal(err)
	}
	assertSnapshotsEqual(t, orig, restored, "after replaying remaining prefix")

	// Sinks: the restored engine's fresh sink contains only post-restore
	// emissions; the original's contains all. Final-window results (which are
	// post-snapshot for everyone) must match exactly.
	lastOrig := origSink.Results()
	lastRest := restoredSink.Results()
	if len(lastRest) == 0 {
		t.Fatal("restored engine emitted nothing after wm=20")
	}
	// Compare the tail emitted after the snapshot point.
	tailOrig := lastOrig[len(lastOrig)-len(lastRest):]
	compareResults(t, tailOrig, lastRest, "post-snapshot emissions")
}

// TestSnapshotReplayNoDoubleCount: simulate a crash after the snapshot.
// Recovery replays the input stream FROM the snapshot point into a restored
// engine with a fresh (rolled-back) sink. The replayed suffix events that the
// snapshot already knew about never get double counted, and the final result
// equals a failure-free run exactly.
func TestSnapshotReplayNoDoubleCount(t *testing.T) {
	cfg := testConfig()

	// Failure-free reference run over the whole stream.
	stream := []GeneratedEvent{
		ge("1", "a", 1, 10),
		ge("2", "a", 9, 5),
		ge("3", "b", 2, 2),
		mark(10), // checkpoint boundary: [0,10) fires
		ge("4", "a", 11, 3),
		ge("5", "a", 14, 4), // late? no, [10,20)
		ge("6", "b", 12, 8),
		mark(20), // [10,20) fires
	}
	refSink := &MemorySink{}
	ref, _ := NewEngine(cfg, refSink)
	if err := NewSource(stream).Push(ref, 1); err != nil {
		t.Fatal(err)
	}
	refResults := refSink.Results()

	// Run up to and including the checkpoint mark, snapshot, then "crash".
	chkSink := &MemorySink{}
	chk, _ := NewEngine(cfg, chkSink)
	if err := NewSource(stream[:4]).Push(chk, 1); err != nil {
		t.Fatal(err)
	}
	snap, _ := chk.Snapshot()
	if snap.Watermark != 10 {
		t.Fatalf("checkpoint wm=%d want 10", snap.Watermark)
	}
	// Fired states must survive the snapshot.
	firedKeys := map[string]bool{}
	for _, w := range snap.Windows {
		firedKeys[w.Key] = w.Fired
	}
	if !firedKeys["a"] || !firedKeys["b"] {
		t.Fatalf("fired flags missing in snapshot: %+v", firedKeys)
	}
	t.Logf("checkpoint taken wm=%d windows=%d; simulating crash and rollback", snap.Watermark, len(snap.Windows))

	// Recover: roll the sink back to the checkpoint (fresh sink) and replay
	// ONLY the suffix (everything strictly after the checkpoint).
	recSink := &MemorySink{}
	rec, err := RestoreEngine(cfg, snap, recSink)
	if err != nil {
		t.Fatal(err)
	}
	suffix := stream[4:]
	if err := NewSource(suffix).Push(rec, 1); err != nil {
		t.Fatal(err)
	}

	// Combined output: pre-checkpoint emissions from chkSink + post-recovery
	// emissions from recSink must equal the reference run exactly, in order.
	var combined []WindowResult
	combined = append(combined, chkSink.Results()...)
	combined = append(combined, recSink.Results()...)
	compareResults(t, refResults, combined, "crash-recovery replay")

	// Aggregates of window [10,20): a = 3+4 = 7 count 2; b = 8 count 1.
	finalBy := map[string]WindowResult{}
	for _, r := range combined {
		finalBy[keyOf(r)] = r
	}
	if finalBy["10/a"].Value != 7 || finalBy["10/a"].Count != 2 {
		t.Fatalf("window [10,20)/a after replay: %+v", finalBy["10/a"])
	}
	if finalBy["10/b"].Value != 8 || finalBy["10/b"].Count != 1 {
		t.Fatalf("window [10,20)/b after replay: %+v", finalBy["10/b"])
	}
	for _, r := range combined {
		t.Logf("replayed emit window=[%d,%d) key=%s value=%v count=%d revised=%v wm=%d",
			r.Window.Start, r.Window.End, r.Key, r.Value, r.Count, r.Revised, r.Watermark)
	}
}

// TestSnapshotReplayDuplicateSuffix: if the replay accidentally starts before
// the checkpoint and re-feeds events the snapshot already contains, they are
// deterministically rejected as DUPLICATE (observable), never counted twice.
func TestSnapshotReplayDuplicateSuffix(t *testing.T) {
	cfg := testConfig()
	cfg.Reporter = NewMemoryDropReporter(64)
	rep := cfg.Reporter.(*MemoryDropReporter)

	sink := &MemorySink{}
	e, _ := NewEngine(cfg, sink)
	_ = e.ProcessEvent(ev("1", "a", 1, 10))
	_ = e.AdvanceWatermark(10) // fires, but lateness horizon (15) keeps state
	snap, _ := e.Snapshot()

	rec, err := RestoreEngine(cfg, snap, &MemorySink{})
	if err != nil {
		t.Fatal(err)
	}
	// Re-feed an event already included in the snapshot (within lateness).
	err = rec.ProcessEvent(ev("1", "a", 1, 10))
	if err == nil {
		t.Fatal("replayed already-consumed event must be rejected")
	}
	if rep.CountByReason()[DropDuplicate] != 1 {
		t.Fatalf("expected one DUPLICATE on replay, got %v", rep.CountByReason())
	}
	// The aggregate stays at 10.
	rs, _ := rec.Snapshot()
	for _, w := range rs.Windows {
		if w.Key == "a" && (w.Value != 10 || w.Count != 1) {
			t.Fatalf("double count on replay: %+v", w)
		}
	}
	t.Logf("replayed consumed event id=1 ts=1 wm=10 window=[0,10): DUPLICATE, no double count")
}

func compareResults(t *testing.T, want, got []WindowResult, label string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: emission count want=%d got=%d\nwant=%+v\ngot=%+v", label, len(want), len(got), want, got)
	}
	for i := range want {
		a, b := want[i], got[i]
		if a != b {
			t.Fatalf("%s: emission #%d differs\nwant=%+v\ngot=%+v", label, i, a, b)
		}
	}
}
