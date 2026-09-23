package engine

import "testing"

func TestTumblingWindowAlignment(t *testing.T) {
	cases := []struct {
		ts, size, start, end int64
	}{
		{0, 10, 0, 10},
		{1, 10, 0, 10},
		{9, 10, 0, 10},
		{10, 10, 10, 20},
		{25, 10, 20, 30},
		{100, 50, 100, 150},
	}
	for _, c := range cases {
		w := TumblingWindow(c.ts, c.size)
		if w.Start != c.start || w.End != c.end {
			t.Fatalf("ts=%d size=%d got [%d,%d) want [%d,%d)", c.ts, c.size, w.Start, w.End, c.start, c.end)
		}
		if !w.Contains(c.ts) {
			t.Fatalf("window %v should contain %d", w, c.ts)
		}
		if c.ts == c.start && c.start > 0 && !w.Contains(c.start) {
			t.Fatalf("window must contain its start")
		}
		// Half-open: end belongs to the next window.
		next := TumblingWindow(c.end, c.size)
		if next.Contains(c.end) && next.Start != c.end {
			t.Fatalf("end %d must not belong to %v", c.end, w)
		}
	}
}

func TestAggregates(t *testing.T) {
	s := &SumAggregate{}
	s.Add(1.5)
	s.Add(2.5)
	if s.Value() != 4.0 || s.Count() != 2 {
		t.Fatalf("sum agg = %v/%d", s.Value(), s.Count())
	}
	s2 := &SumAggregate{}
	s2.Add(10)
	s.Merge(s2)
	if s.Value() != 14.0 || s.Count() != 3 {
		t.Fatalf("merge = %v/%d", s.Value(), s.Count())
	}
	c := &CountAggregate{}
	c.Add(0)
	c.Add(0)
	if c.Value() != 2 || c.Count() != 2 {
		t.Fatalf("count agg = %v/%d", c.Value(), c.Count())
	}
}

func TestWatermarkMonotonic(t *testing.T) {
	tr := NewWatermarkTracker(-1)
	if tr.Current() != -1 {
		t.Fatalf("initial wm = %d", tr.Current())
	}
	wm, adv := tr.Advance(5)
	if !adv || wm != 5 {
		t.Fatalf("advance to 5: wm=%d adv=%v", wm, adv)
	}
	if _, adv := tr.Advance(5); adv {
		t.Fatal("equal target must not advance")
	}
	if _, adv := tr.Advance(3); adv {
		t.Fatal("smaller target must not advance")
	}
	if tr.Current() != 5 {
		t.Fatalf("wm moved backwards to %d", tr.Current())
	}
	logWatermark(t, "monotonic", 3, tr.Current(), false, "equal/backward rejected, wm stays 5")
}

func TestDropReporterBounded(t *testing.T) {
	r := NewMemoryDropReporter(2)
	r.Report(DropRecord{Reason: DropTooLate, EventID: "a"})
	r.Report(DropRecord{Reason: DropDuplicate, EventID: "b"})
	r.Report(DropRecord{Reason: DropTooLate, EventID: "c"})
	if len(r.Records()) != 2 {
		t.Fatalf("stored = %d, want 2", len(r.Records()))
	}
	if r.OverflowCount() != 1 {
		t.Fatalf("overflow = %d, want 1", r.OverflowCount())
	}
	cnt := r.CountByReason()
	if cnt[DropTooLate] != 2 || cnt[DropDuplicate] != 1 {
		t.Fatalf("counts = %v", cnt)
	}
}

func TestEventValidation(t *testing.T) {
	bad := []Event{
		{ID: "", Key: "k", Timestamp: 1, Value: 1},
		{ID: "1", Key: "", Timestamp: 1, Value: 1},
		{ID: "1", Key: "k", Timestamp: -5, Value: 1},
		{ID: "1", Key: "k", Timestamp: 1, Value: nanValue()},
	}
	want := []DropReason{DropMissingField, DropMissingField, DropInvalidTimestamp, DropInvalidValue}
	for i, e := range bad {
		verr := e.Validate()
		if verr == nil || verr.Reason != want[i] {
			t.Fatalf("case %d got %v want %s", i, verr, want[i])
		}
	}
	if ev("1", "k", 1, 1).Validate() != nil {
		t.Fatal("valid event rejected")
	}
	if ev("zero", "k", 0, 1).Validate() != nil {
		t.Fatal("epoch-zero timestamp must be valid")
	}
}
