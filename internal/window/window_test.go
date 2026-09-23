package window

import (
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/event"
)

func TestSpecWindowBounds(t *testing.T) {
	s := Spec{SizeMillis: 1000}
	cases := []struct {
		eventTime, wantStart, wantEnd int64
	}{
		{0, 0, 1000},
		{999, 0, 1000},
		{1000, 1000, 2000},
		{2500, 2000, 3000},
	}
	for _, c := range cases {
		got := s.Start(c.eventTime)
		if got != c.wantStart || s.End(got) != c.wantEnd {
			t.Fatalf("t=%d window=[%d,%d) want [%d,%d)", c.eventTime, got, s.End(got), c.wantStart, c.wantEnd)
		}
		t.Logf("eventTime=%d -> window [%d,%d) 判定依据: start=t-t%%size=%d end=start+size=%d",
			c.eventTime, got, s.End(got), got, s.End(got))
	}
}

func TestAggAddAndDedup(t *testing.T) {
	a := NewAgg("k1", 0, 1000)
	evs := []event.Event{
		{ID: "e1", Key: "k1", Value: 10, EventTime: 100},
		{ID: "e2", Key: "k1", Value: 20, EventTime: 500},
		{ID: "e1", Key: "k1", Value: 10, EventTime: 100}, // 重复 ID
	}
	for i, e := range evs {
		ok, reason := a.Add(e)
		t.Logf("Add eventTime=%d id=%s accepted=%v reason=%q", e.EventTime, e.ID, ok, reason)
		if i < 2 && !ok {
			t.Fatalf("event %s should be accepted", e.ID)
		}
		if i == 2 {
			if ok || reason != ReasonDuplicate {
				t.Fatalf("duplicate event: accepted=%v reason=%q, want %q", ok, reason, ReasonDuplicate)
			}
		}
	}
	r := a.ToResult(Fire, 0)
	if r.Count != 2 || r.Sum != 30 {
		t.Fatalf("result sum=%v count=%d want sum=30 count=2", r.Sum, r.Count)
	}
	t.Logf("FIRE result key=%s window=[%d,%d) sum=%v count=%d version=%d",
		r.Key, r.WindowStart, r.WindowEnd, r.Sum, r.Count, r.Version)
}

func TestAggLateUpdateVersionSemantics(t *testing.T) {
	a := NewAgg("k1", 1000, 2000)
	a.Add(event.Event{ID: "e1", Key: "k1", Value: 10, EventTime: 1100})
	a.Close()
	fire := a.ToResult(Fire, 0)

	// 宽限期内的迟到修正：版本严格递增，Fire 只发生一次
	ok, _ := a.Add(event.Event{ID: "e2", Key: "k1", Value: 5, EventTime: 1200})
	if !ok {
		t.Fatal("late event within tolerance must be accepted")
	}
	upd := a.ToResult(Update, 1)
	if upd.Sum != 15 || upd.Count != 2 || upd.Version != 1 || upd.Kind != Update {
		t.Fatalf("update result = %+v", upd)
	}
	t.Logf("FIRED window=[1000,2000) sum=%v count=%d; 迟到事件 eventTime=1200 -> UPDATE version=%d sum=%v count=%d",
		fire.Sum, fire.Count, upd.Version, upd.Sum, upd.Count)
}

func TestAggSnapshotRestore(t *testing.T) {
	a := NewAgg("k1", 0, 1000)
	a.Add(event.Event{ID: "e1", Key: "k1", Value: 10, EventTime: 100})
	a.Add(event.Event{ID: "e2", Key: "k1", Value: 20, EventTime: 200})
	a.Close()
	snap := a.Snapshot()
	b := RestoreAgg(snap)

	// 恢复后重放已消费事件：必须被识别为重复，不漏计也不双计
	for _, id := range []string{"e1", "e2"} {
		ok, reason := b.Add(event.Event{ID: id, Key: "k1", Value: 10, EventTime: 100})
		t.Logf("replay id=%s accepted=%v reason=%q", id, ok, reason)
		if ok || reason != ReasonDuplicate {
			t.Fatalf("replayed event %s must be DUPLICATE, got accepted=%v", id, ok)
		}
	}
	// 快照点之后的新事件照常计入
	ok, _ := b.Add(event.Event{ID: "e3", Key: "k1", Value: 5, EventTime: 300})
	if !ok {
		t.Fatal("post-snapshot event should be accepted")
	}
	r := b.ToResult(Update, 1)
	if !b.Closed() || r.Sum != 35 || r.Count != 3 {
		t.Fatalf("restored agg mismatch: closed=%v sum=%v count=%d", b.Closed(), r.Sum, r.Count)
	}
	t.Logf("after restore+replay, window=[%d,%d) closed=%v sum=%v count=%d (期望 35/3，重放不双计)",
		r.WindowStart, r.WindowEnd, b.Closed(), r.Sum, r.Count)
}
