package watermark

import "testing"

func TestObserveAdvancesMonotonically(t *testing.T) {
	// outOfOrder=1000：WM = maxSeen - 1000，下限 0
	w := New(1000, 0)

	cases := []struct {
		eventTime int64
		advance   bool
		wantWM    int64
		late      bool
	}{
		{500, false, 0, false},    // maxSeen=500 -> WM 被下限钳为 0，等于当前水位线，不算前进
		{400, false, 0, false},    // 乱序旧事件：maxSeen 不变，WM 不前进
		{2000, true, 1000, false}, // 推高 WM 到 1000；被观察事件自身不迟到
		{2000, false, 1000, false},
		{1500, false, 1000, false}, // 乱序但仍 >= WM：IsLate=false
		{900, false, 1000, true},   // 落后于 WM：迟到
		{3000, true, 2000, false},
	}
	for _, c := range cases {
		advanced := w.Observe(c.eventTime)
		t.Logf("Observe eventTime=%d advanced=%v watermark=%d (maxSeen=%d) isLate(now)=%v",
			c.eventTime, advanced, w.Current(), w.MaxSeen(), w.IsLate(c.eventTime))
		if advanced != c.advance {
			t.Fatalf("eventTime=%d advance=%v want %v", c.eventTime, advanced, c.advance)
		}
		if w.Current() != c.wantWM {
			t.Fatalf("eventTime=%d watermark=%d want %d", c.eventTime, w.Current(), c.wantWM)
		}
		if w.IsLate(c.eventTime) != c.late {
			t.Fatalf("eventTime=%d isLate=%v want %v", c.eventTime, w.IsLate(c.eventTime), c.late)
		}
	}
}

func TestWatermarkNeverRegresses(t *testing.T) {
	w := New(0, 0)
	w.Observe(5000)
	if w.Current() != 5000 {
		t.Fatalf("WM=%d want 5000", w.Current())
	}
	w.Observe(100) // 大幅倒退
	t.Logf("after late event: maxSeen=%d watermark=%d", w.MaxSeen(), w.Current())
	if w.Current() != 5000 {
		t.Fatalf("watermark regressed to %d", w.Current())
	}
}

func TestWatermarkStartFloor(t *testing.T) {
	w := New(1000, 3000)
	if w.Current() != 3000 {
		t.Fatalf("initial WM=%d want 3000", w.Current())
	}
	if w.Observe(3500) {
		t.Fatal("WM should not advance above floor: maxSeen-1000=2500 < 3000")
	}
	t.Logf("eventTime=3500 maxSeen=%d watermark=%d (floored at 3000)", w.MaxSeen(), w.Current())
	if !w.Observe(5000) {
		t.Fatal("WM should advance to 4000")
	}
	if w.Current() != 4000 {
		t.Fatalf("WM=%d want 4000", w.Current())
	}
}

func TestWatermarkSnapshotRestore(t *testing.T) {
	w := New(500, 0)
	w.Observe(10_000)
	t.Logf("pre-snapshot: maxSeen=%d watermark=%d", w.MaxSeen(), w.Current())
	s := w.Snapshot()
	w2 := Restore(s)
	if w2.Current() != w.Current() || w2.MaxSeen() != w.MaxSeen() {
		t.Fatalf("restored WM mismatch: (%d,%d) vs (%d,%d)",
			w2.Current(), w2.MaxSeen(), w.Current(), w.MaxSeen())
	}
	// 恢复后继续观察旧事件，水位线仍不得倒退
	if w2.Observe(1000) {
		t.Fatal("restored watermark must not regress")
	}
	t.Logf("post-restore Observe(1000) watermark=%d", w2.Current())
}
