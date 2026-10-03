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

// TestSlidingSpecStartsFor 验证"每 slide 往前看 size"的重叠窗口归属：
// size=5000、slide=1000 时，同一事件最多进入 5 个窗口，起点升序、左闭右开、
// 起点按步长对齐，且 slide==size 时退化为唯一窗口（滚动语义不变）。
func TestSlidingSpecStartsFor(t *testing.T) {
	s := Spec{SizeMillis: 5000, SlideMillis: 1000}
	cases := []struct {
		t    int64
		want []int64
	}{
		{0, []int64{0}},                               // 早期：窗口起点不为负，只有 [0,5000)
		{999, []int64{0}},                             // 仍只被 [0,5000) 覆盖
		{1000, []int64{0, 1000}},                      // 交界：[0,5000) 与 [1000,6000)
		{4999, []int64{0, 1000, 2000, 3000, 4000}},    // 5 个窗口全中
		{5000, []int64{1000, 2000, 3000, 4000, 5000}}, // 左闭右开：t=5000 不再属于 [0,5000)
		{6300, []int64{2000, 3000, 4000, 5000, 6000}},
	}
	for _, c := range cases {
		got := s.StartsFor(c.t)
		if len(got) != len(c.want) {
			t.Fatalf("t=%d starts=%v want %v", c.t, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("t=%d starts=%v want %v", c.t, got, c.want)
			}
			if got[i]%1000 != 0 {
				t.Fatalf("窗口起点未按步长对齐: %d", got[i])
			}
			if !(got[i] <= c.t && c.t < got[i]+5000) {
				t.Fatalf("窗口 [%d,%d) 不覆盖 t=%d", got[i], got[i]+5000, c.t)
			}
		}
		if last := s.Start(c.t); last != c.want[len(c.want)-1] {
			t.Fatalf("t=%d Start()=%d 应为 StartsFor 最右起点 %d", c.t, last, c.want[len(c.want)-1])
		}
		t.Logf("eventTime=%d 命中 %d 个滑动窗口: %v", c.t, len(got), got)
	}

	// 步长 == 窗口大小：退化为滚动窗口，StartsFor 恰有一个元素且与 Start 相同
	tum := Spec{SizeMillis: 1000, SlideMillis: 1000}
	for _, ts := range []int64{0, 999, 1000, 2500} {
		got := tum.StartsFor(ts)
		want := ts - ts%1000
		if len(got) != 1 || got[0] != want || tum.Start(ts) != want {
			t.Fatalf("滚动退化 t=%d got=%v want=[%d]", ts, got, want)
		}
	}
	// 缺省步长（0）同样按滚动处理
	def := Spec{SizeMillis: 1000}
	if def.Sliding() || len(def.StartsFor(2500)) != 1 {
		t.Fatal("SlideMillis=0 必须等价于滚动窗口")
	}
}

// TestSlidingSpecNonDivisible 验证 size 不是 slide 整数倍时仍有确定归属：
// 窗口个数 = ceil(size/slide)，每个事件都被连续若干个步长对齐窗口覆盖。
func TestSlidingSpecNonDivisible(t *testing.T) {
	s := Spec{SizeMillis: 500, SlideMillis: 300} // ceil(5/3)=2 个窗口
	for ts := int64(0); ts <= 2000; ts++ {
		got := s.StartsFor(ts)
		if len(got) < 1 || len(got) > 2 {
			t.Fatalf("t=%d 窗口数=%d 应在 1..2 之间: %v", ts, len(got), got)
		}
		for _, st := range got {
			if st%300 != 0 || !(st <= ts && ts < st+500) {
				t.Fatalf("t=%d 非法归属窗口 [%d,%d)", ts, st, st+500)
			}
		}
	}
	t.Log("size=500/slide=300: 全时间轴归属连续且确定（1~2 个窗口）")
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
