package engine_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/engine"
	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// "每 1 分钟往前看 5 分钟"：窗口 5000ms，步长 1000ms（测试里按毫秒缩放）。
func slidingCfg() engine.Config {
	return engine.Config{
		ShardCount:            2,
		WindowSizeMillis:      5000,
		WindowSlideMillis:     1000,
		OutOfOrderMillis:      0, // WM = 已观察最大事件时间，便于按窗口右端点精确断言
		AllowedLatenessMillis: 1000,
		MaxKeys:               0,
	}
}

func mustSubmit(t *testing.T, e *engine.Engine, ev event.Event) {
	t.Helper()
	if err := e.Submit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
}

func logResults(t *testing.T, sink *endpoint.Sink) {
	t.Helper()
	for _, r := range sink.Results() {
		t.Logf("RESULT kind=%-6s key=%s window=[%d,%d) version=%d sum=%v count=%d",
			r.Kind, r.Key, r.WindowStart, r.WindowEnd, r.Version, r.Sum, r.Count)
	}
	for _, d := range sink.Drops() {
		t.Logf("DROP id=%s key=%s window=[%d,%d) reason=%s",
			d.Ev.ID, d.Ev.Key, d.WindowStart, d.WindowEnd, d.Reason)
	}
}

// TestSlidingOverlappingAggregation 同一事件进入多个窗口：各窗口独立聚合、
// 独立按水位线 FIRE，互不串味；一次推进让多个窗口同时到点时按窗口起点升序输出。
func TestSlidingOverlappingAggregation(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(slidingCfg(), sink, engine.WithLogger(testLoggerShim{t: t}))
	defer e.Close()

	mustSubmit(t, e, event.Event{ID: "e1", Key: "a", Value: 10, EventTime: 100})
	mustSubmit(t, e, event.Event{ID: "e2", Key: "a", Value: 20, EventTime: 1500})
	if len(sink.Results()) != 0 {
		t.Fatalf("WM=1500 未到任何窗口右端点(5000)，不得提前输出: %+v", sink.Results())
	}

	// WM -> 6000：[0,5000) 与 [1000,6000) 同时到点。
	mustSubmit(t, e, event.Event{ID: "e3", Key: "a", Value: 30, EventTime: 6000})
	rs := sink.Results()
	if len(rs) != 2 {
		logResults(t, sink)
		t.Fatalf("同时到点窗口数=2, got %d", len(rs))
	}
	// 输出顺序必须按窗口起点升序，同批输入每次运行一致。
	if rs[0].Kind != window.Fire || rs[0].WindowStart != 0 || rs[0].Sum != 30 || rs[0].Count != 2 {
		t.Fatalf("W[0,5000) 应含 e1+e2 sum=30/c2, got %+v", rs[0])
	}
	if rs[1].Kind != window.Fire || rs[1].WindowStart != 1000 || rs[1].Sum != 20 || rs[1].Count != 1 {
		t.Fatalf("W[1000,6000) 应只含 e2 sum=20/c1, got %+v", rs[1])
	}
	t.Logf("重叠窗口独立聚合: W0={e1,e2}=30, W1000={e2}=20；同时到点按起点升序 FIRE")

	// 多跑一轮验证所有 e3 覆盖窗口各自独立（Close 冲刷未到点窗口）。
	e.Close()
	fin := sink.FinalResults()
	got := map[int64]window.Result{}
	for _, r := range fin {
		if r.Key == "a" {
			got[r.WindowStart] = r
		}
	}
	// e3(t=6000) 属于 [2000,7000)..[6000,11000) 共 5 个窗口，每个只含 e3。
	for _, st := range []int64{2000, 3000, 4000, 5000, 6000} {
		r, ok := got[st]
		if !ok || r.Sum != 30 || r.Count != 1 {
			t.Fatalf("W[%d,%d) 应只含 e3 sum=30/c1, got %+v ok=%v", st, st+5000, r, ok)
		}
	}
	logResults(t, sink)
}

// TestSlidingBoundaryEvents 窗口交界处事件：左闭右开，t=5000 不属于 [0,5000)，
// 而 t=4999 属于；步长网格上的归属连续无重叠缝隙。
func TestSlidingBoundaryEvents(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(slidingCfg(), sink)
	defer e.Close()

	mustSubmit(t, e, event.Event{ID: "b1", Key: "b", Value: 1, EventTime: 4999})
	mustSubmit(t, e, event.Event{ID: "b2", Key: "b", Value: 2, EventTime: 5000}) // WM=5000：W0 恰好到点
	e.Close()

	fin := map[int64]window.Result{}
	for _, r := range sink.FinalResults() {
		fin[r.WindowStart] = r
	}
	// [0,5000) 只含 b1；[1000..4000] 两事件都在；[5000,10000) 只含 b2。
	check := func(start int64, sum float64, count int64) {
		r, ok := fin[start]
		if !ok {
			t.Fatalf("缺少窗口 [%d,%d)", start, start+5000)
		}
		if r.Sum != sum || r.Count != count {
			t.Fatalf("[%d,%d)=sum %v/c%d want sum %v/c%d", start, start+5000, r.Sum, r.Count, sum, count)
		}
	}
	check(0, 1, 1)
	for _, st := range []int64{1000, 2000, 3000, 4000} {
		check(st, 3, 2)
	}
	check(5000, 2, 1)
	t.Log("边界事件归属正确：t=4999∈W0，t=5000∉W0，中间窗口同时含两者")
}

// TestSlidingLateUpdateAndDedup 重叠窗口下的重复去重与宽限内迟到修正都按窗口独立：
// 同一事件 ID 在每个窗口只算一次；迟到事件只修正真正受影响的窗口；超宽限按
// LATE_BEYOND_TOLERANCE 与重复原因分别可观测，且不污染其他窗口。
func TestSlidingLateUpdateAndDedup(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(slidingCfg(), sink, engine.WithLogger(testLoggerShim{t: t}))
	defer e.Close()

	mustSubmit(t, e, event.Event{ID: "e1", Key: "a", Value: 10, EventTime: 100})
	mustSubmit(t, e, event.Event{ID: "e2", Key: "a", Value: 20, EventTime: 1500})
	// e2 重复：它命中 [0,5000) 与 [1000,6000) 两个窗口，必须各判一次重复。
	mustSubmit(t, e, event.Event{ID: "e2", Key: "a", Value: 20, EventTime: 1500})
	if st := e.Stats(); st.DroppedDup != 2 {
		t.Fatalf("重叠窗口重复应按归属计 2 次 DUPLICATE, got %d (%+v)", st.DroppedDup, st)
	}
	// 推进 WM=6000：两窗口 FIRE。
	mustSubmit(t, e, event.Event{ID: "e3", Key: "a", Value: 30, EventTime: 6000})
	// t=200 迟到，但 WM=6000 <= end(5000)+lat(1000)=6000（含边界）：只修正 W0。
	mustSubmit(t, e, event.Event{ID: "late1", Key: "a", Value: 5, EventTime: 200})

	fin := map[int64]window.Result{}
	for _, r := range sink.FinalResults() {
		fin[r.WindowStart] = r
	}
	if r := fin[0]; r.Version != 1 || r.Sum != 35 || r.Count != 3 {
		t.Fatalf("W0 迟到修正后应为 v1 sum=35/c3, got %+v", r)
	}
	if r := fin[1000]; r.Version != 0 || r.Sum != 20 || r.Count != 1 {
		t.Fatalf("W1000 不受 t=200 事件影响, got %+v", r)
	}
	t.Log("迟到只修正 W0（v1=35/c3），W1000 保持 v0=20/c1，互不串味")

	// 越过宽限：WM=7001 > 5000+1000=6000，W0 已清理；t=100 只可能属于 W0 -> 迟到丢弃。
	mustSubmit(t, e, event.Event{ID: "adv", Key: "a", Value: 1, EventTime: 7001})
	mustSubmit(t, e, event.Event{ID: "late2", Key: "a", Value: 9, EventTime: 100})
	var lateDrop *window.Drop
	for i := range sink.Drops() {
		d := sink.Drops()[i]
		if d.Ev.ID == "late2" {
			lateDrop = &d
		}
	}
	if lateDrop == nil || lateDrop.Reason != engine.DropLate || lateDrop.WindowStart != 0 || lateDrop.WindowEnd != 5000 {
		t.Fatalf("late2 应按 %s 记录到窗口 [0,5000), got %+v", engine.DropLate, lateDrop)
	}
	// 修正后的 W0 终值不被超宽限事件改动。
	if r := sink.FinalResults(); len(r) > 0 {
		for _, x := range r {
			if x.Key == "a" && x.WindowStart == 0 && (x.Sum != 35 || x.Count != 3) {
				t.Fatalf("W0 被超宽限事件带偏: %+v", x)
			}
		}
	}
	if e.Stats().PurgedWindows < 1 {
		t.Fatalf("宽限期满应有窗口被清理")
	}
	logResults(t, sink)
}

// TestSlidingDeterministicOrder 多窗口同时到点 + 迟到 UPDATE 混合场景下，
// 同一份输入两次独立运行的完整输出流（含 Kind/Version/顺序）必须完全一致。
func TestSlidingDeterministicOrder(t *testing.T) {
	evs := []event.Event{
		{ID: "r1", Key: "a", Value: 1, EventTime: 100},
		{ID: "r2", Key: "a", Value: 2, EventTime: 4500},
		{ID: "r3", Key: "b", Value: 4, EventTime: 4600},
		{ID: "r4", Key: "a", Value: 8, EventTime: 6000}, // WM(a)=6000：a 的 W0..W1000 到点
		{ID: "r2", Key: "a", Value: 2, EventTime: 4500}, // 重复（窗口级去重）
		{ID: "r5", Key: "a", Value: 16, EventTime: 200}, // W0 宽限内迟到修正
	}
	run := func() string {
		sink := endpoint.NewSink()
		e := engine.New(slidingCfg(), sink)
		for _, ev := range evs {
			_ = e.Submit(context.Background(), ev)
		}
		e.Close()
		b, _ := json.Marshal(sink.Results())
		return string(b)
	}
	x, y := run(), run()
	if x != y {
		t.Fatalf("两次运行输出流不一致:\n%s\n%s", x, y)
	}
	t.Logf("滑动窗口输出流可复现: %s", x)
}

// TestSlidingCapacityBoundary 持续输入下容量边界：
// MaxKeys 按分片活跃 key 数封顶；滑动扇出（每 key 至多 ceil(size/slide)=5 个窗口）
// 有界；新 key 在满员时按 MEMORY_LIMIT_REJECTED 被挡（可观测），回收后容量恢复；
// 已存在 key 的窗口扇出不被新 key 名额误伤。
func TestSlidingCapacityBoundary(t *testing.T) {
	c := slidingCfg()
	c.ShardCount = 1
	c.AllowedLatenessMillis = 0
	c.MaxKeys = 3
	const fanout = 5 // ceil(5000/1000)

	sink := endpoint.NewSink()
	e := engine.New(c, sink)
	defer e.Close()

	for _, k := range []string{"k1", "k2", "k3"} {
		mustSubmit(t, e, event.Event{ID: "e-" + k, Key: k, Value: 1, EventTime: 4500})
	}
	// 全新 key k4：3 个活跃 key 已满员，WM 未推进、无窗口可回收 -> 拒绝其全部 5 个归属。
	mustSubmit(t, e, event.Event{ID: "e-k4", Key: "k4", Value: 1, EventTime: 4500})
	st := e.Stats()
	if st.DroppedMemory != fanout {
		t.Fatalf("k4 五个窗口归属应全部可观测拒绝, DroppedMemory=%d want %d", st.DroppedMemory, fanout)
	}

	// 已存在 key 继续扇出到新窗口：不受新 key 名额限制。
	before := e.Stats().Processed
	mustSubmit(t, e, event.Event{ID: "e-k1b", Key: "k1", Value: 1, EventTime: 5500})
	if e.Stats().DroppedMemory != fanout {
		t.Fatal("已存在 key 的滑动扇出不应被容量挡下")
	}
	if e.Stats().Processed-before != fanout {
		t.Fatalf("k1 在 t=5500 应被 5 个窗口接受, got %d", e.Stats().Processed-before)
	}

	// 任意时刻 (窗口,key) 条目数不超过 MaxKeys*fanout（提交结束后观察）。
	if n := e.Stats().ActiveWindows; n > c.MaxKeys*fanout {
		t.Fatalf("活跃窗口无界: %d > %d", n, c.MaxKeys*fanout)
	}

	// 推进 WM 让全部旧窗口过宽限期并回收：容量恢复，k4 可正常进入。
	mustSubmit(t, e, event.Event{ID: "adv", Key: "k1", Value: 1, EventTime: 20000})
	snap := e.SaveSnapshot()
	keys := map[string]map[int64]struct{}{}
	for _, ss := range snap.Shards {
		for _, a := range ss.Windows {
			if keys[a.Key] == nil {
				keys[a.Key] = map[int64]struct{}{}
			}
			keys[a.Key][a.Start] = struct{}{}
		}
	}
	// k2/k3 的全部窗口都应已过宽限期被回收；k1 仅保留 adv 新开的存活窗口。
	if len(keys["k2"]) != 0 || len(keys["k3"]) != 0 {
		t.Fatalf("旧 key 窗口应全部回收, 残留: %v", keys)
	}
	if _, ok := keys["k4"]; ok {
		t.Fatal("被拒的 k4 绝不能留下任何状态")
	}
	t.Logf("回收后存活 key/窗口: %v", keys)
	mustSubmit(t, e, event.Event{ID: "e-k4b", Key: "k4", Value: 1, EventTime: 20000})
	for _, d := range sink.Drops() {
		if d.Ev.ID == "e-k4b" {
			t.Fatalf("容量恢复后 k4 不应被拒: %+v", d)
		}
	}
	if n := e.Stats().ActiveWindows; n > c.MaxKeys*fanout {
		t.Fatalf("回收后活跃窗口越界: %d", n)
	}
	t.Logf("容量边界: 满员拒绝可观测(DroppedMemory=%d)，回收后恢复，活跃窗口恒 <=%d",
		fanout, c.MaxKeys*fanout)
}

// TestSlidingSnapshotRestoreReplay 滑动窗口中途快照 -> 恢复 -> 重放：
// 每个归属都识别为重复、零新增输出；之后继续处理的终值与一次性运行完全一致；
// 同一状态连续两次快照字节一致。
func TestSlidingSnapshotRestoreReplay(t *testing.T) {
	c := slidingCfg()
	first := []event.Event{
		{ID: "s1", Key: "k1", Value: 1, EventTime: 100},
		{ID: "s2", Key: "k1", Value: 2, EventTime: 1500},
		{ID: "s3", Key: "k2", Value: 10, EventTime: 4500},
		{ID: "s4", Key: "k1", Value: 3, EventTime: 6000}, // WM(k1)=6000：W0/W1000 FIRE
	}

	sink1 := endpoint.NewSink()
	e1 := engine.New(c, sink1)
	for _, ev := range first {
		mustSubmit(t, e1, ev)
	}
	snap := e1.SaveSnapshot()
	raw1, _ := json.Marshal(snap)
	raw2, _ := json.Marshal(e1.SaveSnapshot())
	if string(raw1) != string(raw2) {
		t.Fatal("滑动窗口同一状态两次快照字节不一致")
	}
	st1 := e1.Stats()
	if len(sink1.Results()) != 2 {
		logResults(t, sink1)
		t.Fatalf("快照点应有 k1 的 W0/W1000 两条 FIRE, got %d", len(sink1.Results()))
	}

	// JSON 落盘往返后恢复到【同一个汇聚出口】（模拟重启后继续向同一出口输出）：
	// 快照前结果保留在 sink1 中，恢复后重放绝不允许产生重复输出。
	var snap2 engine.Snapshot
	if err := json.Unmarshal(raw1, &snap2); err != nil {
		t.Fatal(err)
	}
	e2 := engine.New(c, sink1)
	if err := e2.Restore(&snap2); err != nil {
		t.Fatal(err)
	}
	if st := e2.Stats(); st.Processed != st1.Processed || st.ResultsEmitted != st1.ResultsEmitted {
		t.Fatalf("恢复后计数器不一致: %+v vs %+v", st, st1)
	}
	outputsBeforeReplay := len(sink1.Results())
	// 重放快照点前全部事件：按窗口归属全部判重，零新增输出。
	for _, ev := range first {
		mustSubmit(t, e2, ev)
	}
	if got := len(sink1.Results()); got != outputsBeforeReplay {
		logResults(t, sink1)
		t.Fatalf("重放不得产生任何输出, %d -> %d", outputsBeforeReplay, got)
	}
	wantDups := 0
	for _, ev := range first {
		// 归属数在快照点时刻（WM 可能已让窗口关闭，但聚合在宽限期内仍存活）：
		// 直接按 Spec 网格计算，重放时这些聚合都还在。
		starts := windowStarts(ev.EventTime, 5000, 1000)
		wantDups += len(starts)
	}
	if got := e2.Stats().DroppedDup; got != int64(wantDups) {
		t.Fatalf("重放重复归属数=%d want %d", got, wantDups)
	}
	t.Logf("重放 %d 事件 -> %d 个窗口归属全部 DUPLICATE，零新增输出", len(first), wantDups)

	// 快照点之后继续输入。
	second := []event.Event{
		{ID: "s5", Key: "k2", Value: 7, EventTime: 7000},
		{ID: "s6", Key: "k1", Value: 4, EventTime: 300}, // W0 宽限内迟到修正
		{ID: "s7", Key: "k1", Value: 9, EventTime: 9000},
	}
	for _, ev := range second {
		mustSubmit(t, e2, ev)
	}
	e2.Close()

	// 参照：一次性运行全部事件。
	ref := endpoint.NewSink()
	er := engine.New(c, ref)
	for _, ev := range append(append([]event.Event{}, first...), second...) {
		mustSubmit(t, er, ev)
	}
	er.Close()

	gotFin, _ := json.Marshal(sink1.FinalResults())
	wantFin, _ := json.Marshal(ref.FinalResults())
	if string(gotFin) != string(wantFin) {
		t.Fatalf("快照恢复续跑终值与一次性运行不一致:\n%s\n%s", gotFin, wantFin)
	}
	t.Logf("快照恢复续跑终值与一次性运行一致: %s", gotFin)
}

// TestSlidingEqualsTumblingWhenSlideEqualsSize 步长配成与窗口一样大时，
// 行为与不配置步长的滚动窗口完全一致。
func TestSlidingEqualsTumblingWhenSlideEqualsSize(t *testing.T) {
	evs := []event.Event{
		{ID: "t1", Key: "a", Value: 1, EventTime: 100},
		{ID: "t2", Key: "a", Value: 2, EventTime: 1600},
		{ID: "t3", Key: "b", Value: 4, EventTime: 900},
		{ID: "t1", Key: "a", Value: 1, EventTime: 100},
		{ID: "t4", Key: "a", Value: 8, EventTime: 2100},
		{ID: "t5", Key: "b", Value: 16, EventTime: 50},
	}
	run := func(slide int64) string {
		cfg := engine.Config{
			ShardCount:            3,
			WindowSizeMillis:      1000,
			WindowSlideMillis:     slide,
			OutOfOrderMillis:      300,
			AllowedLatenessMillis: 1500,
		}
		sink := endpoint.NewSink()
		e := engine.New(cfg, sink)
		for _, ev := range evs {
			_ = e.Submit(context.Background(), ev)
		}
		e.Close()
		type bundle struct {
			Results []window.Result `json:"results"`
			Drops   []window.Drop   `json:"drops"`
			Stats   engine.Stats    `json:"stats"`
		}
		b, _ := json.Marshal(bundle{sink.FinalResults(), sink.Drops(), e.Stats()})
		return string(b)
	}
	// slide=0（缺省滚动）与 slide=1000（显式相等）必须逐字节一致。
	if run(0) != run(1000) {
		t.Fatalf("slide=0 与 slide=size 行为不一致:\n%s\n%s", run(0), run(1000))
	}
	t.Log("slide 缺省与 slide=size 配置下结果/丢弃/计数完全一致")
}

// windowStarts 测试辅助：与 window.Spec.StartsFor 相同的网格计算，避免导出测试耦合。
func windowStarts(t, size, slide int64) []int64 {
	var out []int64
	last := t - (t % slide)
	n := (size + slide - 1) / slide
	for st := last - (n-1)*slide; st <= last; st += slide {
		if st < 0 || st+size <= t {
			continue
		}
		out = append(out, st)
	}
	return out
}

// testLoggerShim 适配 engine.WithLogger。
type testLoggerShim struct{ t *testing.T }

func (l testLoggerShim) Logf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf(format, args...)
}
