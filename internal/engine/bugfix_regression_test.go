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

// submitAll 依次提交事件并返回引擎（调用方负责 Close）。
func submitAll(t *testing.T, e *engine.Engine, evs ...event.Event) {
	t.Helper()
	for _, ev := range evs {
		if err := e.Submit(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
}

// logResults 把原始输出序列打到测试日志（事件时间/水位线/窗口/版本可回溯）。
func logResults(t *testing.T, sink *endpoint.Sink) {
	t.Helper()
	for i, r := range sink.Results() {
		t.Logf("OUT[%d] kind=%s key=%s window=[%d,%d) version=%d sum=%v count=%d",
			i, r.Kind, r.Key, r.WindowStart, r.WindowEnd, r.Version, r.Sum, r.Count)
	}
}

// TestFireAlwaysPrecedesUpdateTumbling 回归：滚动退化（slide==size）下，
// "水位线已越过窗口右端之后，较早的事件才补进来"时，该窗口的首次关闭（FIRE v0）
// 必须在【同一次提交】内发生，且永远排在它的任何修正（UPDATE v1..）之前。
// 场景：size=1000、无乱序容忍、宽限给足，依次提交事件时间 1300、400、700。
func TestFireAlwaysPrecedesUpdateTumbling(t *testing.T) {
	cfg := engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      1000,
		SlideMillis:           1000, // 显式退化：与滚动窗口一致
		OutOfOrderMillis:      0,
		AllowedLatenessMillis: 100000, // 宽限给足，排除迟到丢弃干扰
	}
	sink := endpoint.NewSink()
	e := engine.New(cfg, sink, engine.WithLogger(slidingLogger{t}))

	submitAll(t, e,
		event.Event{ID: "e1", Key: "k", Value: 1, EventTime: 1300}, // 命中 [1000,2000)，WM=1300
		event.Event{ID: "e2", Key: "k", Value: 2, EventTime: 400},  // 命中 [0,1000)，WM 已越过右端
		event.Event{ID: "e3", Key: "k", Value: 3, EventTime: 700},  // 命中 [0,1000)，宽限内修正
	)
	logResults(t, sink)

	rs := sink.Results()
	// 老行为：e2 到达时 [0,1000) 立即首次关闭（FIRE v0，含 e2）；
	// e3 到达时该窗口已关闭 -> UPDATE v1（含 e2+e3）。绝不允许先 v1 后 v0。
	if len(rs) != 2 {
		t.Fatalf("应恰好 2 条输出（[0,1000) 的 FIRE+UPDATE），got %d: %+v", len(rs), rs)
	}
	if rs[0].Kind != window.Fire || rs[0].WindowStart != 0 || rs[0].Version != 0 {
		t.Fatalf("首条输出必须是 [0,1000) FIRE v0, got %+v", rs[0])
	}
	if rs[0].Sum != 2 || rs[0].Count != 1 {
		t.Fatalf("FIRE 应只含 e2: sum=%v count=%d", rs[0].Sum, rs[0].Count)
	}
	if rs[1].Kind != window.Update || rs[1].WindowStart != 0 || rs[1].Version != 1 {
		t.Fatalf("第二条输出必须是 [0,1000) UPDATE v1, got %+v", rs[1])
	}
	if rs[1].Sum != 5 || rs[1].Count != 2 {
		t.Fatalf("UPDATE 应含 e2+e3: sum=%v count=%d", rs[1].Sum, rs[1].Count)
	}
	e.Close()
}

// TestMaxKeysOnlyBlocksGenuinelyNewKeys 回归：MaxKeys 只挡"真正的新 key"，
// 已活跃 key 在重叠窗口间扩展（同一 key 同时落入多个窗口）不受 key 数上限影响
// （总量由 MaxWindows 兜底）。size=5000、slide=1000、MaxKeys=2：
// 第二个 key 必须进入覆盖它的全部 5 个窗口；第三个（超限新）key 才被容量拒绝，
// 且拒绝只落在它自己进不去的窗口、不殃及既有 key 的结果。
func TestMaxKeysOnlyBlocksGenuinelyNewKeys(t *testing.T) {
	cfg := engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      5000,
		SlideMillis:           1000,
		OutOfOrderMillis:      0,
		AllowedLatenessMillis: 0,
		MaxKeys:               2,
	}
	sink := endpoint.NewSink()
	e := engine.New(cfg, sink, engine.WithLogger(slidingLogger{t}))

	// k1@4500 命中 [0,5000)..[4000,9000) 共 5 个窗口（第一个 key，正常进入）。
	submitAll(t, e, event.Event{ID: "a1", Key: "k1", Value: 1, EventTime: 4500})
	if n := e.Stats().ActiveWindows; n != 5 {
		t.Fatalf("k1 应占 5 个重叠窗口, active=%d", n)
	}
	// k2@4600 同样命中 5 个窗口：它是第二个 key（未超限），必须 5 个全进，
	// 不得因"同一 key 落到多个窗口"被误挡。
	submitAll(t, e, event.Event{ID: "b1", Key: "k2", Value: 2, EventTime: 4600})
	if n := e.Stats().DroppedMemory; n != 0 {
		t.Fatalf("k2 未超 key 上限，不应有任何容量拒绝, DroppedMemory=%d", n)
	}
	if n := e.Stats().ActiveWindows; n != 10 {
		t.Fatalf("k2 应进入全部 5 个窗口, active=%d want 10", n)
	}
	// k3@4700 是真正超限的新 key：它命中的每个窗口都应各记一条 MEMORY_LIMIT_REJECTED，
	// 与迟到/重复原因区分清楚。
	submitAll(t, e, event.Event{ID: "c1", Key: "k3", Value: 4, EventTime: 4700})
	if n := e.Stats().DroppedMemory; n != 5 {
		t.Fatalf("k3 的 5 个目标窗口应各记一次容量拒绝, DroppedMemory=%d want 5", n)
	}
	for _, d := range sink.Drops() {
		if d.Reason != engine.DropMemoryLimit || d.Ev.Key != "k3" {
			t.Fatalf("拒绝记录必须是 k3 的 MEMORY_LIMIT_REJECTED, got %+v", d)
		}
		t.Logf("DROP id=%s key=%s window=[%d,%d) reason=%s", d.Ev.ID, d.Ev.Key, d.WindowStart, d.WindowEnd, d.Reason)
	}
	// 已活跃 key 后续继续开新的重叠窗口（随水位线推进）也不受 MaxKeys 影响。
	submitAll(t, e, event.Event{ID: "a2", Key: "k1", Value: 8, EventTime: 5500})
	if n := e.Stats().DroppedMemory; n != 5 {
		t.Fatalf("k1 开新窗口不应被 MaxKeys 挡, DroppedMemory=%d want 5", n)
	}

	// 被拒的 k3 不得产生任何窗口结果；k1/k2 的结果不被带偏。
	e.Close()
	for _, r := range sink.FinalResults() {
		if r.Key == "k3" {
			t.Fatalf("被容量拒绝的 k3 不得产生任何窗口结果: %+v", r)
		}
	}
	for _, start := range []int64{0, 1000, 2000, 3000, 4000} {
		r := finalByStart(t, sink, "k2", start)
		if r.Sum != 2 || r.Count != 1 {
			t.Fatalf("k2 窗口 [%d,%d) 应只含 b1: sum=%v count=%d", start, start+5000, r.Sum, r.Count)
		}
	}
	r := finalByStart(t, sink, "k1", 4000)
	if r.Sum != 9 || r.Count != 2 { // a1(1@4500)+a2(8@5500) 都覆盖 [4000,9000)
		t.Fatalf("k1 [4000,9000) 应含 a1+a2=9/2: sum=%v count=%d", r.Sum, r.Count)
	}
	r5 := finalByStart(t, sink, "k1", 5000)
	if r5.Sum != 8 || r5.Count != 1 {
		t.Fatalf("k1 [5000,10000) 应只含 a2: sum=%v count=%d", r5.Sum, r5.Count)
	}
	t.Logf("MaxKeys=2：k2 的 5 个重叠窗口全部进入，k3 被容量拒绝 5 次且未污染任何结果")
}

// TestTumblingDegenerateMatchesLegacyByteForByte 滚动退化（slide==size 或 slide 缺省）
// 配置下，同一批输入的【原始输出序列】必须与滑动窗口引入前的老版本逐字节一致。
// goldenResults 由老实现（commit 96d81dc，无 SlideMillis 概念）对同一输入实际跑出；
// 输入特意包含"水位线已越过窗口右端后、较早事件才补进来"的场景（e2/e3/e5/e6/e7）。
func TestTumblingDegenerateMatchesLegacyByteForByte(t *testing.T) {
	evs := []event.Event{
		{ID: "e1", Key: "k", Value: 1, EventTime: 1300},
		{ID: "e2", Key: "k", Value: 2, EventTime: 400},  // WM 已越过 [0,1000) 右端
		{ID: "e3", Key: "k", Value: 3, EventTime: 700},  // 宽限内修正 [0,1000)
		{ID: "e4", Key: "k", Value: 4, EventTime: 2500}, // 推进 WM，关 [1000,2000)
		{ID: "e5", Key: "k", Value: 5, EventTime: 100},  // 再次修正 [0,1000)
		{ID: "e3", Key: "k", Value: 3, EventTime: 700},  // 重复
		{ID: "e6", Key: "q", Value: 6, EventTime: 900},  // 另一 key，同样补进已越过窗口
		{ID: "e7", Key: "q", Value: 7, EventTime: 950},  // q 的 [0,1000) 修正
		{ID: "e8", Key: "k", Value: 8, EventTime: 3200}, // 推进 WM，关 [2000,3000)
	}
	const goldenResults = `[{"key":"k","window_start":0,"window_end":1000,"sum":2,"count":1,"kind":"FIRE","version":0},{"key":"k","window_start":0,"window_end":1000,"sum":5,"count":2,"kind":"UPDATE","version":1},{"key":"k","window_start":1000,"window_end":2000,"sum":1,"count":1,"kind":"FIRE","version":0},{"key":"k","window_start":0,"window_end":1000,"sum":10,"count":3,"kind":"UPDATE","version":2},{"key":"q","window_start":0,"window_end":1000,"sum":6,"count":1,"kind":"FIRE","version":0},{"key":"q","window_start":0,"window_end":1000,"sum":13,"count":2,"kind":"UPDATE","version":1},{"key":"k","window_start":2000,"window_end":3000,"sum":4,"count":1,"kind":"FIRE","version":0},{"key":"k","window_start":3000,"window_end":4000,"sum":8,"count":1,"kind":"FIRE","version":0}]`

	run := func(slide int64) (string, []window.Drop, engine.Stats) {
		cfg := engine.Config{
			ShardCount:            1,
			WindowSizeMillis:      1000,
			SlideMillis:           slide,
			OutOfOrderMillis:      0,
			AllowedLatenessMillis: 100000,
		}
		sink := endpoint.NewSink()
		e := engine.New(cfg, sink, engine.WithLogger(slidingLogger{t}))
		submitAll(t, e, evs...)
		e.Close()
		logResults(t, sink)
		jr, _ := json.Marshal(sink.Results())
		return string(jr), sink.Drops(), e.Stats()
	}

	for _, slide := range []int64{0, 1000} { // 缺省滚动 / 显式退化，两种配法都要一致
		got, drops, st := run(slide)
		if got != goldenResults {
			t.Fatalf("slide=%d 原始输出序列与老版本不一致:\ngot    %s\ngolden %s", slide, got, goldenResults)
		}
		if len(drops) != 1 || drops[0].Reason != window.ReasonDuplicate || drops[0].Ev.ID != "e3" {
			t.Fatalf("slide=%d 应只有 e3 一条重复丢弃, got %+v", slide, drops)
		}
		if st.ResultsEmitted != 8 || st.DroppedDup != 1 || st.DroppedLate != 0 {
			t.Fatalf("slide=%d 计数器与老版本口径不符: %+v", slide, st)
		}
		t.Logf("slide=%d：%d 条原始输出与老版本逐字节一致（含 FIRE 先于 UPDATE 的版本次序）", slide, st.ResultsEmitted)
	}
}

// TestSlidingFireOrderAndDeterminism 滑动窗口下：
//  1. 一次提交同时让多个窗口到点时，这些窗口按 (windowStart, key) 稳定顺序
//     【一次性全部】输出，不拖到下一次提交，也没有到点窗口漏关；
//  2. 同一窗口的首次关闭（FIRE v0）永远先于它的任何修正（UPDATE v1..），
//     包括"迟到事件新建了一个早已到点的窗口"——该窗口必须同次提交立即 FIRE；
//  3. 同一批输入重复跑，原始输出序列逐条一致。
func TestSlidingFireOrderAndDeterminism(t *testing.T) {
	cfg := engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      5000,
		SlideMillis:           1000,
		OutOfOrderMillis:      0,
		AllowedLatenessMillis: 10000,
	}
	evs := []event.Event{
		{ID: "a1", Key: "k1", Value: 1, EventTime: 1000},   // [0,5000) [1000,6000)
		{ID: "b1", Key: "k2", Value: 2, EventTime: 1500},   // [0,5000) [1000,6000)
		{ID: "adv", Key: "k1", Value: 0, EventTime: 8000},  // WM=8000：[0,5000)[1000,6000) 同时到点
		{ID: "late", Key: "k1", Value: 5, EventTime: 2500}, // 修正 [0,5000)[1000,6000)；新建即到点的 [2000,7000)
	}

	run := func() []window.Result {
		sink := endpoint.NewSink()
		e := engine.New(cfg, sink, engine.WithLogger(slidingLogger{t}))

		submitAll(t, e, evs[0], evs[1])
		if n := len(sink.Results()); n != 0 {
			t.Fatalf("WM=1500 无窗口到点，不应有输出, got %d", n)
		}

		// 第三次提交让 [0,5000) 与 [1000,6000)（各含 k1、k2）同时到点：
		// 必须在【本次提交】一次性全部 FIRE，顺序 (start,key) 稳定。
		submitAll(t, e, evs[2])
		rs := sink.Results()
		if len(rs) != 4 {
			t.Fatalf("一次提交到点的 4 个窗口必须一次性全部输出, got %d: %+v", len(rs), rs)
		}
		wantOrder := []struct {
			key   string
			start int64
		}{{"k1", 0}, {"k2", 0}, {"k1", 1000}, {"k2", 1000}}
		for i, w := range wantOrder {
			if rs[i].Kind != window.Fire || rs[i].Version != 0 || rs[i].Key != w.key || rs[i].WindowStart != w.start {
				t.Fatalf("到点批次第 %d 条应为 %s@[%d,..) FIRE v0, got %+v", i, w.key, w.start, rs[i])
			}
		}
		// 没有到点窗口被拖后：显式 Drain 不应再产出任何东西。
		e.Drain()
		if n := len(sink.Results()); n != 4 {
			t.Fatalf("到点窗口不得拖到后续 Drain/提交, got 新增 %d 条", n-4)
		}

		// 迟到 late@2500：修正两个已关窗口（UPDATE v1），并新建早已到点的 [2000,7000)
		// ——该窗口必须同次提交立即 FIRE v0，且排在本窗口后续任何 UPDATE 之前。
		submitAll(t, e, evs[3])
		rs = sink.Results()
		if len(rs) != 7 {
			t.Fatalf("迟到提交应产出 2 条 UPDATE + 1 条即到点 FIRE, got %d: %+v", len(rs)-4, rs[4:])
		}
		batch := rs[4:]
		if batch[0].Kind != window.Update || batch[0].WindowStart != 0 || batch[0].Version != 1 {
			t.Fatalf("应为 [0,5000) UPDATE v1, got %+v", batch[0])
		}
		if batch[1].Kind != window.Update || batch[1].WindowStart != 1000 || batch[1].Version != 1 {
			t.Fatalf("应为 [1000,6000) UPDATE v1, got %+v", batch[1])
		}
		if batch[2].Kind != window.Fire || batch[2].WindowStart != 2000 || batch[2].Version != 0 {
			t.Fatalf("新建即到点的 [2000,7000) 必须同次提交 FIRE v0, got %+v", batch[2])
		}
		e.Close()

		// 全文核对：同一 (key,window) 的版本号在输出流中严格递增（FIRE v0 必在最前）。
		type winID struct {
			key   string
			start int64
		}
		seen := map[winID]int64{}
		for i, r := range sink.Results() {
			id := winID{r.Key, r.WindowStart}
			if prev, ok := seen[id]; ok && r.Version <= prev {
				t.Fatalf("OUT[%d] %s@%d version=%d 出现在 version=%d 之后（首次关闭必须先于修正）",
					i, r.Key, r.WindowStart, r.Version, prev)
			}
			seen[id] = r.Version
			if r.Version == 0 && r.Kind != window.Fire {
				t.Fatalf("OUT[%d] version=0 必须是 FIRE, got %+v", i, r)
			}
		}
		return sink.Results()
	}

	a := run()
	b := run()
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatalf("同批输入两次运行原始输出不一致:\n%s\n%s", ja, jb)
	}
	t.Logf("滑动窗口：多窗口同次到点一次性有序输出、FIRE 恒先于 UPDATE、两次运行 %d 条输出逐条一致", len(a))
}
