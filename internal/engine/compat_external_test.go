package engine_test

import (
	"encoding/json"
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/engine"
	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// tumblingCompatCfgs 返回滚动窗口的两种等价配置：缺省（slide=0）与显式 slide==size。
// 二者都必须与滑动窗口引入前的老行为逐输出一致。
func tumblingCompatCfgs() []engine.Config {
	base := engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      1000,
		OutOfOrderMillis:      0,        // 不设乱序容忍：WM = maxSeen
		AllowedLatenessMillis: 100000,   // 宽限给足：迟到事件只修正、不丢弃
	}
	slide0 := base            // SlideMillis 缺省：滚动窗口
	slideEq := base
	slideEq.SlideMillis = 1000 // 步长 == 窗口大小：退化为滚动窗口
	return []engine.Config{slide0, slideEq}
}

// TestTumblingDegenerateFireBeforeUpdate 覆盖"水位线已越过窗口右端后，较早事件才补进来"
// 的场景：窗口大小 1000、不设乱序容忍、宽限给足，依次提交事件时间 1300、400、700。
//
// 老行为（必须逐字节保持）：
//   - 1300：WM=1300，进入 [1000,2000)，无输出；
//   - 400：进入 [0,1000)，该窗口已到点（WM=1300>=1000），【当次提交】首次关闭
//     FIRE v0（sum=400 count=1），绝不拖到下一次提交；
//   - 700：修正已关闭的 [0,1000) -> UPDATE v1（sum=1100 count=2）。
//
// 即同一窗口的首次关闭（v0）永远排在它的任何修正（v1..n）之前。
func TestTumblingDegenerateFireBeforeUpdate(t *testing.T) {
	evs := []event.Event{
		{ID: "e1", Key: "k", Value: 1300, EventTime: 1300},
		{ID: "e2", Key: "k", Value: 400, EventTime: 400}, // 水位线(1300)已越过 [0,1000) 右端才补进来
		{ID: "e3", Key: "k", Value: 700, EventTime: 700},
	}
	for i, c := range tumblingCompatCfgs() {
		sink := endpoint.NewSink()
		e := engine.New(c, sink, engine.WithLogger(slidingLogger{t}))

		mustSubmit(t, e, evs[0]) // 1300
		if n := len(sink.Results()); n != 0 {
			t.Fatalf("cfg[%d] 提交 1300 后不应有输出, got %d", i, n)
		}
		mustSubmit(t, e, evs[1]) // 400：[0,1000) 当次提交首次关闭
		rs := sink.Results()
		if len(rs) != 1 {
			t.Fatalf("cfg[%d] 提交 400 后 [0,1000) 应当次提交 FIRE, got %d 条输出", i, len(rs))
		}
		fire := rs[0]
		if fire.Kind != window.Fire || fire.Version != 0 ||
			fire.WindowStart != 0 || fire.WindowEnd != 1000 ||
			fire.Sum != 400 || fire.Count != 1 {
			t.Fatalf("cfg[%d] 首次关闭应为 FIRE v0 [0,1000) sum=400 count=1, got %+v", i, fire)
		}
		t.Logf("cfg[%d] [0,1000) 在事件 400 到达的当次提交首次关闭: FIRE v0 sum=%v count=%d",
			i, fire.Sum, fire.Count)

		mustSubmit(t, e, evs[2]) // 700：修正已关闭窗口 -> UPDATE v1
		rs = sink.Results()
		if len(rs) != 2 {
			t.Fatalf("cfg[%d] 提交 700 后应新增一条 UPDATE, got 共 %d 条输出", i, len(rs))
		}
		upd := rs[1]
		if upd.Kind != window.Update || upd.Version != 1 ||
			upd.WindowStart != 0 || upd.Sum != 1100 || upd.Count != 2 {
			t.Fatalf("cfg[%d] 迟到修正应为 UPDATE v1 [0,1000) sum=1100 count=2, got %+v", i, upd)
		}
		// 核心次序保证：同一窗口 v0(FIRE) 严格先于 v1(UPDATE) 出现在原始输出流中。
		if rs[0].Version >= rs[1].Version {
			t.Fatalf("cfg[%d] 版本次序倒置: 先看到 v%d 再看到 v%d", i, rs[0].Version, rs[1].Version)
		}
		e.Close()
		if n := len(sink.Results()); n != 3 { // + [1000,2000) 停机冲刷
			t.Fatalf("cfg[%d] Close 后应共 3 条输出, got %d", i, n)
		}
	}

	// 两种滚动等价配置的原始输出序列必须逐字节一致。
	raws := make([]string, 2)
	for i, c := range tumblingCompatCfgs() {
		sink := endpoint.NewSink()
		e := engine.New(c, sink)
		for _, ev := range evs {
			mustSubmit(t, e, ev)
		}
		e.Close()
		b, _ := json.Marshal(sink.Results())
		raws[i] = string(b)
	}
	if raws[0] != raws[1] {
		t.Fatalf("slide=0 与 slide==size 原始输出不一致:\n%s\n%s", raws[0], raws[1])
	}
	t.Logf("slide=0 与 slide==size 原始输出逐字节一致: %s", raws[0])
}

// assertVersionsMonotonic 扫描原始输出流，断言每个 (key,windowStart) 的
// Version 严格按 0,1,2,... 依次出现，且 v0 必为 FIRE、其余必为 UPDATE——
// 即任何窗口的首次关闭永远排在它的修正之前。
func assertVersionsMonotonic(t *testing.T, rs []window.Result) {
	t.Helper()
	type kw struct {
		key   string
		start int64
	}
	next := map[kw]int64{}
	for i, r := range rs {
		id := kw{r.Key, r.WindowStart}
		want := next[id]
		if r.Version != want {
			t.Fatalf("第 %d 条输出 %+v：窗口 %s@[%d,%d) 期望 version=%d，实际 version=%d（首次关闭必须先于修正）",
				i, r, r.Key, r.WindowStart, r.WindowEnd, want, r.Version)
		}
		if r.Version == 0 && r.Kind != window.Fire {
			t.Fatalf("第 %d 条输出 %+v：version=0 必须是 FIRE", i, r)
		}
		if r.Version > 0 && r.Kind != window.Update {
			t.Fatalf("第 %d 条输出 %+v：version>0 必须是 UPDATE", i, r)
		}
		next[id] = want + 1
	}
}

// TestSlidingFirstClosePrecedesUpdateAndSameSubmitFire 滑动窗口下的次序与及时性保证：
//  1. 一次提交同时让多个窗口到点 -> 当次提交内按 (start,key) 稳定顺序全部 FIRE，
//     不拖到下一次提交（事后 Drain 必须为空）；
//  2. 水位线越过窗口右端后才补进来的较早事件：若窗口尚不存在，则计入后【当次提交】
//     首次关闭（FIRE v0 含该事件）；若窗口已关闭，则只产生更高版本的 UPDATE；
//  3. 任意交错下，同一窗口 FIRE(v0) 永远先于其 UPDATE(v1..n)；
//  4. 同批输入重复运行，原始输出序列逐条一致。
func TestSlidingFirstClosePrecedesUpdateAndSameSubmitFire(t *testing.T) {
	cfg := engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      5000,
		SlideMillis:           1000,
		OutOfOrderMillis:      0,
		AllowedLatenessMillis: 100000, // 宽限给足：迟到只修正、不丢弃
	}
	evs := []event.Event{
		{ID: "m1", Key: "k", Value: 10, EventTime: 2000}, // 窗口 [0,5000) [1000,6000) [2000,7000)
		{ID: "m2", Key: "k", Value: 100, EventTime: 9000}, // WM=9000：[0..2000] 三个窗口当次提交全部到点
		{ID: "m3", Key: "k", Value: 5, EventTime: 2500},   // 水位线越过后补进：修正三个已关闭窗口 -> UPDATE v1 x3
		{ID: "m4", Key: "k", Value: 7, EventTime: 4000},   // 补进更早事件：命中已关闭窗口(UPDATE) + 新建即到点窗口(当次 FIRE)
	}

	run := func() []window.Result {
		sink := endpoint.NewSink()
		e := engine.New(cfg, sink, engine.WithLogger(slidingLogger{t}))
		for _, ev := range evs {
			mustSubmit(t, e, ev)
		}
		e.Close()
		return sink.Results()
	}

	// —— 分阶段断言（用独立引擎逐步观察） ——
	sink := endpoint.NewSink()
	e := engine.New(cfg, sink, engine.WithLogger(slidingLogger{t}))
	mustSubmit(t, e, evs[0]) // m1@2000
	if n := len(sink.Results()); n != 0 {
		t.Fatalf("m1 后不应有输出, got %d", n)
	}

	mustSubmit(t, e, evs[1]) // m2@9000：一次提交让 [0,5000)[1000,6000)[2000,7000) 同时到点
	rs := sink.Results()
	if len(rs) != 3 {
		t.Fatalf("m2 应当次提交一次性 FIRE 3 个到点窗口, got %d: %+v", len(rs), rs)
	}
	for i, r := range rs {
		if r.Kind != window.Fire || r.Version != 0 || r.WindowStart != int64(i)*1000 || r.Sum != 10 || r.Count != 1 {
			t.Fatalf("第 %d 条应为 FIRE v0 [%d,...) sum=10 count=1, got %+v", i, i*1000, r)
		}
	}
	t.Logf("m2@9000 一次提交到点 3 窗口，按 (start,key) 顺序全部 FIRE: [%d,%d,%d]",
		rs[0].WindowStart, rs[1].WindowStart, rs[2].WindowStart)
	// 到点窗口不拖到下一次提交：Drain 必须为空。
	e.Drain()
	if n := len(sink.Results()); n != 3 {
		t.Fatalf("Drain 不应再产生输出（到点窗口已在当次提交全部关闭）, got 新增 %d", n-3)
	}

	mustSubmit(t, e, evs[2]) // m3@2500：修正三个已关闭窗口
	rs = sink.Results()
	if len(rs) != 6 {
		t.Fatalf("m3 应产生 3 条 UPDATE, got 共 %d 条", len(rs))
	}
	for i, r := range rs[3:] {
		if r.Kind != window.Update || r.Version != 1 || r.WindowStart != int64(i)*1000 || r.Sum != 15 || r.Count != 2 {
			t.Fatalf("m3 第 %d 条修正应为 UPDATE v1 [%d,...) sum=15 count=2, got %+v", i, i*1000, r)
		}
	}

	// m4@4000：命中 [0,5000)[1000,6000)[2000,7000)（已关闭 -> UPDATE v2），
	// 并新建 [3000,8000) 与 [4000,9000)（WM=9000 均已到点 -> 当次提交各 FIRE v0，含 m4 自己）。
	mustSubmit(t, e, evs[3])
	rs = sink.Results()
	if len(rs) != 11 {
		t.Fatalf("m4 应产生 3 条 UPDATE + 2 条新窗口 FIRE, got 共 %d 条: %+v", len(rs), rs)
	}
	for _, r := range rs[6:9] {
		if r.Kind != window.Update || r.Version != 2 || r.Sum != 22 || r.Count != 3 {
			t.Fatalf("m4 对已关闭窗口应为 UPDATE v2 sum=22 count=3, got %+v", r)
		}
	}
	for i, r := range rs[9:] {
		wantStart := int64(3000 + i*1000)
		if r.Kind != window.Fire || r.Version != 0 ||
			r.WindowStart != wantStart || r.WindowEnd != wantStart+5000 ||
			r.Sum != 7 || r.Count != 1 {
			t.Fatalf("m4 新建即到点的 [%d,%d) 应当次提交 FIRE v0 sum=7 count=1, got %+v",
				wantStart, wantStart+5000, r)
		}
	}
	t.Logf("m4@4000：先修正已关闭窗口(UPDATE v2 x3)，新建即到点的 [3000,8000) [4000,9000) 当次提交各 FIRE v0")
	e.Close()

	// 全流不变量：每个窗口 v0(FIRE) 严格先于 v1..n(UPDATE)。
	assertVersionsMonotonic(t, sink.Results())

	// 同批输入重复运行，原始输出序列逐条一致。
	a, b := run(), run()
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatalf("两次运行原始输出序列不一致:\n%s\n%s", ja, jb)
	}
	assertVersionsMonotonic(t, a)
	t.Logf("两次运行 %d 条原始输出逐条一致，且全程满足 FIRE(v0) 先于 UPDATE(v1..n)", len(a))
}

// TestSlidingMaxKeysOnlyBlocksNewKeys 滑动窗口下 MaxKeys 的精确语义：
//   - MaxKeys 只挡"真正的新 key"：已活跃 key 继续开新的重叠窗口不受 MaxKeys 影响
//     （同 key 的窗口条目总量由 MaxWindows 兜底，本用例不设 MaxWindows）；
//   - 第二个 key 的事件命中 5 个重叠窗口时，5 个窗口必须全部进入，一个都不能被误挡；
//   - 第三个（超限的）新 key 才被按容量拒绝，且只拒绝它自己进不去的窗口：
//     每个被拒窗口各记一条 MEMORY_LIMIT_REJECTED（带窗口坐标），与迟到/重复原因互斥；
//   - 被拒事件不污染其他 key 的窗口结果。
func TestSlidingMaxKeysOnlyBlocksNewKeys(t *testing.T) {
	c := engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      5000,
		SlideMillis:           1000,
		OutOfOrderMillis:      0,
		AllowedLatenessMillis: 0,
		MaxKeys:               2,
	}
	sink := endpoint.NewSink()
	e := engine.New(c, sink, engine.WithLogger(slidingLogger{t}))

	mustSubmit(t, e, event.Event{ID: "a1", Key: "ka", Value: 1, EventTime: 100})
	// 第二个 key 的事件命中覆盖它的全部 5 个重叠窗口：一个都不许被 MaxKeys 误挡。
	mustSubmit(t, e, event.Event{ID: "b1", Key: "kb", Value: 2, EventTime: 4000})
	if n := e.Stats().DroppedMemory; n != 0 {
		t.Fatalf("已活跃 key 开重叠窗口不应触发容量拒绝, DroppedMemory=%d", n)
	}
	if n := e.Stats().ActiveWindows; n != 6 { // ka@[0,5000) + kb 的 5 个窗口
		t.Fatalf("kb 应进入全部 5 个重叠窗口（共 6 个活跃条目）, got %d", n)
	}
	t.Logf("MaxKeys=2 下 kb@4000 成功进入覆盖它的全部 5 个重叠窗口（活跃条目=%d）", e.Stats().ActiveWindows)

	// 第三个 key 是真正的新 key 且超限：它命中的每个窗口各记一条容量拒绝。
	mustSubmit(t, e, event.Event{ID: "c1", Key: "kc", Value: 3, EventTime: 4100})
	if n := e.Stats().DroppedMemory; n != 5 {
		t.Fatalf("超限新 key 的 5 个目标窗口应各记一次容量拒绝, DroppedMemory=%d want 5", n)
	}
	for _, d := range sink.Drops() {
		if d.Ev.ID != "c1" {
			t.Fatalf("此时不应有其他事件的丢弃记录: %+v", d)
		}
		if d.Reason != engine.DropMemoryLimit {
			t.Fatalf("容量拒绝原因必须是 %s（不得混成迟到/重复）, got %s", engine.DropMemoryLimit, d.Reason)
		}
		t.Logf("DROP id=%s key=%s window=[%d,%d) reason=%s", d.Ev.ID, d.Ev.Key, d.WindowStart, d.WindowEnd, d.Reason)
	}
	if n := e.Stats().DroppedLate + e.Stats().DroppedDup; n != 0 {
		t.Fatalf("容量拒绝不得与迟到/重复混淆, late+dup=%d", n)
	}

	// 已活跃 key 继续到达（含新开重叠窗口）依然不受 MaxKeys 影响。
	mustSubmit(t, e, event.Event{ID: "b2", Key: "kb", Value: 4, EventTime: 4500})
	if n := e.Stats().DroppedMemory; n != 5 {
		t.Fatalf("已活跃 key 的后续事件不应再被拒, DroppedMemory=%d want 仍为 5", n)
	}

	// 推进水位线关闭全部窗口；被拒的 kc 不得产生任何窗口结果，ka/kb 结果不被带偏。
	mustSubmit(t, e, event.Event{ID: "adv", Key: "ka", Value: 0, EventTime: 10000})
	e.Close()
	for _, r := range sink.FinalResults() {
		if r.Key == "kc" {
			t.Fatalf("被容量拒绝的 kc 不得产生任何窗口结果: %+v", r)
		}
	}
	if r := finalByStart(t, sink, "ka", 0); r.Sum != 1 || r.Count != 1 { // 仅 a1（adv@10000 不在该窗口）
		t.Fatalf("ka@[0,5000) 不应被被拒事件带偏: sum=%v count=%d", r.Sum, r.Count)
	}
	// kb 的 5 个窗口各自精确：b1(2) 命中全部 5 个；b2(4) 命中 [0,5000)..[4000,9000) 全部 5 个。
	for start := int64(0); start <= 4000; start += 1000 {
		r := finalByStart(t, sink, "kb", start)
		if r.Sum != 6 || r.Count != 2 {
			t.Fatalf("kb@[%d,%d) 应为 b1+b2=6/2, got sum=%v count=%d", start, start+5000, r.Sum, r.Count)
		}
	}
	t.Logf("kc 被容量拒绝（5 条 MEMORY_LIMIT_REJECTED），ka/kb 各窗口结果精确未受影响")
}
