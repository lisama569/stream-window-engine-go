package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/engine"
	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// slidingCfg 每分钟往前看 5 分钟：size=5000, slide=1000。
// 关闭乱序容忍（WM=maxSeen）让窗口到点顺序可直接按事件时间推导；宽限=0 使窗口
// Fire 后立即可回收，便于断言状态释放。
func slidingCfg() engine.Config {
	return engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      5000,
		SlideMillis:           1000,
		OutOfOrderMillis:      0,
		AllowedLatenessMillis: 0,
		MaxKeys:               0,
		MaxWindows:            0,
	}
}

func mustSubmit(t *testing.T, e *engine.Engine, ev event.Event) {
	t.Helper()
	if err := e.Submit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
}

// finalByStart 从最终折叠结果里取某 key 在某窗口起点的结果。
func finalByStart(t *testing.T, sink *endpoint.Sink, key string, start int64) window.Result {
	t.Helper()
	for _, r := range sink.FinalResults() {
		if r.Key == key && r.WindowStart == start {
			return r
		}
	}
	t.Fatalf("缺少最终窗口 %s@[%d,...)", key, start)
	return window.Result{}
}

// logDrops 打印全部丢弃记录（含窗口坐标与原因）。
func logDrops(t *testing.T, sink *endpoint.Sink) {
	for _, d := range sink.Drops() {
		t.Logf("DROP id=%s key=%s window=[%d,%d) reason=%s", d.Ev.ID, d.Ev.Key, d.WindowStart, d.WindowEnd, d.Reason)
	}
}

// TestSlidingOverlappingAggregation 同一事件同时进入覆盖它的多个窗口，
// 每个窗口各自独立聚合；窗口大小/步长按配置生效。
func TestSlidingOverlappingAggregation(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(slidingCfg(), sink, engine.WithLogger(slidingLogger{t}))

	// 事件时间 2000 命中窗口 [0,5000) [1000,6000) [2000,7000)
	mustSubmit(t, e, event.Event{ID: "e1", Key: "k", Value: 10, EventTime: 2000})
	// 事件时间 4500 命中 [0,5000) [1000,6000) [2000,7000) [3000,8000) [4000,9000)
	mustSubmit(t, e, event.Event{ID: "e2", Key: "k", Value: 5, EventTime: 4500})

	fin := sink.FinalResults()
	t.Logf("未关闭前最终视图条目数=%d", len(fin))
	// 此时没有窗口到点（WM=4500，最早窗口右端=5000），不应有任何输出
	if len(sink.Results()) != 0 {
		t.Fatalf("窗口未到水位线不得提前输出, got %d", len(sink.Results()))
	}

	// 推进 WM 到 5000：[0,5000) 到点 Fire（只此一个），其余仍打开
	mustSubmit(t, e, event.Event{ID: "e3", Key: "k", Value: 100, EventTime: 5000})
	rs := sink.Results()
	if len(rs) != 1 || rs[0].Kind != window.Fire || rs[0].WindowStart != 0 {
		t.Fatalf("WM=5000 应只 Fire [0,5000), got %+v", rs)
	}
	if rs[0].Count != 2 || rs[0].Sum != 15 {
		t.Fatalf("[0,5000) = sum=%v count=%d, want 15/2 (e1+e2)", rs[0].Sum, rs[0].Count)
	}
	t.Logf("[0,5000) FIRE sum=%v count=%d（e1、e2 均覆盖）", rs[0].Sum, rs[0].Count)

	// 推进到 9000，关闭所有剩余窗口，核对每个窗口各自的归属
	mustSubmit(t, e, event.Event{ID: "e4", Key: "k", Value: 1, EventTime: 9000})
	e.Close() // 未到点窗口由停机冲刷一次性输出
	// 各事件覆盖：e1(2000)->0,1,2；e2(4500)->0..4；e3(5000)->1..5；e4(9000)->5..9。
	expect := []struct {
		start int64
		sum   float64
		count int64
	}{
		{0, 15, 2},     // e1,e2
		{1000, 115, 3}, // e1,e2,e3
		{2000, 115, 3}, // e1,e2,e3
		{3000, 105, 2}, // e2,e3
		{4000, 105, 2}, // e2,e3
		{5000, 101, 2}, // e3,e4
		{6000, 1, 1},   // 仅 e4
		{7000, 1, 1},
		{8000, 1, 1},
		{9000, 1, 1},
	}
	for _, w := range expect {
		r := finalByStart(t, sink, "k", w.start)
		if r.Sum != w.sum || r.Count != w.count {
			t.Fatalf("窗口 [%d,%d) = sum=%v count=%d, want sum=%v count=%d",
				w.start, w.start+5000, r.Sum, r.Count, w.sum, w.count)
		}
		t.Logf("窗口 [%d,%d) sum=%v count=%d OK", w.start, w.start+5000, r.Sum, r.Count)
	}
}

// TestSlidingBoundaryEvents 窗口交界处（t 恰为某窗口右端）遵循左闭右开：
// t 不属于 [t-size, t)，只属于起点包含 t 的窗口。
func TestSlidingBoundaryEvents(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(slidingCfg(), sink)
	defer e.Close()

	// t=5000 恰好是 [0,5000) 的右端（不含），命中 1000..5000 五个窗口
	mustSubmit(t, e, event.Event{ID: "b1", Key: "k", Value: 1, EventTime: 5000})
	// 推进 WM 到 11000：end<=11000 且被 b1 命中的窗口全部按水位线正常 FIRE
	mustSubmit(t, e, event.Event{ID: "b2", Key: "k", Value: 1, EventTime: 11000})

	// [0,5000) 没有任何事件，根本不产生聚合结果；b1 计入 1000..5000 五个窗口
	if r, ok := lookupFinal(sink, "k", 0); ok {
		t.Fatalf("边界事件 t=5000 不得计入 [0,5000), got %+v", r)
	}
	for _, start := range []int64{1000, 2000, 3000, 4000, 5000} {
		r := finalByStart(t, sink, "k", start)
		if r.Count != 1 || r.Sum != 1 {
			t.Fatalf("边界事件应计入 [%d,%d), got sum=%v count=%d", start, start+5000, r.Sum, r.Count)
		}
	}
	t.Log("左闭右开边界：t=5000 不入 [0,5000)，进入 [1000,6000)..[5000,10000)")
}

func lookupFinal(sink *endpoint.Sink, key string, start int64) (window.Result, bool) {
	for _, r := range sink.FinalResults() {
		if r.Key == key && r.WindowStart == start {
			return r, true
		}
	}
	return window.Result{}, false
}

// TestSlidingMultiWindowFireOrder 一次提交同时让多个窗口到点时，输出按
// (windowStart, key) 确定排序；同批输入多次运行输出序列一致。
func TestSlidingMultiWindowFireOrder(t *testing.T) {
	run := func() []window.Result {
		sink := endpoint.NewSink()
		e := engine.New(slidingCfg(), sink)
		// 两个 key 交错，先把各窗口种入数据
		mustSubmit(t, e, event.Event{ID: "p1", Key: "p", Value: 1, EventTime: 0})
		mustSubmit(t, e, event.Event{ID: "q1", Key: "q", Value: 2, EventTime: 1000})
		mustSubmit(t, e, event.Event{ID: "p2", Key: "p", Value: 1, EventTime: 2000})
		mustSubmit(t, e, event.Event{ID: "q2", Key: "q", Value: 2, EventTime: 3000})
		// t=9000：WM=9000，end<=9000 的全部窗口一次性到点（[0,5000)..[8000,13000) 中
		// 有数据的窗口），同一次提交的 FIRE 顺序必须 (start, key) 确定。
		mustSubmit(t, e, event.Event{ID: "adv", Key: "p", Value: 0, EventTime: 9000})
		e.Close()
		return sink.Results()
	}

	a := run()
	// 取 adv 之前已有的输出之前，先找到 t=9000 这一批：此前 WM=3000，[0,5000) 未到点，
	// 因此所有 FIRE 都在最后一次提交产生。
	var batch []window.Result
	for _, r := range a {
		if r.WindowStart <= 4000 && r.WindowEnd <= 9000 {
			batch = append(batch, r)
		}
	}
	if len(batch) == 0 {
		t.Fatalf("预期多窗口同时到点，实际输出 %+v", a)
	}
	for i := 1; i < len(batch); i++ {
		prev, cur := batch[i-1], batch[i]
		if prev.WindowStart > cur.WindowStart ||
			(prev.WindowStart == cur.WindowStart && prev.Key > cur.Key) {
			t.Fatalf("到点输出未按 (start,key) 排序: [%s@%d] 在 [%s@%d] 前",
				prev.Key, prev.WindowStart, cur.Key, cur.WindowStart)
		}
	}
	for _, r := range batch {
		t.Logf("BATCH FIRE key=%s window=[%d,%d) sum=%v count=%d", r.Key, r.WindowStart, r.WindowEnd, r.Sum, r.Count)
	}

	// 同批输入再跑一次，原始输出序列必须逐条一致（不只是折叠后一致）。
	b := run()
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatalf("两次运行输出序列不一致:\n%s\n%s", ja, jb)
	}
	t.Logf("同批输入两次运行，%d 条输出序列逐条一致", len(a))
}

// TestSlidingLateUpdateAndDedupPerWindow 重叠窗口下：
//   - 同一事件 ID 在【每个】窗口各自去重（一次重放对命中的每个窗口都是 DUPLICATE，
//     但每个窗口只算一次）；
//   - 宽限期内迟到事件只修正真正受影响（已 Fire 且仍在宽限）的窗口，输出各自的
//     UPDATE，未关闭窗口静默计入、无 UPDATE；
//   - 超过宽限期的迟到事件只对真正过期的窗口记 LATE_BEYOND_TOLERANCE，其他窗口不受影响；
//   - 一条坏事件不会带偏其他窗口的结果。
func TestSlidingLateUpdateAndDedupPerWindow(t *testing.T) {
	c := slidingCfg()
	c.AllowedLatenessMillis = 3000 // Fire 后保留 3s 宽限
	sink := endpoint.NewSink()
	e := engine.New(c, sink)

	// e1@2000 命中 [0,5000)[1000,6000)[2000,7000)，各计 10
	mustSubmit(t, e, event.Event{ID: "e1", Key: "k", Value: 10, EventTime: 2000})
	// adv@5500：WM=5500 -> 仅 end<=5500 的 [0,5000) Fire
	mustSubmit(t, e, event.Event{ID: "adv", Key: "k", Value: 1, EventTime: 5500})
	if n := len(sink.Results()); n != 1 || sink.Results()[0].WindowStart != 0 {
		t.Fatalf("仅 [0,5000) 应 Fire, got %+v", sink.Results())
	}

	// 重放 e1：它命中的三个窗口都已含该 ID -> 每条 DUPLICATE，计数不变、无输出
	before := len(sink.Results())
	mustSubmit(t, e, event.Event{ID: "e1", Key: "k", Value: 10, EventTime: 2000})
	if got := e.Stats().DroppedDup; got != 3 {
		t.Fatalf("重放 e1 应在 3 个窗口各记一次重复, DroppedDup=%d want 3", got)
	}
	if len(sink.Results()) != before {
		t.Fatalf("重复事件不得产生窗口输出, got 新增 %d", len(sink.Results())-before)
	}
	r0 := finalByStart(t, sink, "k", 0)
	if r0.Sum != 10 || r0.Count != 1 {
		t.Fatalf("重复重放后 [0,5000) 被带偏: sum=%v count=%d", r0.Sum, r0.Count)
	}
	logDrops(t, sink)

	// 新的迟到事件 late@2500（全新 ID），命中 [0..2000] 三个窗口：
	// [0,5000) 已 Fire 且 5500 <= 5000+3000=8000 在宽限内 -> UPDATE v1 sum=15；
	// [1000,6000)[2000,7000) 未 Fire -> 静默计入，无 UPDATE。
	mustSubmit(t, e, event.Event{ID: "late", Key: "k", Value: 5, EventTime: 2500})
	rs := sink.Results()
	last := rs[len(rs)-1]
	if last.Kind != window.Update || last.WindowStart != 0 || last.Version != 1 || last.Sum != 15 || last.Count != 2 {
		t.Fatalf("迟到修正应为 [0,5000) UPDATE v1 sum=15/2, got %+v", last)
	}
	for _, st := range []int64{1000, 2000} {
		r, ok := lookupFinal(sink, "k", st)
		if ok {
			t.Fatalf("未 Fire 窗口 [%d,%d) 不应有任何输出, got %+v", st, st+5000, r)
		}
	}
	t.Logf("迟到 late@2500 只修正已 Fire 的 [0,5000) -> UPDATE sum=%v；其余窗口静默计入", last.Sum)

	// 推进 WM=8500：[1000,6000)[2000,7000) 到点 Fire；
	// [0,5000) 宽限至 8000 -> 已过期清理；[1000,6000) 宽限至 9000（含边界，保留）。
	mustSubmit(t, e, event.Event{ID: "adv2", Key: "k", Value: 1, EventTime: 8500})
	purged := e.Stats().PurgedWindows
	if purged < 1 {
		t.Fatalf("过宽限期窗口应被清理, purged=%d", purged)
	}
	// 再来一个 t=2000 的新 ID 事件：[0,5000) 已过期 -> LATE_BEYOND_TOLERANCE；
	// 但它同时命中 [1000,6000)（宽限至 9000 含边界，仍存活）-> 正常修正并 UPDATE；
	// [2000,7000) 刚 Fire 且宽限至 10000，同样被修正 -> 另一窗口各自独立 UPDATE。
	mustSubmit(t, e, event.Event{ID: "late2", Key: "k", Value: 7, EventTime: 2000})
	var lateDrop, memDrop, dupDrop int
	var lateWindow int64 = -1
	for _, d := range sink.Drops() {
		switch d.Reason {
		case engine.DropLate:
			lateDrop++
			lateWindow = d.WindowStart
		case engine.DropMemoryLimit:
			memDrop++
		case window.ReasonDuplicate:
			dupDrop++
		}
	}
	if lateDrop != 1 || lateWindow != 0 {
		t.Fatalf("late2 应仅对 [0,5000) 记一次迟到丢弃, lateDrop=%d window=%d", lateDrop, lateWindow)
	}
	// [1000,6000) 在 adv2 时 Fire（WM=8500>=6000），late2 在宽限边界内修正：
	// 两个窗口都覆盖 e1(2000)/late(2500)/adv(5500)/late2(2000)：
	// [1000,6000) 另含 adv2@8500？否（8500>=6000 右端）。两个窗口组成相同：
	// e1(10)+late(5)+adv(1)+late2(7)=23/4，adv2@8500 两个窗口都不覆盖。
	r1 := finalByStart(t, sink, "k", 1000)
	if r1.Sum != 23 || r1.Count != 4 {
		t.Fatalf("[1000,6000) 应为 e1+late+adv+late2=23/4, got sum=%v count=%d v=%d", r1.Sum, r1.Count, r1.Version)
	}
	r2 := finalByStart(t, sink, "k", 2000)
	if r2.Sum != 23 || r2.Count != 4 {
		t.Fatalf("[2000,7000) 应为 e1+late+adv+late2=23/4, got sum=%v count=%d v=%d", r2.Sum, r2.Count, r2.Version)
	}
	e.Close()

	// 终态核对：坏/迟到/重复事件没有污染任何无关窗口
	r0Final := finalByStart(t, sink, "k", 0)
	if r0Final.Sum != 15 || r0Final.Count != 2 {
		t.Fatalf("[0,5000) 终态应为 e1+late=15/2, got %v/%d", r0Final.Sum, r0Final.Count)
	}
	t.Logf("终态 [0,5000)=%v/%d [1000,6000)=%v/%d 重复=%d 迟到=%d 容量拒绝=%d",
		r0Final.Sum, r0Final.Count, r1.Sum, r1.Count, dupDrop, lateDrop, memDrop)
}

// TestSlidingCapacityAndStateReclaim 滑动窗口重叠状态下：
//  1. 持续单调输入时活跃窗口数有界（重叠度 = ceil(size/slide)，过期即释放），不随历史无限涨；
//  2. 达到 MaxWindows 硬上限且无法回收时，事件按 MEMORY_LIMIT_REJECTED 被【可观测】拒绝，
//     而不是悄悄丢；
//  3. 水位线推进、过期窗口回收后容量恢复，后续事件照常处理；
//  4. 被容量拒绝的事件只影响它自己进不去的窗口，其他窗口/事件结果不被带偏。
//
// TestSlidingCapacityAndStateReclaim 滑动窗口重叠状态下：
//  1. 活跃窗口数达 MaxWindows 硬上限且无可回收窗口时，事件按 MEMORY_LIMIT_REJECTED
//     被可观测拒绝（带窗口坐标），而不是悄悄丢，也不拖死引擎；
//  2. 被容量拒绝的事件只影响它自己进不去的窗口，其他窗口/事件结果不被带偏；
//  3. 长时间持续单调输入下，过期窗口过宽限期即真正释放，活跃状态保持有界，
//     快照/内存不随历史无限涨；容量回收后后续事件照常处理。
func TestSlidingCapacityAndStateReclaim(t *testing.T) {
	// —— 第一部分：卡住上限，验证可观测拒绝与隔离性 ——
	{
		c := slidingCfg()
		c.MaxWindows = 4 // 稳态重叠度为 5（size/slide），用 4 卡住
		sink := endpoint.NewSink()
		e := engine.New(c, sink)

		// 单调时间流 0,1000,2000,3000：存活（未到点）窗口数为 1,2,3,4，均 <=4。
		mustSubmit(t, e, event.Event{ID: "t0", Key: "k", Value: 1, EventTime: 0})
		mustSubmit(t, e, event.Event{ID: "t1", Key: "k", Value: 1, EventTime: 1000})
		mustSubmit(t, e, event.Event{ID: "t2", Key: "k", Value: 1, EventTime: 2000})
		mustSubmit(t, e, event.Event{ID: "t3", Key: "k", Value: 1, EventTime: 3000})
		if n := e.Stats().ActiveWindows; n != 4 {
			t.Fatalf("稳态前活跃窗口=%d want 4", n)
		}
		if e.Stats().DroppedMemory != 0 {
			t.Fatalf("未到上限不应有容量拒绝, got %d", e.Stats().DroppedMemory)
		}

		// t=4000：WM=4000 无窗口到点（最早右端=5000），事件需要第 5 个窗口 -> 容量拒绝。
		mustSubmit(t, e, event.Event{ID: "t4", Key: "k", Value: 1, EventTime: 4000})
		st := e.Stats()
		if st.DroppedMemory != 1 {
			t.Fatalf("第 5 个重叠窗口应被容量拒绝一次, DroppedMemory=%d", st.DroppedMemory)
		}
		if st.ActiveWindows > c.MaxWindows {
			t.Fatalf("拒绝后活跃窗口不得突破上限: %d > %d", st.ActiveWindows, c.MaxWindows)
		}
		// 拒绝记录必须带得清是哪个窗口、什么原因，不能混成迟到/重复。
		d := sink.Drops()[len(sink.Drops())-1]
		if d.Reason != engine.DropMemoryLimit || d.WindowStart != 4000 {
			t.Fatalf("容量拒绝记录错误: %+v", d)
		}
		t.Logf("t=4000 需第 5 个重叠窗口，被 %s 挡下（window=[%d,%d)），活跃=%d 未超限",
			d.Reason, d.WindowStart, d.WindowEnd, st.ActiveWindows)

		// 引擎未被拖死：后续落在【已存在】窗口里的事件照常处理（不新建窗口、不受上限影响）。
		mustSubmit(t, e, event.Event{ID: "t3b", Key: "k", Value: 7, EventTime: 3500})
		// t4@4000 只有第 5 个新窗口 [4000,9000) 被容量挡下；它命中的其余 4 个
		// 已存在窗口（起点 0..3000）正常计入——只拒绝真正装不下的窗口，不殃及其他。
		e.Close()
		if _, ok := lookupFinal(sink, "k", 4000); ok {
			t.Fatalf("被容量拒绝的 [4000,9000) 不得产生任何聚合结果")
		}
		r := finalByStart(t, sink, "k", 3000)
		if r.Sum != 9 || r.Count != 3 { // t3(1@3000)+t4(1@4000)+t3b(7@3500)
			t.Fatalf("[3000,8000) 应含 t3+t4+t3b=9/3: sum=%v count=%d", r.Sum, r.Count)
		}
	}

	// —— 第二部分：长时间持续输入，活跃状态必须有界、过期即释放 ——
	{
		c := slidingCfg() // MaxWindows=0：靠过期回收自然有界
		sink := endpoint.NewSink()
		e := engine.New(c, sink)
		var maxActive int
		for ts := int64(0); ts <= 200000; ts += 1000 {
			mustSubmit(t, e, event.Event{
				ID: fmt.Sprintf("c%d", ts), Key: "k", Value: 1, EventTime: ts,
			})
			if n := e.Stats().ActiveWindows; n > maxActive {
				maxActive = n
			}
		}
		finalActive := e.Stats().ActiveWindows
		e.Close()
		// 稳态终态仅 end>WM 的最多 5 个窗口存活；单步提交内新窗口已建、旧窗口待清理的
		// 瞬态至多 6（不随时间增长，这才是"有界"的含义）。
		if maxActive > 6 {
			t.Fatalf("持续输入下活跃窗口无界增长: 峰值=%d > 有界上界 6", maxActive)
		}
		if finalActive > 6 {
			t.Fatalf("长流结束活跃窗口=%d 应有界<=6（历史窗口已过宽限期释放）", finalActive)
		}
		t.Logf("持续输入到 t=200000：活跃窗口峰值=%d 终值=%d（有界，历史状态已释放），purge=%d",
			maxActive, finalActive, e.Stats().PurgedWindows)
		if e.Stats().PurgedWindows == 0 {
			t.Fatal("宽限期满窗口应被持续清理，PurgedWindows 不应为 0")
		}

		// 计数守恒：每个事件进入其覆盖的窗口，201 个事件（t=0..200000 步长1000），
		// 早期事件窗口数较少（负起点被截断），其余 5 窗；总计数 = 1+2+3+4+197*5=995。
		var totalCount int64
		for _, r := range sink.FinalResults() {
			totalCount += r.Count
		}
		if totalCount != 995 {
			t.Fatalf("窗口计数守恒破坏: total=%d want 995", totalCount)
		}
	}
}

// TestSlidingMaxKeysStillEnforced 滑动窗口下 MaxKeys（不同 key 数）上限依旧独立生效，
// 与 MaxWindows 互不替代。
func TestSlidingMaxKeysStillEnforced(t *testing.T) {
	c := slidingCfg()
	c.MaxKeys = 2
	sink := endpoint.NewSink()
	e := engine.New(c, sink)
	defer e.Close()

	mustSubmit(t, e, event.Event{ID: "a", Key: "ka", Value: 1, EventTime: 100})
	mustSubmit(t, e, event.Event{ID: "b", Key: "kb", Value: 1, EventTime: 200})
	// 第三个 key 命中全新窗口集合，水位线未推进、无可回收 -> 容量拒绝。
	mustSubmit(t, e, event.Event{ID: "c", Key: "kc", Value: 1, EventTime: 300})
	// 事件命中 1~5 个窗口，每个都因 MaxKeys 拒绝（ka/kb 的窗口不允许新 key 进入）。
	if n := e.Stats().DroppedMemory; n == 0 {
		t.Fatalf("第三个 key 应被 MaxKeys 挡下, stats=%+v", e.Stats())
	}
	// ka、kb 的既有窗口结果不受被拒事件影响。
	mustSubmit(t, e, event.Event{ID: "a2", Key: "ka", Value: 3, EventTime: 150})
	mustSubmit(t, e, event.Event{ID: "end", Key: "ka", Value: 0, EventTime: 10000})
	e.Close()
	r := finalByStart(t, sink, "ka", 0)
	if r.Sum != 4 || r.Count != 2 {
		t.Fatalf("ka 的窗口不应被被拒的 kc 带偏: sum=%v count=%d", r.Sum, r.Count)
	}
	logDrops(t, sink)
}

// TestSlidingSnapshotRestoreReplay 滑动窗口中途快照：恢复后重放快照点之前的事件，
// 每个被命中窗口都识别为重复（不重不漏、不产生任何新增输出）；继续喂新事件，
// 最终每个窗口的聚合与"一口气从头跑"完全一致。
func TestSlidingSnapshotRestoreReplay(t *testing.T) {
	c := slidingCfg()
	c.ShardCount = 2 // 顺带覆盖多分片
	c.AllowedLatenessMillis = 2000

	first := []event.Event{
		{ID: "s1", Key: "k1", Value: 1, EventTime: 100},
		{ID: "s2", Key: "k1", Value: 2, EventTime: 2000},
		{ID: "s3", Key: "k2", Value: 10, EventTime: 3000},
		{ID: "s4", Key: "k1", Value: 4, EventTime: 6500},  // 推进 k1 水位线，关掉部分窗口
		{ID: "s5", Key: "k2", Value: 20, EventTime: 1200}, // 乱序，落入早期重叠窗口
	}
	second := []event.Event{
		{ID: "s6", Key: "k2", Value: 30, EventTime: 8000},
		{ID: "s7", Key: "k1", Value: 5, EventTime: 9000},
		{ID: "s8", Key: "k1", Value: 3, EventTime: 2600}, // 宽限内迟到
	}

	// 参考：一口气从头跑到 Close 的确定终态。
	reference := func() []window.Result {
		sink := endpoint.NewSink()
		e := engine.New(c, sink)
		for _, ev := range append(append([]event.Event{}, first...), second...) {
			mustSubmit(t, e, ev)
		}
		e.Close()
		return sink.FinalResults()
	}
	want := reference()

	// 中途快照。
	sink := endpoint.NewSink()
	e1 := engine.New(c, sink)
	for _, ev := range first {
		mustSubmit(t, e1, ev)
	}
	snap := e1.SaveSnapshot()
	t.Logf("快照含 %d 个窗口聚合", len(snap.Shards[0].Windows)+len(snap.Shards[1].Windows))
	preSnapshotOutputs := len(sink.Results())

	// JSON 落盘往返，模拟真实重启。
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var restored engine.Snapshot
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}

	e2 := engine.New(c, sink) // 同一汇聚出口（模拟重启后继续向下游同一出口写）
	if err := e2.Restore(&restored); err != nil {
		t.Fatal(err)
	}

	// 重放快照点之前的全部事件：只允许幂等的重复观测，不产生新增输出。
	for _, ev := range first {
		mustSubmit(t, e2, ev)
	}
	if got := len(sink.Results()); got != preSnapshotOutputs {
		t.Fatalf("重放已消费数据产生 %d 条新增输出（应 0）", got-preSnapshotOutputs)
	}
	// 每条重放事件对它命中的每个窗口都应记一次重复。
	wantDup := 0
	for _, ev := range first {
		wantDup += len(specStartsCount(c.WindowSizeMillis, c.SlideMillis, ev.EventTime))
	}
	if got := e2.Stats().DroppedDup; int(got) != wantDup {
		t.Fatalf("重放重复计数=%d want %d（按窗口去重）", got, wantDup)
	}

	// 快照点之后的新事件正常处理，含宽限内迟到修正。
	for _, ev := range second {
		mustSubmit(t, e2, ev)
	}
	e2.Close()

	got := sink.FinalResults()
	j1, _ := json.Marshal(want)
	j2, _ := json.Marshal(got)
	if string(j1) != string(j2) {
		t.Fatalf("快照恢复路径终态与参考不一致:\\nwant %s\\ngot  %s", j1, j2)
	}
	t.Logf("滑动窗口快照恢复+重放：%d 窗口终态与一口气跑完全一致，重复重放零新增输出", len(want))
}

// TestSlidingSnapshotDeterministicBytes 同一滑动状态连续两次快照字节一致
// （含每个窗口独立的去重 ID 集合的顺序稳定性）。
func TestSlidingSnapshotDeterministicBytes(t *testing.T) {
	c := slidingCfg()
	sink := endpoint.NewSink()
	e := engine.New(c, sink)
	for _, ev := range []event.Event{
		{ID: "z1", Key: "z", Value: 1, EventTime: 100},
		{ID: "m1", Key: "m", Value: 2, EventTime: 200},
		{ID: "a1", Key: "a", Value: 3, EventTime: 300},
		{ID: "z2", Key: "z", Value: 4, EventTime: 6600},
	} {
		mustSubmit(t, e, ev)
	}
	b1 := fmt.Sprintf("%+v", e.SaveSnapshot())
	b2 := fmt.Sprintf("%+v", e.SaveSnapshot())
	if b1 != b2 {
		t.Fatal("同一滑动状态的两次快照不一致（不确定）")
	}
	e.Close()
}

// TestSlidingRestoreRejectsDifferentSpec 快照不能恢复进窗口划分不同的引擎。
func TestSlidingRestoreRejectsDifferentSpec(t *testing.T) {
	c := slidingCfg()
	sink := endpoint.NewSink()
	e := engine.New(c, sink)
	mustSubmit(t, e, event.Event{ID: "x", Key: "k", Value: 1, EventTime: 100})
	snap := e.SaveSnapshot()
	e.Close()

	other := c
	other.SlideMillis = 2000
	e2 := engine.New(other, endpoint.NewSink())
	if err := e2.Restore(snap); err == nil {
		t.Fatal("用不同步长恢复滑动快照应报错拒绝")
	}
	other = c
	other.WindowSizeMillis = 9999
	other.SlideMillis = 1000
	e3 := engine.New(other, endpoint.NewSink())
	if err := e3.Restore(snap); err == nil {
		t.Fatal("用不同窗口大小恢复快照应报错拒绝")
	}
	t.Log("窗口划分不一致的恢复被显式拒绝，避免窗口错位")
}

// specStartsCount 复制 window.Spec.Starts 的划分规则用于断言（external 测试包无法访问内部）。
func specStartsCount(size, slide, t int64) []int64 {
	if slide <= 0 {
		slide = size
	}
	mod := func(a, m int64) int64 {
		r := a % m
		if r < 0 {
			r += m
		}
		return r
	}
	latest := t - mod(t, slide)
	earliest := t - size - mod(t-size, slide) + slide
	var out []int64
	for st := earliest; st <= latest; st += slide {
		if st >= 0 {
			out = append(out, st)
		}
	}
	return out
}

// TestSlidingDegeneratesToTumbling 步长配成与窗口大小一致时，行为必须与滚动窗口
// 逐输出等价（老行为一点不变）：同一批乱序/迟到/重复输入，两种配置的原始输出序列
// （含 Kind/Version/窗口坐标/聚合值）与丢弃记录完全一致。
func TestSlidingDegeneratesToTumbling(t *testing.T) {
	evs := []event.Event{
		{ID: "d1", Key: "k", Value: 3, EventTime: 2900},
		{ID: "d2", Key: "k", Value: 1, EventTime: 100},
		{ID: "d3", Key: "k", Value: 4, EventTime: 3100},
		{ID: "d4", Key: "k", Value: 2, EventTime: 1500},
		{ID: "d5", Key: "k", Value: 5, EventTime: 200},
		{ID: "d2", Key: "k", Value: 1, EventTime: 100},  // 重复
		{ID: "d6", Key: "q", Value: 8, EventTime: 7000}, // 推进 WM 触发清理+迟到丢弃
		{ID: "d7", Key: "k", Value: 6, EventTime: 50},   // 超宽限迟到
	}
	run := func(slide int64) (string, string, engine.Stats) {
		c := engine.Config{
			ShardCount:            3,
			WindowSizeMillis:      1000,
			SlideMillis:           slide, // 1000=显式滑动退化；0=缺省滚动
			OutOfOrderMillis:      200,
			AllowedLatenessMillis: 1000,
		}
		sink := endpoint.NewSink()
		e := engine.New(c, sink)
		for _, ev := range evs {
			mustSubmit(t, e, ev)
		}
		e.Close()
		jr, _ := json.Marshal(sink.Results())
		jd, _ := json.Marshal(sink.Drops())
		return string(jr), string(jd), e.Stats()
	}

	rTum, dTum, sTum := run(0)
	rSlide, dSlide, sSlide := run(1000)
	if rTum != rSlide {
		t.Fatalf("退化滑动与滚动的结果序列不一致:\\ntumbling %s\\nsliding  %s", rTum, rSlide)
	}
	if dTum != dSlide {
		t.Fatalf("退化滑动与滚动的丢弃记录不一致:\\ntumbling %s\\nsliding  %s", dTum, dSlide)
	}
	if sTum != sSlide {
		t.Fatalf("退化滑动与滚动的计数器不一致: %+v vs %+v", sTum, sSlide)
	}
	t.Logf("slide==size 与滚动窗口逐输出/丢弃/计数完全一致（%d 条输出）", sTum.ResultsEmitted)
}

// TestSlidingConcurrentSameKey 多个 goroutine 同时向同一 key 提交滑动窗口事件：
// 每个事件应精确进入其覆盖的全部窗口，并发交错下总计数守恒、每窗口计数确定。
func TestSlidingConcurrentSameKey(t *testing.T) {
	c := slidingCfg()
	c.ShardCount = 4
	sink := endpoint.NewSink()
	e := engine.New(c, sink)
	ctx := context.Background()

	const writers = 8
	const n = 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				// 时间在 [0,5000) 内按 7ms 步长铺开：全部事件都属于 [0,5000)，
				// 且各事件命中不同数量的重叠窗口（1~5），可同时断言单窗精确与总数守恒。
				ts := int64((w*n+i)*7) % 5000
				err := e.Submit(ctx, event.Event{
					ID:        fmt.Sprintf("w%d-%d", w, i),
					Key:       "hot",
					Value:     1,
					EventTime: ts,
				})
				if err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	e.Close()

	// 全部 800 个事件的时间都在 [0,5000)，故每个事件都属于 [0,5000)。
	r := finalByStart(t, sink, "hot", 0)
	if r.Count != writers*n || r.Sum != float64(writers*n) {
		t.Fatalf("滑动窗口并发 [0,5000) = count=%d sum=%v want %d/%d",
			r.Count, r.Sum, writers*n, writers*n)
	}
	// 全部窗口的计数总和 = 每个事件命中的窗口数之和，守恒。
	var total int64
	for _, rr := range sink.FinalResults() {
		total += rr.Count
	}
	var wantTotal int64
	for w := 0; w < writers; w++ {
		for i := 0; i < n; i++ {
			ts := int64((w*n+i)*7) % 5000
			wantTotal += int64(len(specStartsCount(5000, 1000, ts)))
		}
	}
	if total != wantTotal {
		t.Fatalf("滑动并发总计数=%d want %d（漏计/双计）", total, wantTotal)
	}
	if e.Stats().DroppedDup != 0 {
		t.Fatalf("唯一 ID 不应有重复丢弃, got %d", e.Stats().DroppedDup)
	}
	t.Logf("滑动窗口同 key 并发：800 事件，[0,5000)=%d，全窗口总计数=%d 守恒", r.Count, total)
}

// slidingLogger 把滑动窗口的逐条诊断（事件时间/水位线/命中窗口/触发与丢弃原因）接到测试日志。
type slidingLogger struct{ t *testing.T }

func (l slidingLogger) Logf(format string, args ...any) { l.t.Logf(format, args...) }
