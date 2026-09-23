package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/engine"
	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

func cfg() engine.Config {
	return engine.Config{
		ShardCount:            4,
		WindowSizeMillis:      1000,
		OutOfOrderMillis:      500,
		AllowedLatenessMillis: 2000,
		MaxKeys:               0,
	}
}

// replayAll 把一批事件按相同顺序重新提交给（恢复后的）引擎。
func replayAll(t *testing.T, e *engine.Engine, evs []event.Event) {
	t.Helper()
	ctx := context.Background()
	for _, ev := range evs {
		if err := e.Submit(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSnapshotRestoreReplay 从中途快照恢复并重放已消费数据：
// 输出不重复、不漏计；水位线与窗口状态与快照点一致。
func TestSnapshotRestoreReplay(t *testing.T) {

	// 第一批：制造"部分窗口已 Fire、部分窗口仍打开"的中途状态。
	first := []event.Event{
		{ID: "s1", Key: "k1", Value: 1, EventTime: 100},
		{ID: "s2", Key: "k1", Value: 2, EventTime: 500},
		{ID: "s3", Key: "k2", Value: 10, EventTime: 200},
		{ID: "s4", Key: "k1", Value: 4, EventTime: 2500},  // WM=2000：W0 Fire
		{ID: "s5", Key: "k2", Value: 20, EventTime: 1200}, // 乱序，落入 W1（未 Fire）
	}

	sink1 := endpoint.NewSink()
	e1 := engine.New(cfg(), sink1)
	replayAll(t, e1, first)
	snap := e1.SaveSnapshot()

	st1 := e1.Stats()
	t.Logf("快照点 stats: %+v", st1)
	t.Logf("快照点输出 %d 条:", len(sink1.Results()))
	for _, r := range sink1.Results() {
		t.Logf("  快照前 OUTPUT kind=%s key=%s window=[%d,%d) v=%d sum=%v count=%d wm隐含于快照",
			r.Kind, r.Key, r.WindowStart, r.WindowEnd, r.Version, r.Sum, r.Count)
	}
	if st1.Processed != 5 {
		t.Fatalf("processed=%d want 5", st1.Processed)
	}
	// 快照点只有 k1 的水位线被 s4(2500) 推进到 2000，故仅 k1/W0 已 Fire；
	// k2/W0、k2/W1、k1/W2 仍打开——这正是需要快照保护的"半开"中途状态。
	if len(sink1.Results()) != 1 {
		t.Fatalf("快照点应只有 1 条 FIRE(k1/W0), got %d", len(sink1.Results()))
	}

	// 用全新引擎恢复到同一 sink（模拟重启后继续向同一个本地汇聚出口输出）：
	// 快照前已发出的结果保留在 sink 中，恢复后重放绝不允许产生重复输出。
	sink2 := sink1
	e2 := engine.New(cfg(), sink2)
	if err := e2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	st2 := e2.Stats()
	if st2.Processed != st1.Processed || st2.ResultsEmitted != st1.ResultsEmitted {
		t.Fatalf("恢复后计数器与快照点不一致: %+v vs %+v", st2, st1)
	}

	// 重放快照点之前已消费的全部事件：必须全部识别为重复，不产生任何【新增】窗口输出。
	outputsBeforeReplay := len(sink2.Results())
	replayAll(t, e2, first)
	if got := len(sink2.Results()); got != outputsBeforeReplay {
		for _, r := range sink2.Results()[outputsBeforeReplay:] {
			t.Errorf("重放已消费数据产生了重复输出: %+v", r)
		}
	}
	stReplay := e2.Stats()
	t.Logf("重放 %d 条已消费事件后: processed=%d (仍=%d) droppedDup=%d outputs=%d",
		len(first), stReplay.Processed, st1.Processed, stReplay.DroppedDup, len(sink2.Results()))
	if stReplay.Processed != st1.Processed {
		t.Fatalf("重放导致重复计数: processed %d -> %d", st1.Processed, stReplay.Processed)
	}
	if stReplay.DroppedDup != int64(len(first)) {
		t.Fatalf("重放事件应全部识别为重复, droppedDup=%d want %d", stReplay.DroppedDup, len(first))
	}

	// 继续喂快照点之后的新事件：新事件正常计入；关闭后最终结果可预测
	second := []event.Event{
		{ID: "s6", Key: "k2", Value: 30, EventTime: 2600}, // WM=2100：W1(end=2000) Fire
		{ID: "s7", Key: "k1", Value: 5, EventTime: 2700},  // W2
	}
	replayAll(t, e2, second)

	// 仅关闭恢复后的引擎；e1 已被快照取代，不能 Close（它与 e2 共用同一 sink，
	// 旧引擎冲刷会把快照前窗口再写一遍，模拟真实重启时旧进程已退出）。
	e2.Close()

	// 快照前输出保留 + 重放零新增 + 后续新事件正常输出，折叠后即为确定终值：
	fin := sink2.FinalResults()
	want := map[string]window.Result{}
	byKey := func(r window.Result) string { return fmt.Sprintf("%s@%d", r.Key, r.WindowStart) }
	for _, r := range fin {
		want[byKey(r)] = r
		t.Logf("FINAL key=%s window=[%d,%d) sum=%v count=%d version=%d",
			r.Key, r.WindowStart, r.WindowEnd, r.Sum, r.Count, r.Version)
	}
	check := func(key string, start int64, sum float64, count int64) {
		r, ok := want[fmt.Sprintf("%s@%d", key, start)]
		if !ok {
			t.Fatalf("缺少最终窗口 %s@%d", key, start)
		}
		if r.Sum != sum || r.Count != count {
			t.Fatalf("%s@%d = sum=%v count=%d, want sum=%v count=%d", key, start, r.Sum, r.Count, sum, count)
		}
	}
	// 依据事件时间的确定归属：
	// k1/W0 = s1+s2 = 3；k2/W0 = s3 = 10；k2/W1 = s5 = 20；
	// k1/W2 = s4(2500)+s7(2700) = 9；k2/W2 = s6(2600) = 30
	check("k1", 0, 3, 2)
	check("k2", 0, 10, 1)
	check("k2", 1000, 20, 1)
	check("k1", 2000, 9, 2)
	check("k2", 2000, 30, 1)
}

// TestSnapshotDeterministicBytes 同一状态连续两次快照必须字节一致（可复现）。
func TestSnapshotDeterministicBytes(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(cfg(), sink)
	replayAll(t, e, []event.Event{
		{ID: "z1", Key: "z", Value: 1, EventTime: 100},
		{ID: "m1", Key: "m", Value: 2, EventTime: 200},
		{ID: "a1", Key: "a", Value: 3, EventTime: 300},
		{ID: "z2", Key: "z", Value: 4, EventTime: 2600},
	})
	b1 := fmt.Sprintf("%+v", e.SaveSnapshot())
	b2 := fmt.Sprintf("%+v", e.SaveSnapshot())
	if b1 != b2 {
		t.Fatal("同一状态的两次快照不一致（不确定）")
	}
	t.Log("连续快照内容一致，快照可复现")
	e.Close()
}

// TestConcurrentInterleaving 多 goroutine 交错提交：按 key 分桶后每个 key 的
// 聚合结果必须与串行预期完全一致，不重不漏。
func TestConcurrentInterleaving(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(cfg(), sink)
	ctx := context.Background()

	const goroutines = 16
	const perKeyEvents = 50
	var wg sync.WaitGroup
	// 每个 goroutine 负责一批互不相同 key 的事件（ID 全局唯一），
	// goroutine 之间故意交错（事件时间乱序），但同一 key 只在一个 goroutine 内串行。
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perKeyEvents; i++ {
				key := fmt.Sprintf("key-%02d", g)
				// 时间戳故意交错：奇数 i 给很大的时间推进水位线，偶数给旧窗口时间
				var ts int64
				if i%2 == 0 {
					ts = int64(i) * 10 // 旧窗口 [0,1000)
				} else {
					ts = 3000 + int64(i)*10
				}
				ev := event.Event{
					ID:        fmt.Sprintf("g%d-e%d", g, i),
					Key:       key,
					Value:     float64(g + 1),
					EventTime: ts,
				}
				if err := e.Submit(ctx, ev); err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	e.Close()

	fin := sink.FinalResults()
	// 每个 key: 偶数 i (0,2,...,48)=25 个落入 W0；奇数 25 个散布在 W3..W7。
	// 校验 W0：25 个事件，每个 value=g+1
	w0 := map[string]window.Result{}
	for _, r := range fin {
		if r.WindowStart == 0 {
			w0[r.Key] = r
		}
	}
	if len(w0) != goroutines {
		t.Fatalf("W0 key 数=%d want %d", len(w0), goroutines)
	}
	for g := 0; g < goroutines; g++ {
		key := fmt.Sprintf("key-%02d", g)
		r := w0[key]
		if r.Count != 25 || r.Sum != float64(25*(g+1)) {
			t.Fatalf("%s W0 = sum=%v count=%d, want %v/25", key, r.Sum, r.Count, 25*(g+1))
		}
	}
	// 所有 key 的事件总数守恒：每个 key 50 条
	totalCount := int64(0)
	for _, r := range fin {
		totalCount += r.Count
	}
	wantTotal := int64(goroutines * perKeyEvents)
	if totalCount != wantTotal {
		t.Fatalf("并发后总计数=%d want %d（存在漏计/双计）", totalCount, wantTotal)
	}
	st := e.Stats()
	t.Logf("并发交错完成: totalCount=%d droppedDup=%d parseErrors=%d results=%d active=%d",
		totalCount, st.DroppedDup, st.ParseErrors, st.ResultsEmitted, st.ActiveWindows)
	if st.DroppedDup != 0 {
		t.Fatalf("唯一 ID 不应有重复丢弃, got %d", st.DroppedDup)
	}
}

// TestConcurrentSameKeyContention 多个 goroutine 同时向同一 key 提交，验证分片锁
// 串行化下结果仍精确（计数 == 提交数，求和确定）。
func TestConcurrentSameKeyContention(t *testing.T) {
	sink := endpoint.NewSink()
	e := engine.New(cfg(), sink)
	ctx := context.Background()

	const writers = 8
	const n = 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				err := e.Submit(ctx, event.Event{
					ID:        fmt.Sprintf("w%d-%d", w, i),
					Key:       "hotkey",
					Value:     1,
					EventTime: int64(i % 500), // 全部在 W0
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

	var w0 *window.Result
	for i, r := range sink.FinalResults() {
		if r.Key == "hotkey" && r.WindowStart == 0 {
			w0 = &sink.FinalResults()[i]
		}
	}
	if w0 == nil {
		t.Fatal("缺少 hotkey/W0 结果")
	}
	if w0.Count != writers*n || w0.Sum != float64(writers*n) {
		t.Fatalf("hotkey W0 = count=%d sum=%v want %d/%d", w0.Count, w0.Sum, writers*n, writers*n)
	}
	t.Logf("同 key 竞争: %d 个写者 x %d 事件 -> count=%d sum=%v（串行化精确）",
		writers, n, w0.Count, w0.Sum)
}

// TestMemoryLimitObservable 活跃 key 达上限时以 MEMORY_LIMIT_REJECTED 可区分处置，
// 而非无界增长；过期窗口回收后容量恢复。
func TestMemoryLimitObservable(t *testing.T) {
	c := cfg()
	c.ShardCount = 1 // 单分片使上限可精确断言
	c.AllowedLatenessMillis = 0
	c.MaxKeys = 3
	sink := endpoint.NewSink()
	e := engine.New(c, sink)
	ctx := context.Background()

	// 三个不同 key 各开一个 W0 窗口，占满容量
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := e.Submit(ctx, event.Event{ID: "e-" + k, Key: k, Value: 1, EventTime: 100}); err != nil {
			t.Fatal(err)
		}
	}
	// 第四个新 key：水位线未推进，无过期窗口可回收 -> 内存上限拒绝
	err := e.Submit(ctx, event.Event{ID: "e-k4", Key: "k4", Value: 1, EventTime: 200})
	if err != nil {
		t.Fatalf("内存上限是可观测丢弃而非 Submit 错误: %v", err)
	}
	st := e.Stats()
	if st.DroppedMemory == 0 || st.ActiveWindows > c.MaxKeys {
		t.Fatalf("内存上限未生效: stats=%+v active=%d", st, st.ActiveWindows)
	}
	t.Logf("容量 %d 占满后 k4 被 %s 拒绝（不返回错误、不污染其他窗口）", c.MaxKeys, engine.DropMemoryLimit)

	// 推进水位线越过 W0：Fire 并立即清理（lateness=0, WM>end），容量恢复
	if err := e.Submit(ctx, event.Event{ID: "adv", Key: "k1", Value: 1, EventTime: 3000}); err != nil {
		t.Fatal(err)
	}
	if err := e.Submit(ctx, event.Event{ID: "e-k4b", Key: "k4", Value: 1, EventTime: 3100}); err != nil {
		t.Fatal(err)
	}
	for _, d := range sink.Drops() {
		t.Logf("DROP id=%s reason=%s", d.Ev.ID, d.Reason)
	}
	if e.Stats().ActiveWindows > c.MaxKeys {
		t.Fatalf("回收后活跃窗口仍超上限: %d", e.Stats().ActiveWindows)
	}
	e.Close()
}
