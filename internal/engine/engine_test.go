package engine

import (
	"context"
	"math"
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

// testLogger 把事件时间/水位线/窗口判定依据打印到单测日志。
type testLogger struct{ t *testing.T }

func (l testLogger) Logf(format string, args ...any) { l.t.Logf(format, args...) }

func baseConfig() Config {
	return Config{
		ShardCount:            2,
		WindowSizeMillis:      1000,
		OutOfOrderMillis:      500,
		AllowedLatenessMillis: 1000,
		MaxKeys:               0,
	}
}

// dumpResults 打印最终结果视图，便于人工核对确定性。
func dumpResults(t *testing.T, sink *endpoint.Sink) {
	t.Helper()
	for _, r := range sink.Results() {
		t.Logf("RESULT kind=%-6s key=%s window=[%d,%d) version=%d sum=%v count=%d",
			r.Kind, r.Key, r.WindowStart, r.WindowEnd, r.Version, r.Sum, r.Count)
	}
}

// TestOutOfOrderWatermarkFiring 验证：乱序输入下窗口不提前输出，水位线满足条件才触发。
func TestOutOfOrderWatermarkFiring(t *testing.T) {
	sink := endpoint.NewSink()
	e := New(baseConfig(), sink, WithLogger(testLogger{t}))
	defer e.Close()
	ctx := context.Background()

	// in-order 早期事件落在 [0,1000)；乱序事件 800 在 1400 之后到达（仍在水位线容忍内）。
	events := []event.Event{
		{ID: "a1", Key: "a", Value: 1, EventTime: 200},
		{ID: "a2", Key: "a", Value: 10, EventTime: 1400}, // WM -> 900，窗口 [0,1000) 未到点
		{ID: "a3", Key: "a", Value: 100, EventTime: 800}, // 乱序：t=800 >= WM(900)? 否，IsLate=true，但 W0 尚未 Fire
	}
	for _, ev := range events {
		if err := e.Submit(ctx, ev); err != nil {
			t.Fatal(err)
		}
		t.Logf("---- after id=%s eventTime=%d, emitted=%d", ev.ID, ev.EventTime, len(sink.Results()))
		if ev.ID == "a2" && len(sink.Results()) != 0 {
			t.Fatalf("窗口未到水位线不得提前输出: got %d results", len(sink.Results()))
		}
	}
	// 此刻 WM=900 < 1000，W0 仍不得 Fire
	if len(sink.Results()) != 0 {
		t.Fatalf("WM=900 < windowEnd=1000, W0 不得输出, got %d", len(sink.Results()))
	}

	// 推高水位线至 >=1000：WM=1500-500=1000，W0 到点 Fire
	if err := e.Submit(ctx, event.Event{ID: "a4", Key: "a", Value: 1000, EventTime: 1500}); err != nil {
		t.Fatal(err)
	}
	if len(sink.Results()) != 1 {
		t.Fatalf("W0 应恰好 Fire 一次, got %d", len(sink.Results()))
	}
	r := sink.Results()[0]
	if r.Kind != window.Fire || r.WindowStart != 0 || r.Count != 2 || r.Sum != 101 {
		t.Fatalf("W0 fire = %+v, want FIRE count=2 sum=101 (a1+a3 乱序计入)", r)
	}
	t.Logf("W0 关闭判定: watermark=1000 >= windowEnd=1000 -> FIRE, 乱序事件 a3(800) 已计入, sum=%v count=%d", r.Sum, r.Count)
}

// TestLateWithinToleranceUpdate 验证：容忍范围内迟到 -> 同一窗口输出递增 UPDATE；
// 超过宽限期的迟到 -> LATE_BEYOND_TOLERANCE 可观测丢弃，且 Fire 绝不重复。
func TestLateWithinToleranceUpdateAndBeyondDrop(t *testing.T) {
	sink := endpoint.NewSink()
	e := New(baseConfig(), sink, WithLogger(testLogger{t}))
	defer e.Close()
	ctx := context.Background()

	must := func(ev event.Event) {
		t.Helper()
		if err := e.Submit(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	must(event.Event{ID: "b1", Key: "b", Value: 10, EventTime: 100})
	// WM=2500-500=2000：W0(end=1000) Fire
	must(event.Event{ID: "b2", Key: "b", Value: 10, EventTime: 2500})
	if len(sink.Results()) != 1 || sink.Results()[0].Kind != window.Fire {
		t.Fatalf("want exactly one FIRE, got %+v", sink.Results())
	}
	t.Logf("FIRE 后 watermark=%d, W0 已关闭，宽限至 end+lat=%d", 2000, 2000)

	// 容忍范围内迟到（WM=2000 < 1000+1000=2000? 边界：WM=2000 == 2000 -> 先看语义）
	// 采用"超过"判定：WM >= end+lat 才丢弃；相等仍在宽限。
	must(event.Event{ID: "b3", Key: "b", Value: 5, EventTime: 300}) // 不推进 WM(300<2500)
	ups := sink.Results()
	if len(ups) != 2 || ups[1].Kind != window.Update || ups[1].Version != 1 || ups[1].Sum != 15 {
		t.Fatalf("late-within-tolerance 应产生 UPDATE v1 sum=15, got %+v", ups)
	}
	t.Logf("迟到事件 b3 eventTime=300, watermark=%d < 宽限线 2000(含) -> UPDATE v1 sum=%v", 2000, ups[1].Sum)

	// 越过宽限线：WM=3500-500=3000 >= 2000，W0 状态已被清理
	must(event.Event{ID: "b4", Key: "b", Value: 10, EventTime: 3500})
	before := len(sink.Results())
	must(event.Event{ID: "b5", Key: "b", Value: 7, EventTime: 200}) // W0 的迟到事件
	if len(sink.Results()) != before {
		t.Fatalf("超宽限期迟到不得产生窗口输出")
	}
	drops := sink.Drops()
	var found bool
	for _, d := range drops {
		t.Logf("DROP id=%s reason=%s eventTime=%d", d.Ev.ID, d.Reason, d.Ev.EventTime)
		if d.Ev.ID == "b5" && d.Reason == DropLate {
			found = true
		}
	}
	if !found {
		t.Fatalf("b5 应按 %s 观测到", DropLate)
	}
	// W0 的 FIRE 始终只有一条（其他窗口各自的 Fire 不受影响）
	var fires int
	for _, r := range sink.Results() {
		if r.Kind == window.Fire && r.Key == "b" && r.WindowStart == 0 {
			fires++
		}
	}
	if fires != 1 {
		t.Fatalf("FIRE 必须唯一, got %d", fires)
	}
	st := e.Stats()
	t.Logf("stats: droppedLate=%d purgedWindows=%d results=%d", st.DroppedLate, st.PurgedWindows, st.ResultsEmitted)
	if st.DroppedLate != 1 || st.PurgedWindows < 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestDuplicateEventDedup 验证重复事件不双计，且按 DUPLICATE_EVENT 可观测。
func TestDuplicateEventDedup(t *testing.T) {
	sink := endpoint.NewSink()
	e := New(baseConfig(), sink, WithLogger(testLogger{t}))
	defer e.Close()
	ctx := context.Background()

	must := func(ev event.Event) {
		if err := e.Submit(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	ev := event.Event{ID: "dup1", Key: "c", Value: 42, EventTime: 100}
	must(ev)
	must(ev)                                                           // 完全重复
	must(event.Event{ID: "dup1", Key: "c", Value: 42, EventTime: 150}) // 同 ID 再次到达
	must(event.Event{ID: "dup2", Key: "c", Value: 8, EventTime: 200})

	if got := e.Stats().DroppedDup; got != 2 {
		t.Fatalf("DroppedDup=%d want 2", got)
	}
	for _, d := range sink.Drops() {
		t.Logf("DROP id=%s key=%s reason=%s", d.Ev.ID, d.Ev.Key, d.Reason)
	}
	e.Drain()
	// Close 前未到水位线；手动 Close 触发有界冲刷后核对
	e.Close()
	var final *window.Result
	fin := sink.FinalResults()
	for i := range fin {
		if fin[i].Key == "c" && fin[i].WindowStart == 0 {
			final = &fin[i]
		}
	}
	if final == nil || final.Count != 2 || final.Sum != 50 {
		t.Fatalf("final c/W0 = %+v, want count=2 sum=50 (重复不双计)", final)
	}
	t.Logf("去重后最终结果: count=%d sum=%v (dup1 仅计一次，dup2 一次)", final.Count, final.Sum)
}

// TestInvalidInputDoesNotKillPipeline 验证非法输入被旁路观测，不影响其他窗口。
func TestInvalidInputDoesNotKillPipeline(t *testing.T) {
	sink := endpoint.NewSink()
	e := New(baseConfig(), sink, WithLogger(testLogger{t}))
	defer e.Close()
	ctx := context.Background()

	bad := []event.Event{
		{ID: "", Key: "d", Value: 1, EventTime: 100},           // 缺 ID
		{ID: "x", Key: "", Value: 1, EventTime: 100},           // 缺 key
		{ID: "x", Key: "d", Value: 1, EventTime: -1},           // 非法时间戳
		{ID: "x", Key: "d", Value: math.NaN(), EventTime: 100}, // NaN
		{ID: "x", Key: "d", Value: math.Inf(1), EventTime: 100},
	}
	for _, ev := range bad {
		if err := e.Submit(ctx, ev); err != nil {
			t.Fatalf("非法输入不得使 Submit 返回错误中断流程: %v", err)
		}
	}
	// 合法事件照常处理
	if err := e.Submit(ctx, event.Event{ID: "good", Key: "d", Value: 3, EventTime: 100}); err != nil {
		t.Fatal(err)
	}
	st := e.Stats()
	if st.ParseErrors != 5 || st.Processed != 1 {
		t.Fatalf("stats=%+v want parseErrors=5 processed=1", st)
	}
	for _, ie := range sink.InvalidEvents() {
		t.Logf("INVALID id=%q key=%q reason=%s", ie.Event.ID, ie.Event.Key, ie.Reason)
	}
	reasons := map[string]bool{}
	for _, ie := range sink.InvalidEvents() {
		reasons[ie.Reason] = true
	}
	for _, want := range []string{event.ReasonMissingID, event.ReasonMissingKey, event.ReasonInvalidTime, event.ReasonInvalidValue} {
		if !reasons[want] {
			t.Fatalf("缺少非法原因观测: %s", want)
		}
	}
}
