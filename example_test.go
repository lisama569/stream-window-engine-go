package engine_test_test

import (
	"context"
	"fmt"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/engine"
	"github.com/lisama569/stream-window-engine-go/internal/event"
)

// Example_localPipeline 演示本地事件源与本地汇聚出口下的乱序/迟到/异常混合处理。
// 运行：go test ./... -run Example_localPipeline -v
func Example_localPipeline() {
	cfg := engine.Config{
		ShardCount:            2,
		WindowSizeMillis:      1000, // 1 秒滚动窗口
		OutOfOrderMillis:      500,  // 水位线滞后最大事件时间 500ms
		AllowedLatenessMillis: 1000, // Fire 后 1 秒宽限
	}
	source := endpoint.NewSource()
	sink := endpoint.NewSink()
	eng := engine.New(cfg, sink)
	defer eng.Close()

	// 本地产生事件：正常、乱序、结构非法、重复、迟到混在一起。
	source.Emit(event.Event{ID: "1", Key: "userA", Value: 10, EventTime: 100})
	source.Emit(event.Event{ID: "2", Key: "userA", Value: 20, EventTime: 1600}) // 推进 WM=1100，W0 Fire
	source.Emit(event.Event{ID: "3", Key: "userA", Value: 5, EventTime: 400})   // 乱序：W0 已 Fire 但在宽限内 -> UPDATE
	source.Emit(event.Event{ID: "bad", Key: "userA", Value: 1, EventTime: -7})  // 非法时间戳 -> 旁路，不中断
	source.Emit(event.Event{ID: "1", Key: "userA", Value: 10, EventTime: 100})  // 重复 -> 去重观测
	source.Emit(event.Event{ID: "4", Key: "userA", Value: 7, EventTime: 9000})  // WM 远超宽限，清理 W0

	ctx := context.Background()
	for _, ev := range source.Drain() {
		_ = eng.Submit(ctx, ev)
	}
	eng.Close()

	for _, r := range sink.FinalResults() {
		fmt.Printf("%s window=[%d,%d) sum=%.0f count=%d v=%d\n",
			r.Key, r.WindowStart, r.WindowEnd, r.Sum, r.Count, r.Version)
	}
	for _, ie := range sink.InvalidEvents() {
		fmt.Printf("invalid: %s\n", ie.Reason)
	}
	for _, d := range sink.Drops() {
		fmt.Printf("drop: %s %s\n", d.Ev.ID, d.Reason)
	}
	// Unordered output:
	// userA window=[0,1000) sum=15 count=2 v=1
	// userA window=[1000,2000) sum=20 count=1 v=0
	// userA window=[9000,10000) sum=7 count=1 v=0
	// invalid: INVALID_TIMESTAMP
	// drop: 1 DUPLICATE_EVENT
}

// Example_slidingPipeline 演示"每 1 分钟往前看 5 分钟"的滑动窗口：
// 同一份事件进入多个窗口、各自到点输出，迟到事件只修正它覆盖到的较早窗口。
// 运行：go test . -run Example_slidingPipeline -v
func Example_slidingPipeline() {
	cfg := engine.Config{
		ShardCount:            1,
		WindowSizeMillis:      5000, // 看 5 分钟（示例按毫秒缩放）
		WindowSlideMillis:     1000, // 每 1 分钟滑动一次
		OutOfOrderMillis:      0,
		AllowedLatenessMillis: 1000,
	}
	sink := endpoint.NewSink()
	eng := engine.New(cfg, sink)
	defer eng.Close()
	ctx := context.Background()

	events := []event.Event{
		{ID: "1", Key: "userA", Value: 10, EventTime: 100},
		{ID: "2", Key: "userA", Value: 20, EventTime: 1500}, // 与 1 同时进入 [0,5000)，另入 [1000,6000)
		{ID: "1", Key: "userA", Value: 10, EventTime: 100},  // 重复：在两个窗口各判一次
		{ID: "3", Key: "userA", Value: 30, EventTime: 6000}, // WM=6000：[0,5000)/[1000,6000) 同时 FIRE
		{ID: "4", Key: "userA", Value: 5, EventTime: 200},   // 宽限内迟到：只修正 [0,5000)
	}
	for _, ev := range events {
		_ = eng.Submit(ctx, ev)
	}
	eng.Close()

	for _, r := range sink.FinalResults() {
		fmt.Printf("%s window=[%d,%d) sum=%.0f count=%d v=%d\n",
			r.Key, r.WindowStart, r.WindowEnd, r.Sum, r.Count, r.Version)
	}
	for _, d := range sink.Drops() {
		fmt.Printf("drop: %s window=[%d,%d) %s\n", d.Ev.ID, d.WindowStart, d.WindowEnd, d.Reason)
	}
	// Unordered output:
	// userA window=[0,5000) sum=35 count=3 v=1
	// userA window=[1000,6000) sum=20 count=1 v=0
	// userA window=[2000,7000) sum=30 count=1 v=0
	// userA window=[3000,8000) sum=30 count=1 v=0
	// userA window=[4000,9000) sum=30 count=1 v=0
	// userA window=[5000,10000) sum=30 count=1 v=0
	// userA window=[6000,11000) sum=30 count=1 v=0
	// drop: 1 window=[0,5000) DUPLICATE_EVENT
}
