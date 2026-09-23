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
