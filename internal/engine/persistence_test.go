package engine_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/endpoint"
	"github.com/lisama569/stream-window-engine-go/internal/engine"
	"github.com/lisama569/stream-window-engine-go/internal/event"
)

// TestSnapshotJSONRoundTrip 快照序列化为 JSON（模拟落盘）并从字节恢复：
// 重放不双计，状态与直接内存恢复一致。
func TestSnapshotJSONRoundTrip(t *testing.T) {
	c := cfg()
	sink := endpoint.NewSink()
	e := engine.New(c, sink)
	ctx := context.Background()
	evs := []event.Event{
		{ID: "j1", Key: "p", Value: 1, EventTime: 100},
		{ID: "j2", Key: "p", Value: 2, EventTime: 300},
		{ID: "j3", Key: "q", Value: 9, EventTime: 2400}, // 推进 p 与 q 各自水位线（不同分片各自独立）
	}
	for _, ev := range evs {
		if err := e.Submit(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	raw, err := json.Marshal(e.SaveSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("快照 JSON 长度=%d 字节", len(raw))

	var snap engine.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}

	sink2 := endpoint.NewSink()
	e2 := engine.New(c, sink2)
	if err := e2.Restore(&snap); err != nil {
		t.Fatal(err)
	}
	// 重放已消费事件：新 sink 上不应出现任何输出，全部按重复处理
	for _, ev := range evs {
		if err := e2.Submit(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink2.Results()) != 0 {
		t.Fatalf("JSON 恢复后重放产生 %d 条重复输出", len(sink2.Results()))
	}
	if e2.Stats().DroppedDup != int64(len(evs)) {
		t.Fatalf("DroppedDup=%d want %d", e2.Stats().DroppedDup, len(evs))
	}
	t.Logf("JSON 快照恢复 + 重放 %d 事件: 零新增输出, droppedDup=%d", len(evs), e2.Stats().DroppedDup)
	e2.Close()
}

// TestDeterministicRepro 同一份乱序输入两次独立运行，最终折叠结果必须完全一致。
func TestDeterministicRepro(t *testing.T) {
	evs := []event.Event{
		{ID: "r1", Key: "d", Value: 3, EventTime: 2900},
		{ID: "r2", Key: "d", Value: 1, EventTime: 100},
		{ID: "r3", Key: "d", Value: 4, EventTime: 3100},
		{ID: "r4", Key: "d", Value: 2, EventTime: 1500},
		{ID: "r5", Key: "d", Value: 5, EventTime: 200},
		{ID: "r2", Key: "d", Value: 1, EventTime: 100}, // 重复
	}
	run := func() string {
		sink := endpoint.NewSink()
		e := engine.New(cfg(), sink)
		ctx := context.Background()
		for _, ev := range evs {
			_ = e.Submit(ctx, ev)
		}
		e.Close()
		b, _ := json.Marshal(sink.FinalResults())
		return string(b)
	}
	a, b := run(), run()
	if a != b {
		t.Fatalf("两次运行结果不一致:\n%s\n%s", a, b)
	}
	t.Logf("两次独立运行最终结果一致: %s", a)
}
