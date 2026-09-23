package endpoint

import (
	"sync"
	"testing"

	"github.com/lisama569/stream-window-engine-go/internal/event"
	"github.com/lisama569/stream-window-engine-go/internal/window"
)

func TestSourceDrain(t *testing.T) {
	s := NewSource()
	s.Emit(event.Event{ID: "1"})
	s.Emit(event.Event{ID: "2"})
	got := s.Drain()
	if len(got) != 2 || s.Len() != 0 {
		t.Fatalf("drain=%v remaining=%d", got, s.Len())
	}
	t.Logf("Source drained %d events: %+v", len(got), got)
}

func TestSinkConcurrentCollectAndFinalResults(t *testing.T) {
	sink := NewSink()
	var wg sync.WaitGroup
	// 两个 goroutine 交错写同一窗口的不同版本输出，验证并发安全与版本折叠
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				sink.CollectResult(window.Result{
					Key: "k", WindowStart: 0, WindowEnd: 1000,
					Kind: window.Update, Version: int64(g*100 + i), Sum: float64(i), Count: int64(i),
				})
			}
		}(g)
	}
	wg.Wait()
	if len(sink.Results()) != 200 {
		t.Fatalf("lost writes: %d", len(sink.Results()))
	}
	fin := sink.FinalResults()
	if len(fin) != 1 || fin[0].Version != 199 {
		t.Fatalf("final fold = %+v, want single result version=199", fin)
	}
	t.Logf("200 条交错输出折叠为 1 条最终结果: version=%d sum=%v count=%d",
		fin[0].Version, fin[0].Sum, fin[0].Count)
}
