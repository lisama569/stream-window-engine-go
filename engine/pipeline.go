package engine

import (
	"hash/fnv"
	"sync"
)

// MemorySink is a thread-safe in-memory Sink (the local convergence
// endpoint).
type MemorySink struct {
	mu      sync.Mutex
	results []WindowResult
}

// Emit appends one result.
func (s *MemorySink) Emit(r WindowResult) {
	s.mu.Lock()
	s.results = append(s.results, r)
	s.mu.Unlock()
}

// Results returns a snapshot copy of everything emitted so far.
func (s *MemorySink) Results() []WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]WindowResult, len(s.results))
	copy(out, s.results)
	return out
}

// GeneratedEvent is a locally produced event with an optional explicit
// watermark punctuation attached. Watermark marks act as barriers in parallel
// pushes: the mark is applied only after all earlier events have been
// processed.
type GeneratedEvent struct {
	Event            Event
	WatermarkMark    int64
	HasWatermarkMark bool
}

// Source is the local event-stream producer.
type Source struct {
	events []GeneratedEvent
}

// NewSource builds a source from pre-generated events.
func NewSource(events []GeneratedEvent) *Source {
	return &Source{events: events}
}

// Push feeds events into the engine.
//
// With parallelism <= 1 everything is processed strictly in slice order.
//
// With parallelism > 1, events are partitioned by DeterministicShard(key)
// onto that many worker goroutines; within one shard (hence within one key)
// order is preserved. Watermark marks are barriers: Push waits for all
// events preceding the mark to finish, applies the mark on the calling
// goroutine (so initial window firings happen in a single, deterministic
// place), and only then releases the next batch. This makes the final state
// identical to a sequential run even under goroutine interleaving.
func (src *Source) Push(e *Engine, parallelism int) error {
	if parallelism <= 1 {
		for _, ge := range src.events {
			if ge.HasWatermarkMark {
				// A non-advancing mark (backward/equal) is observable via the
				// reporter; like bad events it must not interrupt the stream.
				_ = e.AdvanceWatermark(ge.WatermarkMark)
			} else if err := e.ProcessEvent(ge.Event); err != nil {
				// Bad input must not interrupt the stream.
				continue
			}
		}
		return nil
	}

	type job struct{ ev Event }
	shardCh := make([]chan job, parallelism)
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		shardCh[i] = make(chan job, 64)
		wg.Add(1)
		go func(ch <-chan job) {
			defer wg.Done()
			for j := range ch {
				// Errors are observable through the reporter/stats; they must
				// never kill the worker.
				_ = e.ProcessEvent(j.ev)
			}
		}(shardCh[i])
	}

	dispatch := func(ev Event) {
		idx := DeterministicShard(ev.Key, parallelism)
		shardCh[idx] <- job{ev: ev}
	}
	barrier := func() {
		// Close and recreate shard channels so all previously dispatched jobs
		// drain before the watermark mark is applied.
		for _, ch := range shardCh {
			close(ch)
		}
		wg.Wait()
		wg = sync.WaitGroup{}
		for i := 0; i < parallelism; i++ {
			shardCh[i] = make(chan job, 64)
			wg.Add(1)
			go func(ch <-chan job) {
				defer wg.Done()
				for j := range ch {
					_ = e.ProcessEvent(j.ev)
				}
			}(shardCh[i])
		}
	}

	for _, ge := range src.events {
		if ge.HasWatermarkMark {
			barrier()
			// A non-advancing mark (replay/duplicate) is observable but must
			// not stop the stream.
			_ = e.AdvanceWatermark(ge.WatermarkMark)
		} else {
			dispatch(ge.Event)
		}
	}
	for _, ch := range shardCh {
		close(ch)
	}
	wg.Wait()
	return nil
}

// DeterministicShard assigns a stable shard index in [0, n) to a key.
func DeterministicShard(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}
