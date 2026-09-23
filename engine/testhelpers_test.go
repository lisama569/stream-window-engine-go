package engine

import (
	"testing"
)

// logDecision prints event time, current watermark and the window decision
// basis into the test log, as required for observability of failures.
func logDecision(t *testing.T, phase string, ev Event, wm int64, w Window, note string) {
	t.Helper()
	t.Logf("[%s] event=%s key=%s eventTime=%d watermark=%d -> window=[%d,%d) %s",
		phase, ev.ID, ev.Key, ev.Timestamp, wm, w.Start, w.End, note)
}

func logWatermark(t *testing.T, phase string, target, wm int64, advanced bool, note string) {
	t.Helper()
	t.Logf("[%s] watermarkMark=%d watermarkNow=%d advanced=%v %s",
		phase, target, wm, advanced, note)
}

func ev(id, key string, ts int64, v float64) Event {
	return Event{ID: id, Key: key, Timestamp: ts, Value: v}
}

func mark(ts int64) GeneratedEvent {
	return GeneratedEvent{WatermarkMark: ts, HasWatermarkMark: true}
}

func ge(id, key string, ts int64, v float64) GeneratedEvent {
	return GeneratedEvent{Event: ev(id, key, ts, v)}
}
