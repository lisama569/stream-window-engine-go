package engine

import "errors"

// Config configures an Engine.
type Config struct {
	// WindowSizeMillis is the tumbling window size in event-time millis.
	WindowSizeMillis int64
	// AllowedLatenessMillis is the extra time after window end during which
	// late events still update a window and a revised result is emitted.
	AllowedLatenessMillis int64
	// MaxOutOfOrderMillis is the bounded-delay heuristic: when positive,
	// observed events may advance the watermark to
	// (max seen event ts - delay) without an explicit punctuation.
	MaxOutOfOrderMillis int64
	// MaxLiveWindows bounds the number of open/active window states held at
	// once (a hard memory bound). Non-positive means unlimited.
	MaxLiveWindows int
	// MaxEventsPerWindow bounds the per-window accepted-event footprint.
	// Non-positive means unlimited.
	MaxEventsPerWindow int
	// Aggregate builds the per-(window,key) aggregate. Defaults to Sum.
	Aggregate AggregateFactory
	// Reporter observes rejected inputs and rejected watermark attempts.
	// Optional; when nil, a bounded default reporter is created.
	Reporter DropReporter
}

func (c Config) validate() error {
	if c.WindowSizeMillis <= 0 {
		return errors.New("WindowSizeMillis must be positive")
	}
	if c.AllowedLatenessMillis < 0 {
		return errors.New("AllowedLatenessMillis must be non-negative")
	}
	if c.MaxOutOfOrderMillis < 0 {
		return errors.New("MaxOutOfOrderMillis must be non-negative")
	}
	return nil
}
