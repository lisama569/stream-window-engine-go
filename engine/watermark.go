package engine

// WatermarkTracker maintains the monotonically non-decreasing event-time
// watermark. It never moves backwards: attempts to do so are rejected by
// Advance and are observable (the engine reports them via the configured
// DropReporter).
type WatermarkTracker struct {
	current int64
}

// NewWatermarkTracker creates a tracker at the given initial watermark.
// Convention: -1 (or any negative value) means "no event-time information
// observed yet"; the first successful Advance establishes real event time.
func NewWatermarkTracker(initial int64) *WatermarkTracker {
	return &WatermarkTracker{current: initial}
}

// Current returns the current watermark.
func (t *WatermarkTracker) Current() int64 {
	return t.current
}

// Advance moves the watermark to target if and only if target is strictly
// greater than the current value. It returns the watermark after the attempt
// and whether it actually advanced. Equal or smaller targets are rejected so
// that the watermark only advances when the progression condition holds.
func (t *WatermarkTracker) Advance(target int64) (int64, bool) {
	if target > t.current {
		t.current = target
		return target, true
	}
	return t.current, false
}
