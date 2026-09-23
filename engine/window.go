package engine

// Window describes a single tumbling window's time range [Start, End).
type Window struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// Contains reports whether the event time ts belongs to this window.
// Windows are half-open: [Start, End).
func (w Window) Contains(ts int64) bool {
	return ts >= w.Start && ts < w.End
}

// TumblingWindow computes the window containing timestamp ts for a tumbling
// windowing scheme aligned to the unix epoch. sizeMillis must be positive.
func TumblingWindow(ts int64, sizeMillis int64) Window {
	idx := floorDiv(ts, sizeMillis)
	start := idx * sizeMillis
	return Window{Start: start, End: start + sizeMillis}
}

// floorDiv returns the mathematical floor of a/b for positive b. Validated
// timestamps are positive, but the floor semantics keep the result sane for
// any input.
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// Aggregate is the incremental per-window aggregation. Implementations must
// be safe to use within the single state-mutation section of the engine.
type Aggregate interface {
	// Add incorporates an accepted event value.
	Add(v float64)
	// Merge merges another partial aggregate of the same type into this one.
	Merge(other Aggregate)
	// Value returns the scalar aggregation result.
	Value() float64
	// Count returns the number of incorporated events.
	Count() int64
}

// SumAggregate computes the sum of accepted event values.
type SumAggregate struct {
	Sum    float64
	Count_ int64
}

func (a *SumAggregate) Add(v float64) {
	a.Sum += v
	a.Count_++
}

func (a *SumAggregate) Merge(o Aggregate) {
	if other, ok := o.(*SumAggregate); ok {
		a.Sum += other.Sum
		a.Count_ += other.Count_
	}
}

func (a *SumAggregate) Value() float64 { return a.Sum }
func (a *SumAggregate) Count() int64   { return a.Count_ }

// CountAggregate counts accepted events (the measured value is ignored).
type CountAggregate struct{ N int64 }

func (a *CountAggregate) Add(v float64) { a.N++ }

func (a *CountAggregate) Merge(o Aggregate) {
	if other, ok := o.(*CountAggregate); ok {
		a.N += other.N
	}
}

func (a *CountAggregate) Value() float64 { return float64(a.N) }
func (a *CountAggregate) Count() int64   { return a.N }

// AggregateFactory constructs fresh aggregates.
type AggregateFactory func() Aggregate
