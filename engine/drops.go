package engine

import "sync"

// DropReason enumerates the distinguishable reasons an input can be rejected.
// Every rejection path reports exactly one of these reasons, so nothing is
// silently dropped.
type DropReason string

const (
	// DropInvalidTimestamp: timestamp is not a positive epoch-millis value.
	DropInvalidTimestamp DropReason = "INVALID_TIMESTAMP"
	// DropInvalidValue: numeric value is NaN or +-Inf.
	DropInvalidValue DropReason = "INVALID_VALUE"
	// DropMissingField: a required field (ID / key) is absent.
	DropMissingField DropReason = "MISSING_FIELD"
	// DropTooLate: event arrived after its window was purged (watermark past
	// window end + allowed lateness).
	DropTooLate DropReason = "TOO_LATE"
	// DropDuplicate: an event with the same ID was already accepted and the
	// window state is still live.
	DropDuplicate DropReason = "DUPLICATE"
	// DropMemoryLimit: accepting the event would exceed the configured memory
	// bound.
	DropMemoryLimit DropReason = "MEMORY_LIMIT"
	// DropBackwardWatermark: a watermark punctuation tried to move the
	// watermark backwards (or to the same value).
	DropBackwardWatermark DropReason = "BACKWARD_WATERMARK"
)

// DropRecord is an observable, reason-tagged record of a rejected input.
type DropRecord struct {
	Reason    DropReason `json:"reason"`
	EventID   string     `json:"event_id,omitempty"`
	Key       string     `json:"key,omitempty"`
	Timestamp int64      `json:"timestamp,omitempty"`
	Watermark int64      `json:"watermark,omitempty"`
	Detail    string     `json:"detail,omitempty"`
}

// DropReporter receives every rejection. Implementations must be safe for
// concurrent use.
type DropReporter interface {
	Report(rec DropRecord)
}

// MemoryDropReporter is a bounded in-memory collector of drop records. The
// reporter itself is bounded: at most capacity records are retained; further
// records still increment per-reason counters and the overflow counter.
type MemoryDropReporter struct {
	mu       sync.Mutex
	records  []DropRecord
	capacity int
	overflow int
	byReason map[DropReason]int
}

// NewMemoryDropReporter creates a reporter storing at most capacity records.
// A non-positive capacity is replaced by 1024.
func NewMemoryDropReporter(capacity int) *MemoryDropReporter {
	if capacity <= 0 {
		capacity = 1024
	}
	return &MemoryDropReporter{
		records:  make([]DropRecord, 0, capacity),
		capacity: capacity,
		byReason: make(map[DropReason]int),
	}
}

// Report stores a drop record. It never blocks or panics once the buffer is
// full; the overflow and per-reason counters keep advancing.
func (r *MemoryDropReporter) Report(rec DropRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byReason[rec.Reason]++
	if len(r.records) < r.capacity {
		r.records = append(r.records, rec)
	} else {
		r.overflow++
	}
}

// Records returns a snapshot copy of stored records.
func (r *MemoryDropReporter) Records() []DropRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]DropRecord, len(r.records))
	copy(out, r.records)
	return out
}

// OverflowCount reports how many records were not stored because the bounded
// buffer was full.
func (r *MemoryDropReporter) OverflowCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.overflow
}

// CountByReason returns the number of reported records per reason, including
// records that did not fit in the bounded buffer.
func (r *MemoryDropReporter) CountByReason() map[DropReason]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[DropReason]int, len(r.byReason))
	for k, v := range r.byReason {
		out[k] = v
	}
	return out
}
