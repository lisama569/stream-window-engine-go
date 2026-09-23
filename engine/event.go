package engine

import "math"

// Event is a single input record carrying an event-time timestamp.
type Event struct {
	// ID is the globally unique identifier used for duplicate detection.
	ID string
	// Key is the aggregation key (window is computed per key).
	Key string
	// Timestamp is the event time in epoch milliseconds.
	Timestamp int64
	// Value is the measured numeric value being aggregated.
	Value float64
}

// ValidationError describes why a raw event is rejected before it can
// influence any window. A validation failure never touches window state and
// therefore cannot pollute other windows.
type ValidationError struct {
	Reason DropReason
	Detail string
}

func (v *ValidationError) Error() string {
	return string(v.Reason) + ": " + v.Detail
}

// Validate checks the raw fields of an event. A nil result means the event
// is well formed. Checks are ordered: required fields, then timestamp, then
// numeric value, so the reported reason is deterministic.
func (e Event) Validate() *ValidationError {
	if e.ID == "" {
		return &ValidationError{Reason: DropMissingField, Detail: "event id is required"}
	}
	if e.Key == "" {
		return &ValidationError{Reason: DropMissingField, Detail: "event key is required"}
	}
	if e.Timestamp < 0 {
		return &ValidationError{Reason: DropInvalidTimestamp, Detail: "timestamp must be a non-negative epoch-millis value"}
	}
	if math.IsNaN(e.Value) || math.IsInf(e.Value, 0) {
		return &ValidationError{Reason: DropInvalidValue, Detail: "value must be a finite number"}
	}
	return nil
}
