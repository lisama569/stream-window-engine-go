package engine

import "encoding/json"

// WindowSnapshot is the serializable state of one (window, key) aggregate.
// Seen and Values are aligned by index and preserve acceptance order, so an
// aggregate factory can rebuild the exact aggregate on restore by replaying
// Add for each value.
type WindowSnapshot struct {
	Start int64    `json:"start"`
	Key   string   `json:"key"`
	Value float64  `json:"value"`
	Count int64    `json:"count"`
	Seen  []string `json:"seen"`
	// Values are the accepted event values in acceptance order, aligned with
	// Seen by index.
	Values []float64 `json:"values"`
	// Fired marks windows whose initial result has already been emitted.
	Fired bool `json:"fired"`
}

// Snapshot is a consistent, serializable engine state taken while all
// mutation is quiesced: watermark, heuristic bookkeeping, every live window
// (including which events were already deduplicated) and the emission flag.
type Snapshot struct {
	Watermark int64            `json:"watermark"`
	MaxSeenTS int64            `json:"max_seen_ts"`
	Windows   []WindowSnapshot `json:"windows"`
	Seq       int64            `json:"seq"`
}

// Marshal serializes the snapshot to JSON.
func (s Snapshot) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// UnmarshalSnapshot restores a snapshot from JSON.
func UnmarshalSnapshot(data []byte) (Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}
