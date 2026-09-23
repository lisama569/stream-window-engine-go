// Package event 定义流处理引擎的输入事件及其校验语义。
//
// 事件时间（EventTime）以毫秒为单位。事件时间相对前序事件"倒退"属于乱序/迟到
// 范畴，由 watermark + window 层处理，本包不将其判为非法；本包只拒绝结构性非法
// 输入（字段缺失、时间戳为负、数值为 NaN/Inf），这类错误以 ParseError 的形式被
// 旁路观测，绝不中断引擎对其他事件的处理。
package event

import (
	"errors"
	"math"
)

// 非法输入原因（可观测、可区分）。
const (
	ReasonMissingID    = "MISSING_ID"
	ReasonMissingKey   = "MISSING_KEY"
	ReasonInvalidTime  = "INVALID_TIMESTAMP"
	ReasonInvalidValue = "INVALID_VALUE"
)

// Event 是一条输入事件。
type Event struct {
	// ID 事件唯一标识，用于跨窗口/重放的重复检测；不能为空。
	ID string
	// Key 聚合键；不能为空。
	Key string
	// Value 参与求和的数值；必须为有限实数。
	Value float64
	// EventTime 事件时间（毫秒，>=0）。允许乱序与倒退。
	EventTime int64
}

// InvalidEvent 表示一条未通过结构校验的事件，携带可区分原因。
type InvalidEvent struct {
	Event  Event
	Reason string
}

func (e *InvalidEvent) Error() string {
	return "invalid event " + e.Event.ID + ": " + e.Reason
}

// Validate 校验事件结构合法性。
// 返回 *InvalidEvent，调用方可按 Reason 区分处理；合法时返回 nil。
func Validate(e Event) error {
	switch {
	case e.ID == "":
		return &InvalidEvent{Event: e, Reason: ReasonMissingID}
	case e.Key == "":
		return &InvalidEvent{Event: e, Reason: ReasonMissingKey}
	case e.EventTime < 0:
		return &InvalidEvent{Event: e, Reason: ReasonInvalidTime}
	case math.IsNaN(e.Value) || math.IsInf(e.Value, 0):
		return &InvalidEvent{Event: e, Reason: ReasonInvalidValue}
	default:
		return nil
	}
}

// AsInvalidEvent 从错误中提取 *InvalidEvent。
func AsInvalidEvent(err error) (*InvalidEvent, bool) {
	var ie *InvalidEvent
	if errors.As(err, &ie) {
		return ie, true
	}
	return nil, false
}
