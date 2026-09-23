package event

import (
	"errors"
	"math"
	"testing"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		ev   Event
		want string // 期望原因；空串表示合法
	}{
		{"valid", Event{ID: "1", Key: "k", Value: 3.5, EventTime: 100}, ""},
		{"zero time valid", Event{ID: "1", Key: "k", Value: 0, EventTime: 0}, ""},
		{"missing id", Event{ID: "", Key: "k", Value: 1, EventTime: 1}, ReasonMissingID},
		{"missing key", Event{ID: "1", Key: "", Value: 1, EventTime: 1}, ReasonMissingKey},
		{"negative time", Event{ID: "1", Key: "k", Value: 1, EventTime: -1}, ReasonInvalidTime},
		{"nan", Event{ID: "1", Key: "k", Value: math.NaN(), EventTime: 1}, ReasonInvalidValue},
		{"+inf", Event{ID: "1", Key: "k", Value: math.Inf(1), EventTime: 1}, ReasonInvalidValue},
		{"-inf", Event{ID: "1", Key: "k", Value: math.Inf(-1), EventTime: 1}, ReasonInvalidValue},
	}
	for _, c := range cases {
		err := Validate(c.ev)
		if c.want == "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", c.name, err)
			}
			continue
		}
		var ie *InvalidEvent
		if !errors.As(err, &ie) {
			t.Errorf("%s: want *InvalidEvent, got %v", c.name, err)
			continue
		}
		t.Logf("%s: reason=%s err=%q", c.name, ie.Reason, ie.Error())
		if ie.Reason != c.want {
			t.Errorf("%s: reason=%s want %s", c.name, ie.Reason, c.want)
		}
	}
}

func TestAsInvalidEvent(t *testing.T) {
	if err := Validate(Event{ID: "x"}); err == nil {
		t.Fatal("missing key should be invalid")
	} else if ie, ok := AsInvalidEvent(err); !ok || ie.Reason != ReasonMissingKey {
		t.Fatalf("AsInvalidEvent = %+v, %v", ie, ok)
	}
	if _, ok := AsInvalidEvent(nil); ok {
		t.Fatal("nil error must not extract InvalidEvent")
	}
}
