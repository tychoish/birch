package events

import (
	"testing"
)

func TestRollupRoundTrip(t *testing.T) {
	data := MakeCustom(4)
	if err := data.Add("a", 1.2); err != nil {
		t.Error(err)
	}
	if err := data.Add("f", 100); err != nil {
		t.Error(err)
	}
	if err := data.Add("b", 45.0); err != nil {
		t.Error(err)
	}
	if err := data.Add("d", []int64{45, 32}); err != nil {
		t.Error(err)
	}
	if err := data.Add("foo", Custom{}); err == nil {
		t.Error("error should be nil")
	}
	if len(data) != 4 {
		t.Errorf("length should be %d", 4)
	}

	t.Run("NewBSON", func(t *testing.T) {
		payload, err := data.MarshalBSON()
		if err != nil {
			t.Fatal(err)
		}

		rt := Custom{}
		err = rt.UnmarshalBSON(payload)
		if err != nil {
			t.Fatal(err)
		}

		if len(rt) != 4 {
			t.Fatalf("lengths of %d and %d are not expected", len(rt), 4)
		}
		if "a" != rt[0].Name {
			t.Error("values should be equal")
		}
		if "b" != rt[1].Name {
			t.Error("values should be equal")
		}
		if "d" != rt[2].Name {
			t.Error("values should be equal")
		}
		if "f" != rt[3].Name {
			t.Error("values should be equal")
		}
		if 1.2 != rt[0].Value {
			t.Error("values should be equal")
		}
		if 45.0 != rt[1].Value {
			t.Error("values should be equal")
		}
		if 100 != rt[3].Value.(int32) {
			t.Fatalf("values are not equal %v and %v", 100, rt[3].Value)
		}
	})
}
