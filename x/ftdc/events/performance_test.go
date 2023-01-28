package events

import (
	"testing"
)

func TestPerformanceType(t *testing.T) {
	t.Run("MethodsPanicWhenNil", func(t *testing.T) {
		var perf *Performance
		func() {
			defer func() {
				if p := recover(); p == nil {
					t.Error("case should panic")
				}
			}()
			_, err := perf.MarshalDocument()
			if err == nil {
				t.Fatal("error should not be nill")
			}
		}()
		func() {
			defer func() {
				if p := recover(); p == nil {
					t.Error("case should panic")
				}
			}()
			_, err := perf.MarshalBSON()
			if err == nil {
				t.Error("error should not be nil")
			}
		}()
		func() {
			defer func() {
				if p := recover(); p == nil {
					t.Error("case should panic")
				}
			}()
			perf.Add(nil)
		}()
		func() {
			defer func() {
				if p := recover(); p == nil {
					t.Error("case should panic")
				}
			}()
			perf = &Performance{}
			perf.Add(nil)
		}()
	})
	t.Run("Document", func(t *testing.T) {
		perf := &Performance{}
		doc, err := perf.MarshalDocument()
		if err != nil {
			t.Fatal(err)
		}
		if doc == nil {
			t.Fatalf("%T value is nil", doc)
		}
		if 5 != doc.Len() {
			t.Error("values should be equal")
		}
	})
	t.Run("BSON", func(t *testing.T) {
		perf := &Performance{}
		out, err := perf.MarshalBSON()
		if err != nil {
			t.Error(err)
		}
		if out == nil {
			t.Fatal("'out' should not be nil")
		}
	})
	t.Run("Add", func(t *testing.T) {
		t.Run("Zero", func(t *testing.T) {
			perf := &Performance{}
			perf.Add(&Performance{})
			if perf.ID != 1 {
				t.Errorf("values should be equal: %d", perf.ID)
			}
		})
		t.Run("OverridesID", func(t *testing.T) {
			perf := &Performance{}
			perf.Add(&Performance{ID: 100})
			if 100 != perf.ID {
				t.Fatalf("values are not equal %v and %v", 100, perf.ID)
			}
			perf.Add(&Performance{ID: 100})
			if 100 != perf.ID {
				t.Fatalf("values are not equal %v and %v", 100, perf.ID)
			}
		})
		t.Run("Counter", func(t *testing.T) {
			perf := &Performance{}
			perf.Add(&Performance{Counters: PerformanceCounters{Number: 100}})
			if 100 != perf.Counters.Number {
				t.Fatalf("values are not equal %v and %v", 100, perf.Counters.Number)
			}
			perf.Add(&Performance{Counters: PerformanceCounters{Number: 100}})
			if 200 != perf.Counters.Number {
				t.Fatalf("values are not equal %v and %v", 200, perf.Counters.Number)
			}
		})
	})
}
