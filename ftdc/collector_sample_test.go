package ftdc

import (
	"testing"
	"time"

	"github.com/tychoish/birch/ftdc/testutil"
)

func TestSamplingCollector(t *testing.T) {
	collector := NewSamplingCollector(10*time.Millisecond, &betterCollector{maxDeltas: 20})
	if 0 != collector.Info().SampleCount {
		t.Error("values should be equal")
	}
	for i := 0; i < 10; i++ {
		if err := collector.Add(testutil.RandFlatDocument(20)); err != nil {
			t.Error(err)
		}
	}
	if 1 != collector.Info().SampleCount {
		t.Error("values should be equal")
	}

	for i := 0; i < 4; i++ {
		time.Sleep(10 * time.Millisecond)
		if err := collector.Add(testutil.RandFlatDocument(20)); err != nil {
			t.Error(err)
		}
	}

	if 5 != collector.Info().SampleCount {
		t.Error("values should be equal")
	}
}
