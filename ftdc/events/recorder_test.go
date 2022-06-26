package events

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tychoish/birch/ftdc"
)

type MockCollector struct {
	Metadata      interface{}
	Data          []interface{}
	MetadataError error
	ResolveError  error
	AddError      error
	Output        []byte
	ResolveCount  int
	ResetCount    int
	State         ftdc.CollectorInfo
}

func (c *MockCollector) SetMetadata(in interface{}) error { c.Metadata = in; return c.MetadataError }
func (c *MockCollector) Add(in interface{}) error         { c.Data = append(c.Data, in); return c.AddError }
func (c *MockCollector) Resolve() ([]byte, error)         { c.ResolveCount++; return c.Output, c.ResolveError }
func (c *MockCollector) Reset()                           { c.ResetCount++ }
func (c *MockCollector) Info() ftdc.CollectorInfo         { return c.State }

type recorderTestCase struct {
	Name string
	Case func(*testing.T, Recorder, *MockCollector)
}

func TestRecorder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, impl := range []struct {
		Name    string
		Factory func(ftdc.Collector) Recorder
		Cases   []recorderTestCase
	}{
		{
			Name:    "Raw",
			Factory: NewRawRecorder,
			Cases: []recorderTestCase{
				{
					Name: "IncOpsFullCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						var lastTotal time.Duration
						var totalDur time.Duration
						iterations := 10
						for i := 0; i < iterations; i++ {
							r.BeginIteration()
							time.Sleep(time.Millisecond)
							start := time.Now()
							if len(c.Data) != i {
								t.Errorf("length should be %d", i)
							}
							r.IncOperations(10)
							if len(c.Data) != i {
								t.Errorf("length should be %d", i)
							}
							dur := time.Since(start)
							r.EndIteration(dur)
							if len(c.Data) != i+1 {
								t.Fatalf("lengths of %d and %d are not expected", len(c.Data), i+1)
							}

							totalDur += dur

							payload, ok := c.Data[i].(*Performance)
							if !ok {
								t.Fatal("truth assertion failed")
							}

							if int64((i+1)*10) != payload.Counters.Operations {
								t.Fatalf("values are not equal %v and %v", (i+1)*10, payload.Counters.Operations)
							}
							if int64(i+1) != payload.Counters.Number {
								t.Fatalf("values are not equal %v and %v", i+1, payload.Counters.Number)
							}
							if totalDur != payload.Timers.Duration {
								t.Error("values should be equal")
							}
							assert.True(t, payload.Timers.Total > lastTotal)
							assert.True(t, payload.Timers.Total >= payload.Timers.Duration)
							lastTotal = payload.Timers.Total
						}
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}
						payload, ok := c.Data[len(c.Data)-1].(*Performance)
						if !ok {
							t.Fatal("truth assertion failed")
						}
						if int64(iterations*10) != payload.Counters.Operations {
							t.Fatalf("values are not equal %v and %v", iterations*10, payload.Counters.Operations)
						}
						if int64(iterations) != payload.Counters.Number {
							t.Fatalf("values are not equal %v and %v", iterations, payload.Counters.Number)
						}
						if totalDur != payload.Timers.Duration {
							t.Error("values should be equal")
						}
						if lastTotal != payload.Timers.Total {
							t.Error("values should be equal")
						}
						assert.True(t, payload.Timers.Total >= payload.Timers.Duration)
						assert.True(t, time.Since(payload.Timestamp) <= time.Second)
					},
				},
			},
		},
		{
			Name:    "SinglePerformance",
			Factory: NewSingleRecorder,
			Cases:   []recorderTestCase{},
		},
		{
			Name:    "SingleHistogram",
			Factory: NewSingleHistogramRecorder,
			Cases:   []recorderTestCase{},
		},
		{
			Name: "RawSync",
			Factory: func(c ftdc.Collector) Recorder {
				return NewSynchronizedRecorder(NewRawRecorder(c))
			},
			Cases: []recorderTestCase{
				{
					Name: "IncOpsFullCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						r.BeginIteration()
						time.Sleep(time.Millisecond)
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.IncOperations(10)
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.EndIteration(time.Minute)
						if len(c.Data) != 1 {
							t.Fatalf("lengths of %d and %d are not expected", len(c.Data), 1)
						}

						payload, ok := c.Data[0].(*Performance)
						if !ok {
							t.Fatal("truth assertion failed")
						}
						if 10 != payload.Counters.Operations {
							t.Fatalf("values are not equal %v and %v", 10, payload.Counters.Operations)
						}
						if 1 != payload.Counters.Number {
							t.Fatalf("values are not equal %v and %v", 1, payload.Counters.Number)
						}
						if time.Minute != payload.Timers.Duration {
							t.Error("values should be equal")
						}
						assert.True(t, payload.Timers.Total > 0)
					},
				},
			},
		},
		{
			Name:    "Histogram",
			Factory: NewHistogramRecorder,
		},
		{
			Name: "GroupedShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewGroupedRecorder(c, 100*time.Millisecond)
			},
			Cases: []recorderTestCase{},
		},
		{
			Name: "GroupedLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewGroupedRecorder(c, time.Second)
			},
			Cases: []recorderTestCase{},
		},
		{
			Name: "IntervalShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalRecorder(ctx, c, 100*time.Millisecond)
			},
			Cases: []recorderTestCase{},
		},
		{
			Name: "IntervalLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalRecorder(ctx, c, time.Second)
			},
			Cases: []recorderTestCase{},
		},
		{
			Name: "GroupedHistogramShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewHistogramGroupedRecorder(c, 100*time.Millisecond)
			},
			Cases: []recorderTestCase{},
		},
		{
			Name: "GroupedHistogramLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewHistogramGroupedRecorder(c, time.Second)
			},
			Cases: []recorderTestCase{},
		},
		{
			Name: "IntervalHistogramShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalHistogramRecorder(ctx, c, 100*time.Millisecond)
			},
			Cases: []recorderTestCase{},
		},
		{
			Name: "IntervalHistogramLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalHistogramRecorder(ctx, c, time.Second)
			},
			Cases: []recorderTestCase{},
		},
	} {
		t.Run(impl.Name, func(t *testing.T) {
			for _, test := range impl.Cases {
				t.Run(test.Name, func(t *testing.T) {
					c := &MockCollector{}
					r := impl.Factory(c)
					test.Case(t, r, c)
				})
			}
			for _, test := range []recorderTestCase{
				{
					Name: "BeginEndOpsCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						for i := 0; i < 10; i++ {
							r.BeginIteration()
							time.Sleep(time.Millisecond)
							r.IncOperations(1)
							r.EndIteration(time.Second)
						}
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}
						if len(c.Data) == 0 {
							t.Fatal("truth assertion failed")
						}

						switch data := c.Data[len(c.Data)-1].(type) {
						case *Performance:
							assert.True(t, data.Timers.Duration >= 9*time.Second, "%s", data.Timers.Duration)
							assert.True(t, data.Timers.Total > 0)
							if data.Counters.Operations != 10 {
								t.Fatalf("values are not equal %v and %v", data.Counters.Operations, 10)
							}
							assert.True(t, time.Since(data.Timestamp) <= time.Second)
						case *PerformanceHDR:
							if 10 != data.Counters.Number.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Counters.Number.TotalCount())
							}
							if 1.0 != data.Counters.Number.Mean() {
								t.Error("values should be equal")
							}

							if 10 != data.Timers.Duration.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Timers.Duration.TotalCount())
							}
							assert.InDelta(t, time.Second, int64(data.Timers.Duration.Mean()), float64(time.Microsecond))

							if 10 != data.Counters.Operations.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Counters.Operations.TotalCount())
							}
							if 1.0 != data.Counters.Operations.Mean() {
								t.Error("values should be equal")
							}
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},
				{
					Name: "BeginEndSizeCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						for i := 0; i < 10; i++ {
							r.BeginIteration()
							time.Sleep(time.Millisecond)
							r.IncSize(1024)
							r.EndIteration(100 * time.Millisecond)
						}
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}
						if len(c.Data) == 0 {
							t.Fatal("truth assertion failed")
						}

						switch data := c.Data[len(c.Data)-1].(type) {
						case *Performance:
							assert.True(t, data.Timers.Duration >= time.Second, "%s", data.Timers.Duration)
							assert.True(t, data.Timers.Total > 0)
							if data.Counters.Size != 10*1024 {
								t.Fatalf("values are not equal %v and %v", data.Counters.Size, 10*1024)
							}
						case *PerformanceHDR:
							if 10 != data.Counters.Number.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Counters.Number.TotalCount())
							}
							if 1.0 != data.Counters.Number.Mean() {
								t.Error("values should be equal")
							}

							if 10 != data.Timers.Duration.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Timers.Duration.TotalCount())
							}
							assert.InDelta(t, 100*time.Millisecond, int64(data.Timers.Duration.Mean()), float64(time.Microsecond))

							if 10 != data.Counters.Size.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Counters.Size.TotalCount())
							}
							if 1024.0 != data.Counters.Size.Mean() {
								t.Error("values should be equal")
							}
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},
				{
					Name: "BeginEndErrorCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						for i := 0; i < 10; i++ {
							r.BeginIteration()
							time.Sleep(time.Millisecond)
							r.IncError(3)
							r.EndIteration(10 * time.Millisecond)
						}
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}
						if len(c.Data) == 0 {
							t.Fatal("truth assertion failed")
						}

						switch data := c.Data[len(c.Data)-1].(type) {
						case *Performance:
							assert.True(t, data.Timers.Duration >= 100*time.Millisecond, "%s", data.Timers.Duration)
							assert.True(t, data.Timers.Total > 0)
						case *PerformanceHDR:
							if 10 != data.Counters.Number.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Counters.Number.TotalCount())
							}
							if 1.0 != data.Counters.Number.Mean() {
								t.Error("values should be equal")
							}

							if 10 != data.Timers.Duration.TotalCount() {
								t.Fatalf("values are not equal %v and %v", 10, data.Timers.Duration.TotalCount())
							}
							assert.InDelta(t, 10*time.Millisecond, int64(data.Timers.Duration.Mean()), float64(time.Microsecond))
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},
				{
					Name: "ResetCall",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.Reset()
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
					},
				},
				{
					Name: "IncrementAndSetDoNotTriggerEndTest",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.IncOperations(21)
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.SetState(2)
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
					},
				},
				{
					Name: "SetStateReplaces",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.SetState(20)
						r.SetState(422)
						r.EndIteration(time.Second)
						r.BeginIteration()
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							if data.Gauges.State != 422 {
								t.Fatalf("values are not equal %v and %v", data.Gauges.State, 422)
							}
						case *PerformanceHDR:
							if data.Gauges.State != 422 {
								t.Fatalf("values are not equal %v and %v", data.Gauges.State, 422)
							}
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},
				{
					Name: "SetWorkersReplaces",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.SetWorkers(20)
						r.SetWorkers(422)
						r.EndIteration(time.Second)
						r.BeginIteration()
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							if data.Gauges.Workers != 422 {
								t.Fatalf("values are not equal %v and %v", data.Gauges.Workers, 422)
							}
						case *PerformanceHDR:
							if data.Gauges.Workers != 422 {
								t.Fatalf("values are not equal %v and %v", data.Gauges.Workers, 422)
							}
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetFailedDefault",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.EndIteration(time.Second)
						r.BeginIteration()
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							assert.False(t, data.Gauges.Failed)
						case *PerformanceHDR:
							assert.False(t, data.Gauges.Failed)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetFailedOverrides",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.SetFailed(true)
						r.EndIteration(time.Second)
						r.BeginIteration()
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							assert.True(t, data.Gauges.Failed)
						case *PerformanceHDR:
							assert.True(t, data.Gauges.Failed)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetFailedCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.SetFailed(true)
						r.SetFailed(false)
						r.SetFailed(true)
						r.EndIteration(time.Second)
						r.BeginIteration()
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							assert.True(t, data.Gauges.Failed)
						case *PerformanceHDR:
							assert.True(t, data.Gauges.Failed)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetTotalDuration",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.SetTotalDuration(time.Minute)

						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							if time.Minute != data.Timers.Total.Round(time.Millisecond) {
								t.Error("values should be equal")
							}
						case *PerformanceHDR:
							count := data.Timers.Total.TotalCount()
							assert.True(t, int64(1) <= count, "count=%d", count)
							if time.Minute != time.Duration(data.Timers.Total.Max()).Round(time.Millisecond) {
								t.Error("values should be equal")
							}
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetDuration",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.SetDuration(time.Minute)
						r.EndIteration(0)

						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							if time.Minute != data.Timers.Duration.Round(time.Millisecond) {
								t.Error("values should be equal")
							}
						case *PerformanceHDR:
							count := data.Timers.Total.TotalCount()
							assert.True(t, int64(1) <= count, "count=%d", count)
							if time.Minute != time.Duration(data.Timers.Duration.Max()).Round(time.Millisecond) {
								t.Error("values should be equal")
							}
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "IncIterations",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						r.BeginIteration()
						r.IncIterations(42)
						r.EndIteration(0)

						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							// it's 42 or 53 depending on the behavior of end
							assert.True(t, 42 == data.Counters.Number || 43 == data.Counters.Number) // nolint
						case *PerformanceHDR:
							count := data.Counters.Number.TotalCount()
							assert.True(t, 1 <= count, "count=%d", count)
							if 42 != data.Counters.Number.Max() {
								t.Fatalf("values are not equal %v and %v", 42, data.Counters.Number.Max())
							}
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetTime",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						ts := time.Now().Add(time.Hour).Round(time.Second)
						r.BeginIteration()
						r.SetTime(ts)
						r.EndIteration(time.Second)
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							if ts != data.Timestamp {
								t.Fatalf("values are not equal %v and %v", ts, data.Timestamp)
							}
						case *PerformanceHDR:
							if ts != data.Timestamp {
								t.Fatalf("values are not equal %v and %v", ts, data.Timestamp)
							}
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetID",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						if len(c.Data) != 0 {
							t.Errorf("length should be %d", 0)
						}
						var id int64 = 42
						r.BeginIteration()
						r.SetID(id)
						r.EndIteration(time.Second)
						if err := r.EndTest(); err != nil {
							t.Fatal(err)
						}

						switch data := c.Data[0].(type) {
						case *Performance:
							if id != data.ID {
								t.Fatalf("values are not equal %v and %v", id, data.ID)
							}
						case *PerformanceHDR:
							if id != data.ID {
								t.Fatalf("values are not equal %v and %v", id, data.ID)
							}
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					c := &MockCollector{}
					r := impl.Factory(c)
					test.Case(t, r, c)
				})

			}
		})
	}
}
