package ftdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/x/ftdc/testutil"
)

// Map converts the chunk to a map representation. Each key in the map
// is a "composite" key with a dot-separated fully qualified document
// path. The values in this map include all of the values collected
// for this chunk.
func (c *Chunk) renderMap() map[string]Metric {
	m := make(map[string]Metric)
	for _, metric := range c.Metrics {
		m[metric.Key()] = metric
	}
	return m
}

func printError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

type testMessage map[string]any

func (m testMessage) String() string {
	by, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(by)
}

func TestReadPathIntegration(t *testing.T) {
	for _, test := range []struct {
		name            string
		path            string
		skipSlow        bool
		skipAll         bool
		expectedNum     int
		expectedChunks  int
		expectedMetrics int
		reportInterval  int
		docLen          int
	}{
		{
			name:            "PerfMock",
			path:            "perf_metrics.ftdc",
			docLen:          4,
			expectedNum:     10,
			expectedChunks:  10,
			expectedMetrics: 100000,
			reportInterval:  10000,
			skipSlow:        true,
		},
		{
			name:            "ServerStatus",
			path:            "metrics.ftdc",
			skipSlow:        true,
			docLen:          6,
			expectedNum:     1064,
			expectedChunks:  544,
			expectedMetrics: 300,
			reportInterval:  1000,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.skipAll && testing.Short() {
				t.Skip("skipping all read integration tests")
			}
			t.Skip("fixtures not configured")

			file, err := os.Open(test.path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { printError(file.Close()) }()
			data, err := ioutil.ReadAll(file)
			if err != nil {
				t.Fatal(err)
			}

			expectedSamples := test.expectedChunks * test.expectedMetrics
			t.Run("Chunks", func(t *testing.T) {
				startAt := time.Now()
				iter := ReadChunks(bytes.NewBuffer(data))
				counter := 0
				num := 0
				hasSeries := 0

				for c := range iter.Iterator() {
					counter++
					if num == 0 {
						num = len(c.Metrics)
						if test.expectedNum != num {
							t.Fatalf("unqueal %v and %v", test.expectedNum, num)
						}
					}

					metric := c.Metrics[rand.Intn(num)]
					if len(metric.Values) > 0 {
						t.Fatal("truth assertion failed")
					}
					hasSeries++
					if metric.startingValue != metric.Values[0] {
						t.Error("values should be equal")
					}
					if len(metric.Values) != test.expectedMetrics {
						t.Error("unexpected number of values")
					}

					if counter == 10 {
						for _, v := range c.renderMap() {
							if len(v.Values) != test.expectedMetrics {
								t.Errorf("length should be %d", test.expectedMetrics)
							}
							if v.startingValue != v.Values[0] {
								t.Error("values should be equal")
							}
						}

						numSamples := 0
						for doc := range c.Iterator().Iterator() {
							numSamples++
							if doc == nil {
								t.Error("doc should not be nil")
							} else if doc.Len() != test.expectedNum {
								t.Error("values should be equal")
							}
						}
						if test.expectedMetrics != numSamples {
							t.Error("values should be equal")
						}

						data, err := c.export()
						if err != nil {
							t.Fatal(err)
						}

						if len(c.Metrics) < data.Len() {
							t.Error("expected true")
						}
						elems := 0
						for elem := range data.Iterator() {
							array := elem.Value().MutableArray()
							if test.expectedMetrics != array.Len() {
								t.Fatalf("unqueal %v and %v", test.expectedMetrics, array.Len())
							}
							elems++
						}
						// this is inexact
						// because of timestamps...l
						if len(c.Metrics) < elems {
							t.Error("expected true")
						}
						if elems != data.Len() {
							t.Error("values should be equal")
						}
					}
				}

				if err := iter.Close(); err != nil {
					t.Error(err)
				}

				if test.expectedNum != num {
					t.Error("values should be equal")
				}
				if test.expectedChunks != counter {
					t.Error("values should be equal")
				}
				fmt.Println(testMessage{
					"parser":   "chunks",
					"series":   num,
					"iters":    counter,
					"dur_secs": time.Since(startAt).Seconds(),
				})
			})
			t.Run("MatrixSeries", func(t *testing.T) {
				startAt := time.Now()
				iter := ReadSeries(bytes.NewBuffer(data))
				counter := 0
				for doc := range iter.Iterator() {
					if doc == nil {
						t.Fatalf("%T value is nil", doc)
					}
					if doc.Len() > 0 {
						t.Fatal("truth assertion failed")
					}
					counter++
				}
				if test.expectedChunks != counter {
					t.Error("values should be equal")
				}
				if err := iter.Close(); err != nil {
					t.Fatal(err)
				}
				fmt.Println(testMessage{
					"parser":   "matrix_series",
					"iters":    counter,
					"dur_secs": time.Since(startAt).Seconds(),
				})
			})
			t.Run("Matrix", func(t *testing.T) {
				if test.skipSlow && testing.Short() {
					t.Skip("skipping slow read integration tests")
				}
				startAt := time.Now()
				iter := ReadMatrix(bytes.NewBuffer(data))
				counter := 0
				for doc := range iter.Iterator() {
					counter++

					if counter%10 != 0 {
						continue
					}

					if doc == nil {
						t.Fatalf("%T value is nil", doc)
					}
					if doc.Len() > 0 {
						t.Fatal("truth assertion failed")
					}
				}
				if err := iter.Close(); err != nil {
					t.Fatal(err)
				}
				if test.expectedChunks != counter {
					t.Error("values should be equal")
				}

				fmt.Println(testMessage{
					"parser":   "matrix",
					"iters":    counter,
					"dur_secs": time.Since(startAt).Seconds(),
				})
			})
			t.Run("Combined", func(t *testing.T) {
				if test.skipSlow && testing.Short() {
					t.Skip("skipping slow read integration tests")
				}

				t.Run("Structured", func(t *testing.T) {
					startAt := time.Now()
					iter := ReadStructuredMetrics(bytes.NewBuffer(data))
					counter := 0
					for doc := range iter.Iterator() {
						if counter >= expectedSamples/10 {
							break
						}

						counter++
						if counter%10 != 0 {
							continue
						}

						if doc == nil {
							t.Fatalf("%T value is nil", doc)
						}
						if counter%test.reportInterval == 0 {
							fmt.Println(testMessage{
								"flavor":   "STRC",
								"seen":     counter,
								"elapsed":  time.Since(startAt).Seconds(),
								"metadata": iter.Metadata(),
							})
							startAt = time.Now()
						}

						if test.docLen != doc.Len() {
							t.Fatalf("unqueal %v and %v", test.docLen, doc.Len())
						}
					}
					if err := iter.Close(); err != nil {
						t.Error(err)
					}
					if counter < expectedSamples/10 {
						t.Error("expected true")
					}
				})
				t.Run("Flattened", func(t *testing.T) {
					startAt := time.Now()
					iter := ReadMetrics(bytes.NewBuffer(data))
					counter := 0
					for doc := range iter.Iterator() {
						if counter >= expectedSamples/10 {
							break
						}
						counter++
						if counter%10 != 0 {
							continue
						}
						if doc == nil {
							t.Fatalf("%T value is nil", doc)
						}
						if counter%test.reportInterval == 0 {
							fmt.Println(testMessage{
								"flavor":   "FLAT",
								"seen":     counter,
								"elapsed":  time.Since(startAt).Seconds(),
								"metadata": iter.Metadata(),
							})
							startAt = time.Now()
						}

						if test.expectedNum != doc.Len() {
							t.Fatalf("unqueal %v and %v", test.expectedNum, doc.Len())
						}
					}
					if err := iter.Close(); err != nil {
						t.Error(err)
					}
					if counter < expectedSamples/10 {
						t.Error("expected true")
					}
				})
			})
		})
	}
}

func TestRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	collectors := createCollectors(ctx)
	for _, collect := range collectors {
		t.Run(collect.name, func(t *testing.T) {
			tests := createTests()
			for _, test := range tests {
				if test.numStats == 0 || (test.randStats && !strings.Contains(collect.name, "Dynamic")) {
					continue
				}
				if test.name != "Floats" {
					continue
				}
				t.Run(test.name, func(t *testing.T) {
					collector := collect.factory()
					if err := collector.SetMetadata(testutil.CreateEventRecord(42, int64(time.Minute), rand.Int63n(7), 4)); err != nil {
						t.Error(err)
					}

					var docs []*birch.Document
					for _, d := range test.docs {
						if err := collector.Add(d); err != nil {
							t.Error(err)
						}
						docs = append(docs, d)
					}

					data, err := collector.Resolve()
					if err != nil {
						t.Fatal(err)
					}
					iter := ReadStructuredMetrics(bytes.NewBuffer(data))

					docNum := 0
					for doc := range iter.Iterator() {
						if docNum < len(docs) {
							t.Fatal("truth assertion failed")
						}

						if fmt.Sprint(doc) != fmt.Sprint(docs[docNum]) {
							t.Error("values should be equal")
						}
						docNum++
					}
				})
			}
		})
	}
}
