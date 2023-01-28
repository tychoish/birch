package ftdc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/x/ftdc/testutil"
)

func TestCollectorInterface(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, collect := range createCollectors(ctx) {
		t.Run(collect.name, func(t *testing.T) {
			tests := createTests()

			for _, test := range tests {
				if testing.Short() {
					continue
				}

				if collect.uncompressed {
					t.Skip("not supported for uncompressed collectors")
				}

				t.Run(test.name, func(t *testing.T) {
					collector := collect.factory()

					if err := collector.SetMetadata(testutil.CreateEventRecord(42, int64(time.Minute), rand.Int63n(7), 4)); err != nil {
						t.Error(err)
					}

					info := collector.Info()
					if info.MetricsCount != 0 || info.SampleCount != 0 {
						t.Errorf("unexpected info %+v", info)
					}

					for _, d := range test.docs {
						if err := collector.Add(d); err != nil {
							t.Fatal(err)
						}
					}
					info = collector.Info()

					if test.randStats {
						if info.MetricsCount < test.numStats {
							t.Fatal("unexpeted value")
						}
					} else {
						if test.numStats != info.MetricsCount {
							t.Fatalf("info=%+v, %v", info, test.docs)
						}
					}

					out, err := collector.Resolve()
					if len(test.docs) > 0 {
						if err != nil {
							t.Error(err)
						}
						if len(out) == 0 {
							t.Fatalf("did not expect zero")
						}

					} else {
						if err == nil {
							t.Error("error should not be nil")
						}
						if len(out) != 0 {
							t.Fatal("expected zero")
						}
					}

					collector.Reset()
					info = collector.Info()
					if info.MetricsCount != 0 || info.SampleCount != 0 {
						t.Fatalf("should be zero: %+v", info)
					}
				})
			}
			t.Run("ResolveWhenNil", func(t *testing.T) {
				collector := collect.factory()
				out, err := collector.Resolve()
				if err == nil {
					t.Error("error should not be nil")
				}
				if out != nil {
					t.Error("expected nil output")
				}
			})
			t.Run("RoundTrip", func(t *testing.T) {
				if collect.uncompressed {
					t.Skip("without compressing these tests don't make much sense")
				}
				for name, docs := range map[string][]*birch.Document{
					"Integers": {
						testutil.RandFlatDocument(5),
						testutil.RandFlatDocument(5),
						testutil.RandFlatDocument(5),
						testutil.RandFlatDocument(5),
					},
					"DecendingHandIntegers": {
						birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5)),
						birch.DC.Elements(birch.EC.Int64("one", 89), birch.EC.Int64("two", 4)),
						birch.DC.Elements(birch.EC.Int64("one", 99), birch.EC.Int64("two", 3)),
						birch.DC.Elements(birch.EC.Int64("one", 101), birch.EC.Int64("two", 2)),
					},
				} {
					t.Run(name, func(t *testing.T) {
						collector := collect.factory()
						count := 0
						for _, d := range docs {
							count++
							if err := collector.Add(d); err != nil {
								t.Error(err)
							}
						}
						time.Sleep(time.Millisecond) // force context switch so that the buffered collector flushes
						info := collector.Info()
						if info.SampleCount != count {
							t.Fatalf("unqueal %v and %v", info.SampleCount, count)
						}

						out, err := collector.Resolve()
						if err != nil {
							t.Fatal(err)
						}
						buf := bytes.NewBuffer(out)

						iter := ReadStructuredMetrics(ctx, buf)
						idx := -1
						for iter.Next(ctx) {
							idx++
							t.Run(fmt.Sprintf("DocumentNumber_%d", idx), func(t *testing.T) {
								s := iter.Value()

								if fmt.Sprint(s) != fmt.Sprint(docs[idx]) {
									t.Error("---", idx)
									t.Error("in: ", docs[idx])
									t.Error("out:", s)
								}
							})
						}
						if len(docs)-1 != idx {
							t.Error("values should be equal", len(docs)-1, idx)
						} // zero index
						if err := iter.Close(ctx); err != nil {
							t.Fatal("close err", err)
						}

					})
				}
			})
		})
	}
}

func TestStreamingEncoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, impl := range []struct {
		name    string
		factory func() (Collector, *bytes.Buffer)
	}{
		{
			name: "StreamingDynamic",
			factory: func() (Collector, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				return NewStreamingDynamicCollector(100, buf), buf
			},
		},
		{
			name: "StreamingDynamicSmall",
			factory: func() (Collector, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				return NewStreamingDynamicCollector(2, buf), buf
			},
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
			for _, test := range createEncodingTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Run("SingleValues", func(t *testing.T) {
						collector, buf := impl.factory()
						for _, val := range test.dataset {
							if err := collector.Add(birch.DC.Elements(birch.EC.Int64("foo", val))); err != nil {
								t.Error(err)
							}
						}
						if err := FlushCollector(collector, buf); err != nil {
							t.Fatal(err)
						}
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						idx := 0
						for iter.Next(ctx) {
							doc := iter.Value()
							if doc == nil {
								t.Fatalf("%T value is nil", doc)
							}
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							if val != test.dataset[idx] {
								t.Error("values should be equal")
							}
							idx++
						}
						if err := iter.Close(ctx); err != nil {
							t.Fatal(err)
						}
						if len(test.dataset) != len(res) {
							t.Fatalf("unqueal %v and %v", len(test.dataset), len(res))
						}
						if !int64SliceEqual(test.dataset, res) {
							t.Error("values should be equal")
						}
					})
					t.Run("MultipleValues", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []*birch.Document{}

						for _, val := range test.dataset {
							doc := birch.DC.Elements(
								birch.EC.Int64("foo", val),
								birch.EC.Int64("dub", 2*val),
								birch.EC.Int64("dup", val),
								birch.EC.Int64("neg", -1*val),
								birch.EC.Int64("mag", 10*val),
							)
							docs = append(docs, doc)
							if err := collector.Add(doc); err != nil {
								t.Error(err)
							}
						}

						if err := FlushCollector(collector, buf); err != nil {
							t.Fatal(err)
						}
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Value()
							if doc == nil {
								t.Fatalf("%T value is nil", doc)
							}
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if fmt.Sprint(doc) != fmt.Sprint(docs[idx]) {
								t.Error("values should be equal")
							}
						}

						if err := iter.Close(ctx); err != nil {
							t.Fatal(err)
						}
						if len(test.dataset) != len(res) {
							t.Fatalf("unqueal %v and %v", len(test.dataset), len(res))
						}
						if !int64SliceEqual(test.dataset, res) {
							t.Error("values should be equal")
						}
					})

					t.Run("MultiValueKeyOrder", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []*birch.Document{}

						for idx, val := range test.dataset {
							var doc *birch.Document
							if len(test.dataset) >= 3 && (idx == 2 || idx == 3) {
								doc = birch.DC.Elements(
									birch.EC.Int64("foo", val),
									birch.EC.Int64("mag", 10*val),
									birch.EC.Int64("neg", -1*val),
								)
							} else {
								doc = birch.DC.Elements(
									birch.EC.Int64("foo", val),
									birch.EC.Int64("dub", 2*val),
									birch.EC.Int64("dup", val),
									birch.EC.Int64("neg", -1*val),
									birch.EC.Int64("mag", 10*val),
								)
							}

							docs = append(docs, doc)
							if err := collector.Add(doc); err != nil {
								t.Error(err)
							}
						}
						if err := FlushCollector(collector, buf); err != nil {
							t.Fatal(err)
						}
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Value()
							if doc == nil {
								t.Fatalf("%T value is nil", doc)
							}
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if fmt.Sprint(doc) != fmt.Sprint(docs[idx]) {
								t.Error("values should be equal")
							}
						}

						if err := iter.Close(ctx); err != nil {
							t.Fatal(err)
						}
						if len(test.dataset) != len(res) {
							t.Fatalf("unqueal %v and %v", len(test.dataset), len(res))
						}
						if !int64SliceEqual(test.dataset, res) {
							t.Error("values should be equal")
						}
					})
					t.Run("DifferentKeys", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []*birch.Document{}

						for idx, val := range test.dataset {
							var doc *birch.Document
							if len(test.dataset) >= 5 && (idx == 2 || idx == 3) {
								doc = birch.DC.Elements(
									birch.EC.Int64("foo", val),
									birch.EC.Int64("dub", 2*val),
									birch.EC.Int64("dup", val),
									birch.EC.Int64("neg", -1*val),
									birch.EC.Int64("mag", 10*val),
								)
							} else {
								doc = birch.DC.Elements(
									birch.EC.Int64("foo", val),
									birch.EC.Int64("mag", 10*val),
									birch.EC.Int64("neg", -1*val),
									birch.EC.Int64("dup", val),
									birch.EC.Int64("dub", 2*val),
								)
							}

							docs = append(docs, doc)
							if err := collector.Add(doc); err != nil {
								t.Error(err)
							}
						}

						if err := FlushCollector(collector, buf); err != nil {
							t.Fatal(err)
						}
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Value()
							if doc == nil {
								t.Fatalf("%T value is nil", doc)
							}
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if fmt.Sprint(doc) != fmt.Sprint(docs[idx]) {
								t.Error("values should be equal")
							}
						}
						if err := iter.Close(ctx); err != nil {
							t.Fatal(err)
						}
						if len(test.dataset) != len(res) {
							t.Fatalf("unqueal %v and %v", len(test.dataset), len(res))
						}
					})
				})
			}
		})
	}
}

func TestFixedEncoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, impl := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{maxDeltas: 20} },
		},
		{
			name:    "StableDynamic",
			factory: func() Collector { return NewDynamicCollector(100) },
		},
		{
			name:    "Streaming",
			factory: func() Collector { return newStreamingCollector(20, &bytes.Buffer{}) },
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
			for _, test := range createEncodingTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Run("SingleValues", func(t *testing.T) {
						collector := impl.factory()
						for _, val := range test.dataset {
							if err := collector.Add(birch.DC.Elements(birch.EC.Int64("foo", val))); err != nil {
								t.Error(err)
							}
						}

						payload, err := collector.Resolve()
						if err != nil {
							t.Fatal(err)
						}
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						idx := 0
						for iter.Next(ctx) {
							doc := iter.Value()
							if doc == nil {
								t.Fatalf("%T value is nil", doc)
							}
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							if val != test.dataset[idx] {
								t.Error("values should be equal")
							}
							idx++
						}
						if err := iter.Close(ctx); err != nil {
							t.Fatal(err)
						}
						if len(test.dataset) != len(res) {
							t.Fatalf("unqueal %v and %v", len(test.dataset), len(res))
						}
						if !int64SliceEqual(test.dataset, res) {
							t.Error("values should be equal")
						}
					})
					t.Run("MultipleValues", func(t *testing.T) {
						collector := impl.factory()
						docs := []*birch.Document{}

						for _, val := range test.dataset {
							doc := birch.DC.Elements(
								birch.EC.Int64("foo", val),
								birch.EC.Int64("dub", 2*val),
								birch.EC.Int64("dup", val),
								birch.EC.Int64("neg", -1*val),
								birch.EC.Int64("mag", 10*val),
							)
							docs = append(docs, doc)
							if err := collector.Add(doc); err != nil {
								t.Error(err)
							}
						}

						payload, err := collector.Resolve()
						if err != nil {
							t.Fatal(err)
						}
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Value()
							if doc == nil {
								t.Fatalf("%T value is nil", doc)
							}
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if fmt.Sprint(doc) != fmt.Sprint(docs[idx]) {
								t.Error("values should be equal")
							}
						}

						if err := iter.Close(ctx); err != nil {
							t.Fatal(err)
						}
						if len(test.dataset) != len(res) {
							t.Fatalf("unqueal %v and %v", len(test.dataset), len(res))
						}
						if !int64SliceEqual(test.dataset, res) {
							t.Error("values should be equal")
						}
					})
				})
			}
			t.Run("SizeMismatch", func(t *testing.T) {
				collector := impl.factory()
				if err := collector.Add(birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5))); err != nil {
					t.Error(err)
				}
				if err := collector.Add(birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5))); err != nil {
					t.Error(err)
				}

				if strings.Contains(impl.name, "Dynamic") {
					if err := collector.Add(birch.DC.Elements(birch.EC.Int64("one", 43))); err != nil {
						t.Error(err)
					}
				} else {
					if err := collector.Add(birch.DC.Elements(birch.EC.Int64("one", 43))); err == nil {
						t.Error("error should be nil")
					}
				}
			})
		})
	}
}

func TestCollectorSizeCap(t *testing.T) {
	for _, test := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{maxDeltas: 1} },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			collector := test.factory()
			if err := collector.Add(birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5))); err != nil {
				t.Error(err)
			}
			if err := collector.Add(birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5))); err != nil {
				t.Error(err)
			}
			if err := collector.Add(birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5))); err == nil {
				t.Error("error should be nil")
			}
		})
	}
}

func TestWriter(t *testing.T) {
	t.Run("NilDocuments", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		_, err := collector.Write(nil)
		if err == nil {
			t.Error("error should not be nil")
		}
		if err := collector.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Run("RealDocument", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		doc, err := birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5)).MarshalBSON()
		if err != nil {
			t.Fatal(err)
		}
		_, err = collector.Write(doc)
		if err != nil {
			t.Error(err)
		}
		if err := collector.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Run("CloseNoError", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		if err := collector.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Run("CloseError", func(t *testing.T) {
		collector := NewWriterCollector(2, &errWriter{})
		doc, err := birch.DC.Elements(birch.EC.Int64("one", 43), birch.EC.Int64("two", 5)).MarshalBSON()
		if err != nil {
			t.Fatal(err)
		}
		_, err = collector.Write(doc)
		if err != nil {
			t.Fatal(err)
		}
		if err := collector.Close(); err == nil {
			t.Error("error should be nil")
		}
	})
}

func TestTimestampHandling(t *testing.T) {
	start := time.Now().Round(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range []struct {
		Name   string
		Values []time.Time
	}{
		{
			Name: "One",
			Values: []time.Time{
				time.Now().Round(time.Millisecond),
			},
		},
		{
			Name: "Same",
			Values: []time.Time{
				start, start, start,
			},
		},
		{
			Name: "SecondSteps",
			Values: []time.Time{
				start.Add(time.Second),
				start.Add(time.Second),
				start.Add(time.Second),
				start.Add(time.Second),
				start.Add(time.Second),
				start.Add(time.Second),
			},
		},
		{
			Name: "HundredMillis",
			Values: []time.Time{
				start.Add(100 * time.Millisecond),
				start.Add(200 * time.Millisecond),
				start.Add(300 * time.Millisecond),
				start.Add(400 * time.Millisecond),
				start.Add(500 * time.Millisecond),
				start.Add(600 * time.Millisecond),
			},
		},
		{
			Name: "TenMillis",
			Values: []time.Time{
				start.Add(10 * time.Millisecond),
				start.Add(20 * time.Millisecond),
				start.Add(30 * time.Millisecond),
				start.Add(40 * time.Millisecond),
				start.Add(50 * time.Millisecond),
				start.Add(60 * time.Millisecond),
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			t.Run("TimeValue", func(t *testing.T) {
				collector := NewBaseCollector(100)
				for _, ts := range test.Values {
					if err := collector.Add(birch.DC.Elements(birch.EC.Time("ts", ts))); err != nil {
						t.Fatal(err)
					}
				}

				out, err := collector.Resolve()
				if err != nil {
					t.Fatal(err)
				}
				t.Run("Structured", func(t *testing.T) {
					iter := ReadStructuredMetrics(ctx, bytes.NewBuffer(out))
					idx := 0
					for iter.Next(ctx) {
						doc := iter.Value()

						val, ok := doc.Lookup("ts").TimeOK()
						if !ok {
							t.Error("expected true")
						} else if test.Values[idx] != val {
							t.Error("values should be equal")
						}

						idx++
					}
					if err := iter.Close(ctx); err != nil {
						t.Fatal(err)
					}
				})
				t.Run("Flattened", func(t *testing.T) {
					iter := ReadMetrics(ctx, bytes.NewBuffer(out))
					idx := 0
					for iter.Next(ctx) {
						doc := iter.Value()
						val, ok := doc.Lookup("ts").TimeOK()
						if !ok {
							t.Error("expected true")
						} else if test.Values[idx] != val {
							t.Fatalf("values are not equal %v and %v", test.Values[idx], val)
						}
						idx++
					}
					if err := iter.Close(ctx); err != nil {
						t.Fatal(err)
					}
				})
				t.Run("Chunks", func(t *testing.T) {
					chunks := ReadChunks(ctx, bytes.NewBuffer(out))
					idx := 0
					for chunks.Next(ctx) {
						chunk := chunks.Value()
						if chunk == nil {
							t.Fatal("'chunk' should not be nil")
						}
						if len(test.Values) != chunk.nPoints {
							t.Error("values should be equal")
						}
						idx++
					}
					if err := chunks.Close(ctx); err != nil {
						t.Fatal(err)
					}
				})

			})
			t.Run("UnixSecond", func(t *testing.T) {
				collector := NewBaseCollector(100)
				for _, ts := range test.Values {
					if err := collector.Add(birch.DC.Elements(birch.EC.Int64("ts", ts.Unix()))); err != nil {
						t.Fatal(err)
					}
				}

				out, err := collector.Resolve()
				if err != nil {
					t.Fatal(err)
				}

				iter := ReadMetrics(ctx, bytes.NewBuffer(out))
				idx := 0
				for iter.Next(ctx) {
					doc := iter.Value()

					val, ok := doc.Lookup("ts").Int64OK()
					if !ok {
						t.Error("expected true")
					} else if test.Values[idx].Unix() != val {
						t.Error("values should be equal")
					}
					idx++
				}
				if err := iter.Close(ctx); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("UnixNano", func(t *testing.T) {
				collector := NewBaseCollector(100)
				for _, ts := range test.Values {
					if err := collector.Add(birch.DC.Elements(birch.EC.Int64("ts", ts.UnixNano()))); err != nil {
						t.Fatal(err)
					}
				}

				out, err := collector.Resolve()
				if err != nil {
					t.Fatal(err)
				}

				iter := ReadMetrics(ctx, bytes.NewBuffer(out))
				idx := 0
				for iter.Next(ctx) {
					doc := iter.Value()

					val, ok := doc.Lookup("ts").Int64OK()
					if !ok {
						t.Error("expected true")
					} else if test.Values[idx].UnixNano() != val {
						t.Error("values should be equal")
					}

					idx++
				}
				if err := iter.Close(ctx); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func int64SliceEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for idx := range a {
		if a[idx] != b[idx] {
			return false
		}
	}
	return true
}
