package ftdc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/evergreen-ci/birch"
	"github.com/evergreen-ci/birch/bsontype"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

type customCollector struct {
	name         string
	factory      func() Collector
	uncompressed bool
	skipBench    bool
}

type customTest struct {
	name      string
	docs      []*birch.Document
	numStats  int
	randStats bool
	skipBench bool
}

func createEventRecord(count, duration, size, workers int64) *birch.Document {
	return birch.NewDocument(
		birch.EC.Int64("count", count),
		birch.EC.Int64("duration", duration),
		birch.EC.Int64("size", size),
		birch.EC.Int64("workers", workers),
	)
}

func randFlatDocument(numKeys int) *birch.Document {
	doc := birch.NewDocument()
	for i := 0; i < numKeys; i++ {
		doc.Append(birch.EC.Int64(fmt.Sprint(i), rand.Int63n(int64(numKeys)*1)))
	}

	return doc
}

func newChunk(num int64) []byte {
	collector := NewBaseCollector(int(num) * 2)
	for i := int64(0); i < num; i++ {
		doc := createEventRecord(i, i+rand.Int63n(num-1), i+i*rand.Int63n(num-1), 1)
		doc.Append(birch.EC.Time("time", time.Now().Add(time.Duration(i)*time.Hour)))
		err := collector.Add(doc)
		grip.EmergencyPanic(err)
	}

	out, err := collector.Resolve()
	grip.EmergencyPanic(err)

	return out
}

func newMixedChunk(num int64) []byte {
	collector := NewDynamicCollector(int(num) * 2)
	for i := int64(0); i < num; i++ {
		doc := createEventRecord(i, i+rand.Int63n(num-1), i+i*rand.Int63n(num-1), 1)
		doc.Append(birch.EC.Time("time", time.Now().Add(time.Duration(i)*time.Hour)))
		err := collector.Add(doc)
		grip.EmergencyPanic(err)
	}
	for i := int64(0); i < num; i++ {
		doc := createEventRecord(i, i+rand.Int63n(num-1), i+i*rand.Int63n(num-1), 1)
		doc.Append(
			birch.EC.Time("time", time.Now().Add(time.Duration(i)*time.Hour)),
			birch.EC.Int64("addition", i+i))
		err := collector.Add(doc)
		grip.EmergencyPanic(err)
	}

	out, err := collector.Resolve()
	grip.EmergencyPanic(err)

	return out

}

func produceMockChunkIter(ctx context.Context, samples int, newDoc func() *birch.Document) *ChunkIterator {
	collector := NewBaseCollector(samples)
	for i := 0; i < samples; i++ {
		if err := collector.Add(newDoc()); err != nil {
			panic(err)
		}
	}
	payload, err := collector.Resolve()
	if err != nil {
		panic(err)
	}

	return ReadChunks(ctx, bytes.NewBuffer(payload))

}

func produceMockMetrics(ctx context.Context, samples int, newDoc func() *birch.Document) []Metric {
	iter := produceMockChunkIter(ctx, samples, newDoc)

	if !iter.Next() {
		panic("could not iterate")
	}

	metrics := iter.Chunk().Metrics
	iter.Close()
	return metrics
}

func randFlatDocumentWithFloats(numKeys int) *birch.Document {
	doc := birch.NewDocument()
	for i := 0; i < numKeys; i++ {
		doc.Append(birch.EC.Double(fmt.Sprintf("%d_float", i), rand.Float64()))
		doc.Append(birch.EC.Int64(fmt.Sprintf("%d_long", i), rand.Int63()))
	}
	return doc
}

func randComplexDocument(numKeys, otherNum int) *birch.Document {
	doc := birch.NewDocument()

	for i := 0; i < numKeys; i++ {
		doc.Append(birch.EC.Int64(fmt.Sprintln(numKeys, otherNum), rand.Int63n(int64(numKeys)*1)))
		doc.Append(birch.EC.Double(fmt.Sprintln("float", numKeys, otherNum), rand.Float64()))

		if otherNum%5 == 0 {
			ar := birch.NewArray()
			for ii := int64(0); i < otherNum; i++ {
				ar.Append(birch.VC.Int64(rand.Int63n(1 + ii*int64(numKeys))))
			}
			doc.Append(birch.EC.Array(fmt.Sprintln("first", numKeys, otherNum), ar))
		}

		if otherNum%3 == 0 {
			doc.Append(birch.EC.SubDocument(fmt.Sprintln("second", numKeys, otherNum), randFlatDocument(otherNum)))
		}

		if otherNum%12 == 0 {
			doc.Append(birch.EC.SubDocument(fmt.Sprintln("third", numKeys, otherNum), randComplexDocument(otherNum, 10)))
		}
	}

	return doc
}
func isMetricsDocument(key string, doc *birch.Document) ([]string, int) {
	iter := doc.Iterator()
	keys := []string{}
	seen := 0
	for iter.Next() {
		elem := iter.Element()
		k, num := isMetricsValue(fmt.Sprintf("%s/%s", key, elem.Key()), elem.Value())
		if num > 0 {
			seen += num
			keys = append(keys, k...)
		}
	}

	return keys, seen
}

func isMetricsArray(key string, array *birch.Array) ([]string, int) {
	idx := 0
	numKeys := 0
	keys := []string{}
	iter := array.Iterator()
	for iter.Next() {
		ks, num := isMetricsValue(key+strconv.Itoa(idx), iter.Value())

		if num > 0 {
			numKeys += num
			keys = append(keys, ks...)
		}

		idx++
	}

	return keys, numKeys
}

func isMetricsValue(key string, val *birch.Value) ([]string, int) {
	switch val.Type() {
	case bsontype.ObjectID:
		return nil, 0
	case bsontype.String:
		return nil, 0
	case bsontype.Decimal128:
		return nil, 0
	case bsontype.Array:
		return isMetricsArray(key, val.MutableArray())
	case bsontype.EmbeddedDocument:
		return isMetricsDocument(key, val.MutableDocument())
	case bsontype.Boolean:
		return []string{key}, 1
	case bsontype.Double:
		return []string{key}, 1
	case bsontype.Int32:
		return []string{key}, 1
	case bsontype.Int64:
		return []string{key}, 1
	case bsontype.DateTime:
		return []string{key}, 1
	case bsontype.Timestamp:
		return []string{key}, 2
	default:
		return nil, 0
	}
}

func createCollectors(ctx context.Context) []*customCollector {
	collectors := []*customCollector{
		{
			name:    "Better",
			factory: func() Collector { return NewBaseCollector(1000) },
		},
		{
			name:    "Buffered",
			factory: func() Collector { return NewBufferedCollector(ctx, 0, NewBaseCollector(1000)) },
		},
		{
			name:      "SmallBatch",
			factory:   func() Collector { return NewBatchCollector(10) },
			skipBench: true,
		},
		{
			name:      "MediumBatch",
			factory:   func() Collector { return NewBatchCollector(100) },
			skipBench: true,
		},
		{
			name:      "LargeBatch",
			factory:   func() Collector { return NewBatchCollector(1000) },
			skipBench: true,
		},
		{
			name:      "XtraLargeBatch",
			factory:   func() Collector { return NewBatchCollector(10000) },
			skipBench: true,
		},
		{
			name:      "SmallDynamic",
			factory:   func() Collector { return NewDynamicCollector(10) },
			skipBench: true,
		},
		{
			name:    "MediumDynamic",
			factory: func() Collector { return NewDynamicCollector(100) },
		},
		{
			name:    "LargeDynamic",
			factory: func() Collector { return NewDynamicCollector(1000) },
		},
		{
			name:      "XtraLargeDynamic",
			factory:   func() Collector { return NewDynamicCollector(10000) },
			skipBench: true,
		},
		{
			name:      "SampleBasic",
			factory:   func() Collector { return NewSamplingCollector(0, &betterCollector{maxDeltas: 100}) },
			skipBench: true,
		},
		{
			name:      "SmallStreaming",
			factory:   func() Collector { return NewStreamingCollector(100, &bytes.Buffer{}) },
			skipBench: true,
		},
		{
			name:    "MediumStreaming",
			factory: func() Collector { return NewStreamingCollector(1000, &bytes.Buffer{}) },
		},
		{
			name:    "LargeStreaming",
			factory: func() Collector { return NewStreamingCollector(10000, &bytes.Buffer{}) },
		},
		{
			name:    "SmallStreamingDynamic",
			factory: func() Collector { return NewStreamingDynamicCollector(100, &bytes.Buffer{}) },
		},
		{
			name:    "MediumStreamingDynamic",
			factory: func() Collector { return NewStreamingDynamicCollector(1000, &bytes.Buffer{}) },
		},
		{
			name:    "LargeStreamingDynamic",
			factory: func() Collector { return NewStreamingDynamicCollector(10000, &bytes.Buffer{}) },
		},
		{
			name:         "UncompressedSmallJSON",
			factory:      func() Collector { return NewUncompressedCollectorJSON(10) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedMediumJSON",
			factory:      func() Collector { return NewUncompressedCollectorJSON(100) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedLargeJSON",
			factory:      func() Collector { return NewUncompressedCollectorJSON(1000) },
			uncompressed: true,
		},
		{
			name:         "UncompressedSmallBSON",
			factory:      func() Collector { return NewUncompressedCollectorBSON(10) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedMediumBSON",
			factory:      func() Collector { return NewUncompressedCollectorBSON(100) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedLargeBSON",
			factory:      func() Collector { return NewUncompressedCollectorBSON(1000) },
			uncompressed: true,
		},
		{
			name:         "UncompressedStreamingSmallJSON",
			factory:      func() Collector { return NewStreamingUncompressedCollectorJSON(10, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingMediumJSON",
			factory:      func() Collector { return NewStreamingUncompressedCollectorJSON(100, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingLargeJSON",
			factory:      func() Collector { return NewStreamingUncompressedCollectorJSON(1000, &bytes.Buffer{}) },
			uncompressed: true,
		},
		{
			name:         "UncompressedStreamingSmallBSON",
			factory:      func() Collector { return NewStreamingUncompressedCollectorBSON(10, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingMediumBSON",
			factory:      func() Collector { return NewStreamingUncompressedCollectorBSON(100, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingLargeBSON",
			factory:      func() Collector { return NewStreamingUncompressedCollectorBSON(1000, &bytes.Buffer{}) },
			uncompressed: true,
		},
		{
			name:         "UncompressedStreamingDynamicSmallJSON",
			factory:      func() Collector { return NewStreamingDynamicUncompressedCollectorJSON(10, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingDynamicMediumJSON",
			factory:      func() Collector { return NewStreamingDynamicUncompressedCollectorJSON(100, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingDynamicLargeJSON",
			factory:      func() Collector { return NewStreamingDynamicUncompressedCollectorJSON(1000, &bytes.Buffer{}) },
			uncompressed: true,
		},
		{
			name:         "UncompressedStreamingDynamicSmallBSON",
			factory:      func() Collector { return NewStreamingDynamicUncompressedCollectorBSON(10, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingDynamicMediumBSON",
			factory:      func() Collector { return NewStreamingDynamicUncompressedCollectorBSON(100, &bytes.Buffer{}) },
			uncompressed: true,
			skipBench:    true,
		},
		{
			name:         "UncompressedStreamingDynamicLargeBSON",
			factory:      func() Collector { return NewStreamingDynamicUncompressedCollectorBSON(1000, &bytes.Buffer{}) },
			uncompressed: true,
		},
	}
	return collectors
}

func createTests() []customTest {
	return []customTest{
		{
			name: "OneDocNoStats",
			docs: []*birch.Document{
				birch.NewDocument(birch.EC.String("foo", "bar")),
			},
			skipBench: true,
		},
		{
			name: "OneDocumentOneStat",
			docs: []*birch.Document{
				birch.NewDocument(birch.EC.Int32("foo", 42)),
			},
			skipBench: true,
			numStats:  1,
		},
		{
			name: "OneSmallFlat",
			docs: []*birch.Document{
				randFlatDocument(12),
			},
			numStats:  12,
			skipBench: true,
		},
		{
			name: "OneLargeFlat",
			docs: []*birch.Document{
				randFlatDocument(360),
			},
			numStats:  360,
			skipBench: true,
		},
		{
			name: "OneHugeFlat",
			docs: []*birch.Document{
				randFlatDocument(36000),
			},
			numStats:  36000,
			skipBench: true,
		},
		{
			name: "SeveralDocNoStats",
			docs: []*birch.Document{
				birch.NewDocument(birch.EC.String("foo", "bar")),
				birch.NewDocument(birch.EC.String("foo", "bar")),
				birch.NewDocument(birch.EC.String("foo", "bar")),
				birch.NewDocument(birch.EC.String("foo", "bar")),
			},
			skipBench: true,
		},
		{
			name: "SeveralDocumentOneStat",
			docs: []*birch.Document{
				birch.NewDocument(birch.EC.Int32("foo", 42)),
				birch.NewDocument(birch.EC.Int32("foo", 42)),
				birch.NewDocument(birch.EC.Int32("foo", 42)),
				birch.NewDocument(birch.EC.Int32("foo", 42)),
				birch.NewDocument(birch.EC.Int32("foo", 42)),
			},
			numStats:  1,
			skipBench: true,
		},
		{
			name: "SeveralSmallFlat",
			docs: []*birch.Document{
				randFlatDocument(10),
				randFlatDocument(10),
				randFlatDocument(10),
				randFlatDocument(10),
				randFlatDocument(10),
				randFlatDocument(10),
				randFlatDocument(10),
				randFlatDocument(10),
			},
			randStats: true,
			numStats:  10,
		},
		{
			name: "SeveralLargeFlat",
			docs: []*birch.Document{
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
				randFlatDocument(200),
			},
			randStats: true,
			numStats:  200,
		},
		{
			name: "SeveralHugeFlat",
			docs: []*birch.Document{
				randFlatDocument(2000),
				randFlatDocument(2000),
				randFlatDocument(2000),
				randFlatDocument(2000),
			},
			randStats: true,
			skipBench: true,
			numStats:  2000,
		},
		{
			name: "OneSmallComplex",
			docs: []*birch.Document{
				randComplexDocument(4, 10),
			},
			randStats: true,
			numStats:  11,
			skipBench: true,
		},
		{
			name: "OneLargeComplex",
			docs: []*birch.Document{
				randComplexDocument(100, 100),
			},
			randStats: true,
			skipBench: true,
			numStats:  101,
		},
		{
			name: "SeveralSmallComplex",
			docs: []*birch.Document{
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
				randComplexDocument(4, 100),
			},
			numStats:  101,
			randStats: true,
		},
		{
			name: "OneHugeComplex",
			docs: []*birch.Document{
				randComplexDocument(10000, 10000),
			},
			randStats: true,
			numStats:  1000,
			skipBench: true,
		},
		{
			name: "SeveralHugeComplex",
			docs: []*birch.Document{
				randComplexDocument(10000, 10000),
				randComplexDocument(10000, 10000),
				randComplexDocument(10000, 10000),
				randComplexDocument(10000, 10000),
				randComplexDocument(10000, 10000),
			},
			randStats: true,
			skipBench: true,
			numStats:  1000,
		},
		{
			name: "SingleFloats",
			docs: []*birch.Document{
				randFlatDocumentWithFloats(1),
				randFlatDocumentWithFloats(1),
			},
			skipBench: true,
			randStats: true,
			numStats:  2,
		},
		{
			name: "MultiFloats",
			docs: []*birch.Document{
				randFlatDocumentWithFloats(50),
				randFlatDocumentWithFloats(50),
			},
			randStats: true,
			skipBench: true,
			numStats:  100,
		},
	}
}

type encodingTests struct {
	name    string
	dataset []int64
}

func createEncodingTests() []encodingTests {
	return []encodingTests{
		{
			name:    "SingleElement",
			dataset: []int64{1},
		},
		{
			name:    "BasicTwoElementIncrease",
			dataset: []int64{23, 24},
		},
		{
			name:    "BasicThreeElementIncrease",
			dataset: []int64{24, 25, 26},
		},
		{
			name:    "BasicTwoElementDecrease",
			dataset: []int64{26, 25},
		},
		{
			name:    "BasicThreeElementDecrease",
			dataset: []int64{24, 23, 22},
		},
		{
			name:    "BasicFourElementDecrease",
			dataset: []int64{24, 23, 22, 21},
		},
		{
			name:    "IncByTens",
			dataset: []int64{20, 30, 40, 50, 60, 70},
		},
		{
			name:    "DecByTens",
			dataset: []int64{100, 90, 80, 70, 60, 50},
		},
		{
			name:    "ClimbAndDecend",
			dataset: []int64{25, 50, 75, 100, 75, 50, 25, 0},
		},
		{
			name: "ClimbAndDecendTwice",
			dataset: []int64{
				25, 50, 75, 100, 75, 50, 25, 0,
				25, 50, 75, 100, 75, 50, 25, 0,
			},
		},
		{
			name:    "RegularGaps",
			dataset: []int64{25, 50, 75, 100},
		},
		{
			name:    "RegularGapsDec",
			dataset: []int64{100, 75, 50, 25, 0},
		},
		{
			name:    "ThreeElementIncreaseJump",
			dataset: []int64{24, 25, 100},
		},
		{
			name:    "Common",
			dataset: []int64{1, 32, 64, 25, 42, 42, 6, 3},
		},
		{
			name:    "CommonWithZeros",
			dataset: []int64{32, 1, 0, 0, 25, 42, 42, 6, 3},
		},
		{
			name:    "CommonEndsWithZero",
			dataset: []int64{32, 1, 0, 0, 25, 42, 42, 6, 3, 0},
		},
		{
			name:    "CommonWithOutZeros",
			dataset: []int64{32, 1, 25, 42, 42, 6, 3},
		},
		{
			name:    "SingleZero",
			dataset: []int64{0},
		},
		{
			name:    "SeriesStartsWithNegatives",
			dataset: []int64{-1, -2, -43, -72, -100, 200, 0, 0, 0},
		},
		{
			name:    "SingleNegativeOne",
			dataset: []int64{-1},
		},
		{
			name:    "SingleNegativeRandSmall",
			dataset: []int64{-rand.Int63n(10)},
		},
		{
			name:    "SingleNegativeRandLarge",
			dataset: []int64{-rand.Int63()},
		},
		{
			name:    "OnlyZeros",
			dataset: []int64{0, 0, 0, 0},
		},
		{
			name:    "AllOnes",
			dataset: []int64{1, 1, 1, 1, 1, 1},
		},
		{
			name:    "AllNegativeOnes",
			dataset: []int64{-1, -1, -1, -1, -1, -1},
		},
		{
			name:    "AllFortyTwo",
			dataset: []int64{42, 42, 42, 42, 42},
		},
		{
			name:    "SmallRandoms",
			dataset: []int64{rand.Int63n(100), rand.Int63n(100), rand.Int63n(100), rand.Int63n(100)},
		},
		{
			name:    "SmallIncreases",
			dataset: []int64{1, 2, 3, 4, 5, 6, 7},
		},
		{
			name:    "SmallIncreaseStall",
			dataset: []int64{1, 2, 2, 2, 2, 3},
		},
		{
			name:    "SmallDecreases",
			dataset: []int64{10, 9, 8, 7, 6, 5, 4, 3, 2},
		},
		{
			name:    "SmallDecreasesStall",
			dataset: []int64{10, 9, 9, 9, 9},
		},
		{
			name:    "SmallRandSomeNegatives",
			dataset: []int64{rand.Int63n(100), -1 * rand.Int63n(100), rand.Int63n(100), -1 * rand.Int63n(100)},
		},
	}
}

type noopWriter struct {
	bytes.Buffer
}

func (n *noopWriter) Write(in []byte) (int, error) { return n.Buffer.Write(in) }
func (n *noopWriter) Close() error                 { return nil }

type errWriter struct {
	bytes.Buffer
}

func (n *errWriter) Write(in []byte) (int, error) { return 0, errors.New("foo") }
func (n *errWriter) Close() error                 { return errors.New("close") }

type marshaler struct {
	doc *birch.Document
}

func (m *marshaler) MarshalBSON() ([]byte, error) {
	if m.doc == nil {
		return nil, errors.New("empty")
	}
	return m.doc.MarshalBSON()
}
