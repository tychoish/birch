package ftdc

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

type customCollector struct {
	name      string
	factory   func() Collector
	skipBench bool
}

type customTest struct {
	name      string
	docs      []*bsonx.Document
	numStats  int
	randStats bool
	skipBench bool
}

func createEventRecord(count, duration, size, workers int64) *bsonx.Document {
	return bsonx.NewDocument(
		bsonx.EC.Int64("count", count),
		bsonx.EC.Int64("duration", duration),
		bsonx.EC.Int64("size", size),
		bsonx.EC.Int64("workers", workers),
	)
}

func randStr() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func randFlatDocument(numKeys int) *bsonx.Document {
	doc := bsonx.NewDocument()
	for i := 0; i < numKeys; i++ {
		doc.Append(bsonx.EC.Int64(fmt.Sprint(i), rand.Int63n(int64(numKeys)*1)))
	}

	return doc
}

func newChunk(num int64) []byte {
	collector := NewBaseCollector(int(num) * 2)
	for i := int64(0); i < num; i++ {
		doc := createEventRecord(i, i+rand.Int63n(num-1), i+i*rand.Int63n(num-1), 1)
		doc.Append(bsonx.EC.Time("time", time.Now().Add(time.Duration(i)*time.Hour)))
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
		doc.Append(bsonx.EC.Time("time", time.Now().Add(time.Duration(i)*time.Hour)))
		err := collector.Add(doc)
		grip.EmergencyPanic(err)
	}
	for i := int64(0); i < num; i++ {
		doc := createEventRecord(i, i+rand.Int63n(num-1), i+i*rand.Int63n(num-1), 1)
		doc.Append(
			bsonx.EC.Time("time", time.Now().Add(time.Duration(i)*time.Hour)),
			bsonx.EC.Int64("addition", i+i))
		err := collector.Add(doc)
		grip.EmergencyPanic(err)
	}

	out, err := collector.Resolve()
	grip.EmergencyPanic(err)

	return out

}

func randFlatDocumentWithFloats(numKeys int) *bsonx.Document {
	doc := bsonx.NewDocument()
	for i := 0; i < numKeys; i++ {
		doc.Append(bsonx.EC.Double(fmt.Sprintf("%d_float", i), rand.Float64()))
		doc.Append(bsonx.EC.Int64(fmt.Sprintf("%d_long", i), rand.Int63()))
	}
	return doc
}

func randComplexDocument(numKeys, otherNum int) *bsonx.Document {
	doc := bsonx.NewDocument()

	for i := 0; i < numKeys; i++ {
		doc.Append(bsonx.EC.Int64(fmt.Sprintln(numKeys, otherNum), rand.Int63n(int64(numKeys)*1)))

		if otherNum%5 == 0 {
			ar := bsonx.NewArray()
			for ii := int64(0); i < otherNum; i++ {
				ar.Append(bsonx.VC.Int64(rand.Int63n(1 + ii*int64(numKeys))))
			}
			doc.Append(bsonx.EC.Array(fmt.Sprintln("first", numKeys, otherNum), ar))
		}

		if otherNum%3 == 0 {
			doc.Append(bsonx.EC.SubDocument(fmt.Sprintln("second", numKeys, otherNum), randFlatDocument(otherNum)))
		}

		if otherNum%12 == 0 {
			doc.Append(bsonx.EC.SubDocument(fmt.Sprintln("third", numKeys, otherNum), randComplexDocument(otherNum, 10)))
		}
	}

	return doc
}

func createCollectors() []*customCollector {
	collectors := []*customCollector{
		{
			name:    "Better",
			factory: func() Collector { return NewBaseCollector(1000) },
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
			factory:   func() Collector { return NewStreamingCollector(10, &bytes.Buffer{}) },
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
			name:      "SmallStreamingDynamic",
			factory:   func() Collector { return NewStreamingDynamicCollector(10, &bytes.Buffer{}) },
			skipBench: true,
		},
		{
			name:    "MediumStreamingDynamic",
			factory: func() Collector { return NewStreamingDynamicCollector(1000, &bytes.Buffer{}) },
		},
		{
			name:    "LargeStreamingDynamic",
			factory: func() Collector { return NewStreamingDynamicCollector(10000, &bytes.Buffer{}) },
		},
	}
	return collectors
}

func createTests() []customTest {
	return []customTest{
		{
			name: "OneDocNoStats",
			docs: []*bsonx.Document{
				bsonx.NewDocument(bsonx.EC.String("foo", "bar")),
			},
			skipBench: true,
		},
		{
			name: "OneDocumentOneStat",
			docs: []*bsonx.Document{
				bsonx.NewDocument(bsonx.EC.Int32("foo", 42)),
			},
			skipBench: true,
			numStats:  1,
		},
		{
			name: "OneSmallFlat",
			docs: []*bsonx.Document{
				randFlatDocument(12),
			},
			numStats:  12,
			skipBench: true,
		},
		{
			name: "OneLargeFlat",
			docs: []*bsonx.Document{
				randFlatDocument(360),
			},
			numStats:  360,
			skipBench: true,
		},
		{
			name: "OneHugeFlat",
			docs: []*bsonx.Document{
				randFlatDocument(36000),
			},
			numStats:  36000,
			skipBench: true,
		},
		{
			name: "SeveralDocNoStats",
			docs: []*bsonx.Document{
				bsonx.NewDocument(bsonx.EC.String("foo", "bar")),
				bsonx.NewDocument(bsonx.EC.String("foo", "bar")),
				bsonx.NewDocument(bsonx.EC.String("foo", "bar")),
				bsonx.NewDocument(bsonx.EC.String("foo", "bar")),
			},
			skipBench: true,
		},
		{
			name: "SeveralDocumentOneStat",
			docs: []*bsonx.Document{
				bsonx.NewDocument(bsonx.EC.Int32("foo", 42)),
				bsonx.NewDocument(bsonx.EC.Int32("foo", 42)),
				bsonx.NewDocument(bsonx.EC.Int32("foo", 42)),
				bsonx.NewDocument(bsonx.EC.Int32("foo", 42)),
				bsonx.NewDocument(bsonx.EC.Int32("foo", 42)),
			},
			numStats:  1,
			skipBench: true,
		},
		{
			name: "SeveralSmallFlat",
			docs: []*bsonx.Document{
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
			docs: []*bsonx.Document{
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
			docs: []*bsonx.Document{
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
			docs: []*bsonx.Document{
				randComplexDocument(4, 10),
			},
			randStats: true,
			numStats:  11,
			skipBench: true,
		},
		{
			name: "OneLargeComplex",
			docs: []*bsonx.Document{
				randComplexDocument(100, 100),
			},
			randStats: true,
			skipBench: true,
			numStats:  101,
		},
		{
			name: "SeveralSmallComplex",
			docs: []*bsonx.Document{
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
			docs: []*bsonx.Document{
				randComplexDocument(10000, 10000),
			},
			randStats: true,
			numStats:  1000,
			skipBench: true,
		},
		{
			name: "SeveralHugeComplex",
			docs: []*bsonx.Document{
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
			docs: []*bsonx.Document{
				randFlatDocumentWithFloats(1),
				randFlatDocumentWithFloats(1),
			},
			skipBench: true,
			randStats: true,
			numStats:  2,
		},
		{
			name: "MultiFloats",
			docs: []*bsonx.Document{
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
	doc *bsonx.Document
}

func (m *marshaler) MarshalBSON() ([]byte, error) {
	if m.doc == nil {
		return nil, errors.New("empty")
	}
	return m.doc.MarshalBSON()
}
