package ftdc

import (
	"io"
	"strings"
	"time"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/bsontype"
)

// Chunk represents a 'metric chunk' of data in the FTDC.
type Chunk struct {
	Metrics   []Metric
	nPoints   int
	id        time.Time
	metadata  *birch.Document
	reference *birch.Document
}

func (c *Chunk) Metadata() *birch.Document { return c.metadata }
func (c *Chunk) Size() int                 { return c.nPoints }
func (c *Chunk) Len() int                  { return len(c.Metrics) }

// Iterator returns an iterator that you can use to read documents for
// each sample period in the chunk. Documents are returned in collection
// order, with keys flattened and dot-separated fully qualified
// paths.
//
// The documents are constructed from the metrics data lazily.
func (c *Chunk) Iterator() *Iterator[*birch.Document] {
	out := &Iterator[*birch.Document]{}
	out.metasource = c
	out.iterator = c.iteratorFlattened
	return out
}

// StructuredIterator returns the contents of the chunk as a sequence
// of documents that (mostly) resemble the original source documents
// (with the non-metrics fields omitted.) The output documents mirror
// the structure of the input documents.
func (c *Chunk) StructuredIterator() *Iterator[*birch.Document] {
	out := &Iterator[*birch.Document]{}
	out.metasource = c
	out.iterator = c.iterator
	return out
}

func (c *Chunk) exportMatrix() map[string]any {
	out := make(map[string]any)
	for _, m := range c.Metrics {
		out[m.Key()] = m.getSeries()
	}
	return out
}

func (c *Chunk) export() (*birch.Document, error) {
	doc := birch.DC.Make(len(c.Metrics))
	sample := 0

	var elem *birch.Element
	var err error

	for i := 0; i < len(c.Metrics); i++ {
		elem, sample, err = rehydrateMatrix(c.Metrics, sample)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		doc.Append(elem)
	}

	return doc, nil
}

// Metric represents an item in a chunk.
type Metric struct {
	// For metrics that were derived from nested BSON documents,
	// this preserves the path to the field, in support of being
	// able to reconstitute metrics/chunks as a stream of BSON
	// documents.
	ParentPath []string

	// KeyName is the specific field name of a metric in. It is
	// *not* fully qualified with its parent document path, use
	// the Key() method to access a value with more appropriate
	// user facing context.
	KeyName string

	// Values is an array of each value collected for this metric.
	// During decoding, this attribute stores delta-encoded
	// values, but those are expanded during decoding and should
	// never be visible to user.
	Values []int64

	// Used during decoding to expand the delta encoded values. In
	// a properly decoded value, it should always report
	startingValue int64

	originalType bsontype.Type
}

func (m *Metric) Key() string {
	return strings.Join(append(m.ParentPath, m.KeyName), ".")
}

func (m *Metric) getSeries() any {
	switch m.originalType {
	case bsontype.Int64, bsontype.Timestamp:
		out := make([]int64, len(m.Values))
		copy(out, m.Values)
		return out
	case bsontype.Int32:
		out := make([]int32, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = int32(p)
		}
		return out
	case bsontype.Boolean:
		out := make([]bool, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = p != 0
		}
		return out
	case bsontype.Double:
		out := make([]float64, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = restoreFloat(p)
		}
		return out
	case bsontype.DateTime:
		out := make([]time.Time, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = timeEpocMs(p)
		}
		return out
	default:
		return nil
	}
}
