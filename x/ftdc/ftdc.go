package ftdc

import (
	"context"
	"strings"
	"time"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/bsontype"
	"github.com/tychoish/fun"
)

// Chunk represents a 'metric chunk' of data in the FTDC.
type Chunk struct {
	Metrics   []Metric
	nPoints   int
	id        time.Time
	metadata  *birch.Document
	reference *birch.Document
}

func (c *Chunk) GetMetadata() *birch.Document { return c.metadata }
func (c *Chunk) Size() int                    { return c.nPoints }
func (c *Chunk) Len() int                     { return len(c.Metrics) }

// Iterator returns an iterator that you can use to read documents for
// each sample period in the chunk. Documents are returned in collection
// order, with keys flattened and dot-separated fully qualified
// paths.
//
// The documents are constructed from the metrics data lazily.
func (c *Chunk) Iterator(ctx context.Context) *Iterator {
	sctx, cancel := context.WithCancel(ctx)
	pipe := make(chan *birch.Document)
	iter := &sampleIterator{
		Stream:   fun.ChannelStream(pipe),
		closer:   cancel,
		metadata: c.GetMetadata(),
	}
	iter.wg.Add(1)
	go func() {
		defer iter.wg.Done()
		defer close(pipe)
		c.streamFlattenedDocuments(sctx, pipe)
	}()

	return &Iterator{
		Stream: iter.Stream,
		state:  iter,
	}
}

// StructuredIterator returns the contents of the chunk as a sequence
// of documents that (mostly) resemble the original source documents
// (with the non-metrics fields omitted.) The output documents mirror
// the structure of the input documents.
func (c *Chunk) StructuredIterator(ctx context.Context) *Iterator {
	sctx, cancel := context.WithCancel(ctx)
	pipe := make(chan *birch.Document)
	iter := &sampleIterator{
		Stream:   fun.ChannelStream(pipe),
		closer:   cancel,
		metadata: c.GetMetadata(),
	}
	iter.wg.Add(1)
	go func() {
		defer iter.wg.Done()
		defer close(pipe)
		c.streamDocuments(sctx, pipe)
	}()
	return &Iterator{
		Stream: iter.Stream,
		state:  iter,
	}
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
