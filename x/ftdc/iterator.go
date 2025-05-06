package ftdc

import (
	"context"
	"io"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/ers"
)

type Iterator struct {
	*fun.Stream[*birch.Document]
	state interface {
		Metadata() *birch.Document
		Close() error
	}
}

func (iter *Iterator) Metadata() *birch.Document { return iter.state.Metadata() }
func (iter *Iterator) Close() error              { return ers.Join(iter.Iterator.Close(), iter.state.Close()) }

// ReadMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator are flattened.
func ReadMetrics(ctx context.Context, r io.Reader) *Iterator {
	pipe := make(chan *birch.Document)
	iterctx, cancel := context.WithCancel(ctx)
	pipeIter := fun.ChannelIterator(pipe)
	citer := &combinedIterator{
		Iterator: pipeIter,
		closer:   cancel,
		flatten:  true,
	}

	citer.wg.Add(1)
	go citer.worker(iterctx, ReadChunks(r), pipe)
	return &Iterator{Iterator: pipeIter, state: citer}
}

// ReadStructuredMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator retain the structure
// of the input documents.
func ReadStructuredMetrics(ctx context.Context, r io.Reader) *Iterator {
	pipe := make(chan *birch.Document)
	iterctx, cancel := context.WithCancel(ctx)

	pipeIter := fun.ChannelIterator(pipe)
	citer := &combinedIterator{
		Iterator: pipeIter,
		closer:   cancel,
		flatten:  false,
	}

	citer.wg.Add(1)
	go citer.worker(iterctx, ReadChunks(r), pipe)
	return &Iterator{Iterator: citer.Iterator, state: citer}
}

// ReadMatrix returns a "matrix format" for the data in a chunk. The
// ducments returned by the iterator represent the entire chunk, in
// flattened form, with each field representing a single metric as an
// array of all values for the event.
//
// The matrix documents have full type fidelity, but are not
// substantially less expensive to produce than full iteration.
func ReadMatrix(ctx context.Context, r io.Reader) *Iterator {
	pipe := make(chan *birch.Document)
	iterctx, cancel := context.WithCancel(ctx)

	miter := &matrixIterator{
		Iterator: fun.ChannelIterator(pipe),
		closer:   cancel,
		chunks:   ReadChunks(r),
	}
	miter.wg.Add(1)
	go miter.worker(iterctx, pipe)
	return &Iterator{Iterator: miter.Iterator, state: miter}
}

// ReadSeries is similar to the ReadMatrix format, and produces a
// single document per chunk, that contains the flattented keys for
// that chunk, mapped to arrays of all the values of the chunk.
//
// The matrix documents have better type fidelity than raw chunks but
// do not properly collapse the bson timestamp type. To use these
// values produced by the iterator, consider marshaling them directly
// to map[string]any and use a case statement, on the values
// in the map, such as:
//
//	switch v.(type) {
//	case []int32:
//	       // ...
//	case []int64:
//	       // ...
//	case []bool:
//	       // ...
//	case []time.Time:
//	       // ...
//	case []float64:
//	       // ...
//	}
//
// Although the *birch.Document type does support iteration directly.
func ReadSeries(ctx context.Context, r io.Reader) *Iterator {
	pipe := make(chan *birch.Document, 25)
	iterctx, cancel := context.WithCancel(ctx)
	iter := &matrixIterator{
		Iterator: fun.ChannelIterator(pipe),
		closer:   cancel,
		chunks:   ReadChunks(r),
		reflect:  true,
	}

	iter.wg.Add(1)
	go iter.worker(iterctx, pipe)
	return &Iterator{Iterator: iter.Iterator, state: iter}
}
