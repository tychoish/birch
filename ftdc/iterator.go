package ftdc

import (
	"context"
	"io"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/itertool"
)

type Iterator interface {
	fun.Iterator[*birch.Document]
	Metadata() *birch.Document
}

// ReadMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator are flattened.
func ReadMetrics(ctx context.Context, r io.Reader) Iterator {
	pipe := make(chan *birch.Document)
	iterctx, cancel := context.WithCancel(ctx)
	iter := &combinedIterator{
		Iterator: itertool.Channel(pipe),
		closer:   cancel,
		flatten:  true,
	}

	iter.wg.Add(1)
	go iter.worker(iterctx, ReadChunks(iterctx, r), pipe)
	return iter
}

// ReadStructuredMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator retain the structure
// of the input documents.
func ReadStructuredMetrics(ctx context.Context, r io.Reader) Iterator {
	pipe := make(chan *birch.Document)
	iterctx, cancel := context.WithCancel(ctx)
	iter := &combinedIterator{
		Iterator: itertool.Channel(pipe),
		closer:   cancel,
		flatten:  false,
	}

	iter.wg.Add(1)
	go iter.worker(iterctx, ReadChunks(iterctx, r), pipe)
	return iter
}

// ReadMatrix returns a "matrix format" for the data in a chunk. The
// ducments returned by the iterator represent the entire chunk, in
// flattened form, with each field representing a single metric as an
// array of all values for the event.
//
// The matrix documents have full type fidelity, but are not
// substantially less expensive to produce than full iteration.
func ReadMatrix(ctx context.Context, r io.Reader) Iterator {
	pipe := make(chan *birch.Document)
	iterctx, cancel := context.WithCancel(ctx)
	iter := &matrixIterator{
		Iterator: itertool.Channel(pipe),
		closer:   cancel,
		chunks:   ReadChunks(iterctx, r),
	}
	iter.wg.Add(1)
	go iter.worker(iterctx, pipe)
	return iter
}

// ReadSeries is similar to the ReadMatrix format, and produces a
// single document per chunk, that contains the flattented keys for
// that chunk, mapped to arrays of all the values of the chunk.
//
// The matrix documents have better type fidelity than raw chunks but
// do not properly collapse the bson timestamp type. To use these
// values produced by the iterator, consider marshaling them directly
// to map[string]interface{} and use a case statement, on the values
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
func ReadSeries(ctx context.Context, r io.Reader) Iterator {
	pipe := make(chan *birch.Document, 25)
	iterctx, cancel := context.WithCancel(ctx)
	iter := &matrixIterator{
		Iterator: itertool.Channel(pipe),
		closer:   cancel,
		chunks:   ReadChunks(iterctx, r),
		reflect:  true,
	}

	iter.wg.Add(1)
	go iter.worker(iterctx, pipe)
	return iter
}
