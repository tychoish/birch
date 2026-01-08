package ftdc

import (
	"io"
	"iter"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/x/ftdc/util"
	"github.com/tychoish/fun/adt"
	"github.com/tychoish/fun/erc"
)

type Iterator[T any] struct {
	iterator   func() iter.Seq[T]
	catcher    erc.Collector
	metasource interface {
		Metadata() *birch.Document
	}
}

func (iter *Iterator[T]) Iterator() iter.Seq[T]     { return iter.iterator() }
func (iter *Iterator[T]) Metadata() *birch.Document { return iter.metasource.Metadata() }
func (iter *Iterator[T]) Close() error              { return iter.catcher.Resolve() }

// ReadMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator are flattened.
func ReadMetrics(r io.Reader) *Iterator[*birch.Document] {
	chunks := ReadChunks(r)
	out := &Iterator[*birch.Document]{}
	out.metasource = chunks
	out.iterator = func() iter.Seq[*birch.Document] {
		return func(yield func(*birch.Document) bool) {
			defer func() { out.catcher.Push(chunks.Close()) }()
			for chunk := range chunks.Iterator() {
				for doc := range chunk.iteratorFlattened() {
					if !yield(doc) {
						return
					}
				}
			}
		}
	}
	return out
}

// ReadStructuredMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator retain the structure
// of the input documents.
func ReadStructuredMetrics(r io.Reader) *Iterator[*birch.Document] {
	chunks := ReadChunks(r)
	out := &Iterator[*birch.Document]{}
	out.metasource = chunks
	out.iterator = func() iter.Seq[*birch.Document] {
		return func(yield func(*birch.Document) bool) {
			defer func() { out.catcher.Push(chunks.Close()) }()
			for chunk := range chunks.Iterator() {
				for doc := range chunk.iterator() {
					if !yield(doc) {
						return
					}
				}
			}
		}
	}
	return out
}

// ReadMatrix returns a "matrix format" for the data in a chunk. The
// ducments returned by the iterator represent the entire chunk, in
// flattened form, with each field representing a single metric as an
// array of all values for the event.
//
// The matrix documents have full type fidelity, but are not
// substantially less expensive to produce than full iteration.
func ReadMatrix(r io.Reader) *Iterator[*birch.Document] {
	chunks := ReadChunks(r)
	out := &Iterator[*birch.Document]{}
	out.metasource = chunks

	out.iterator = func() iter.Seq[*birch.Document] {
		return func(yield func(*birch.Document) bool) {
			defer func() { out.catcher.Push(chunks.Close()) }()
			for chunk := range chunks.Iterator() {
				doc, err := chunk.export()
				if err != nil {
					out.catcher.Push(err)
					continue
				}

				if !yield(doc) {
					return
				}
			}
		}
	}
	return out
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
func ReadSeries(r io.Reader) *Iterator[*birch.Document] {
	chunks := ReadChunks(r)
	out := &Iterator[*birch.Document]{}
	out.metasource = chunks

	out.iterator = func() iter.Seq[*birch.Document] {
		return func(yield func(*birch.Document) bool) {
			defer func() { out.catcher.Push(chunks.Close()) }()
			for chunk := range chunks.Iterator() {
				payload, err := util.GlobalMarshaler()(chunk.exportMatrix())
				if err != nil {
					out.catcher.Push(err)
					continue
				}
				doc, err := birch.ReadDocument(payload)
				if err != nil {
					out.catcher.Push(err)
					continue
				}
				if !yield(doc) {
					return
				}
			}
		}
	}
	return out
}

type metasourceImpl struct {
	inner adt.Atomic[*birch.Document]
}

func (msi *metasourceImpl) Metadata() *birch.Document { return msi.inner.Load() }
