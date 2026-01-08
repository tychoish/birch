package ftdc

import (
	"context"
	"io"
	"iter"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun/fnx"
	"github.com/tychoish/fun/stw"
)

// ReadChunks creates a ChunkIterator from an underlying FTDC data
// source.
func ReadChunks(r io.Reader) *Iterator[*Chunk] {
	pipe := make(chan *Chunk)
	ipc := make(chan *birch.Document)

	out := &Iterator[*Chunk]{}

	ch := stw.ChanBlocking(pipe)
	msi := &metasourceImpl{}
	out.metasource = msi
	out.iterator = func() iter.Seq[*Chunk] {
		wg := &fnx.WaitGroup{}

		setup := fnx.Operation(func(ctx context.Context) {
			wg.Launch(ctx, func(ctx context.Context) { out.catcher.Push(readChunks(ctx, ipc, pipe)) })
			wg.Launch(ctx, func(ctx context.Context) { out.catcher.Push(readDiagnostic(ctx, r, ipc)) })
			wg.Operation().Background(ctx)
			wg.Launch(ctx, func(ctx context.Context) {
				for {
					select {
					case <-ctx.Done():
					case meta := <-ipc:
						msi.inner.Store(meta)
					}
				}
			})
		}).Once()

		return func(yield func(*Chunk) bool) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			setup(ctx)

			for elem := range ch.Receive().Iterator(ctx) {
				if !yield(elem) {
					return
				}
			}
		}
	}
	return out
}

func (c *Chunk) iterator() iter.Seq[*birch.Document] {
	return func(yield func(*birch.Document) bool) {
		for i := 0; i < c.nPoints; i++ {
			doc, _ := restoreDocument(c.reference, i, c.Metrics, 0)
			if !yield(doc) {
				return
			}
		}
	}
}

func (c *Chunk) iteratorFlattened() iter.Seq[*birch.Document] {
	return func(yield func(*birch.Document) bool) {
		for i := 0; i < c.nPoints; i++ {

			doc := birch.DC.Make(len(c.Metrics))
			for _, m := range c.Metrics {
				elem, ok := restoreFlat(m.originalType, m.Key(), m.Values[i])
				if !ok {
					continue
				}

				doc.Append(elem)
			}

			if !yield(doc) {
				return
			}
		}
	}
}

func (c *Chunk) streamFlattenedDocuments(ctx context.Context, out chan *birch.Document) {
	for i := 0; i < c.nPoints; i++ {

		doc := birch.DC.Make(len(c.Metrics))
		for _, m := range c.Metrics {
			elem, ok := restoreFlat(m.originalType, m.Key(), m.Values[i])
			if !ok {
				continue
			}

			doc.Append(elem)
		}

		select {
		case out <- doc:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (c *Chunk) streamDocuments(ctx context.Context, out chan *birch.Document) {
	for i := 0; i < c.nPoints; i++ {
		doc, _ := restoreDocument(c.reference, i, c.Metrics, 0)
		select {
		case <-ctx.Done():
			return
		case out <- doc:
			continue
		}
	}
}
