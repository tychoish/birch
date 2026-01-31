package ftdc

import (
	"context"
	"io"
	"iter"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun/fnx"
)

// ReadChunks creates a ChunkIterator from an underlying FTDC data
// source.
func ReadChunks(r io.Reader) *Iterator[*Chunk] {
	out := &Iterator[*Chunk]{}
	msi := &metasourceImpl{}
	out.metasource = msi
	out.iterator = func() iter.Seq[*Chunk] {
		return func(yield func(*Chunk) bool) {
			pipe := make(chan *Chunk)
			ipc := make(chan *birch.Document)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wg := &fnx.WaitGroup{}
			wg.Launch(ctx, func(ctx context.Context) { out.catcher.Push(readChunks(ctx, msi, ipc, pipe)) })
			wg.Launch(ctx, func(ctx context.Context) { out.catcher.Push(readDiagnostic(ctx, r, ipc)) })

			go func() {
				wg.Wait(ctx)
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case elem, ok := <-pipe:
					if !ok {
						return
					}
					if !yield(elem) {
						return
					}
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
