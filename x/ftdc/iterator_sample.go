package ftdc

import (
	"context"
	"sync"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun"
)

// sampleIterator provides an iterator for iterating through the
// results of a FTDC data chunk as BSON documents.
type sampleIterator struct {
	fun.Iterator[*birch.Document]
	closer   context.CancelFunc
	metadata *birch.Document
	wg       sync.WaitGroup
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
		doc, _ := restoreDocument(ctx, c.reference, i, c.Metrics, 0)
		select {
		case <-ctx.Done():
			return
		case out <- doc:
			continue
		}
	}
}

// Close releases all resources associated with the iterator.
func (iter *sampleIterator) Close() error {
	iter.closer()
	iter.wg.Wait()

	return iter.Iterator.Close()
}

func (iter *sampleIterator) Metadata() *birch.Document { return iter.metadata }
