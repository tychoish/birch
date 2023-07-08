package ftdc

import (
	"context"
	"errors"
	"sync"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/erc"
)

type combinedIterator struct {
	*fun.Iterator[*birch.Document]
	closer   context.CancelFunc
	metadata *birch.Document
	document *birch.Document
	wg       sync.WaitGroup
	catcher  erc.Collector
	flatten  bool
}

func (iter *combinedIterator) Close() error {
	iter.closer()
	iter.catcher.Add(iter.Iterator.Close())
	iter.wg.Wait()
	return iter.catcher.Resolve()
}

func (iter *combinedIterator) Metadata() *birch.Document { return iter.metadata }

func (iter *combinedIterator) worker(
	ctx context.Context,
	chunks *fun.Iterator[*Chunk],
	pipe chan *birch.Document,
) {
	defer iter.wg.Done()
	defer close(pipe)

	for chunks.Next(ctx) {
		chunk := chunks.Value()

		var sample *Iterator
		if iter.flatten {
			sample = chunk.Iterator(ctx)
		} else {
			sample = chunk.StructuredIterator(ctx)
		}

		if iter.metadata != nil {
			iter.metadata = chunk.GetMetadata()
		}

		for sample.Next(ctx) {
			select {
			case pipe <- sample.Value():
				continue
			case <-ctx.Done():
				iter.catcher.Add(errors.New("operation aborted"))
				return
			}

		}
		iter.catcher.Add(sample.Close())
	}
	iter.catcher.Add(chunks.Close())
}
