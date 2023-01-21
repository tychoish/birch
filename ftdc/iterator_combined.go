package ftdc

import (
	"context"
	"errors"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/erc"
)

type combinedIterator struct {
	closer   context.CancelFunc
	chunks   fun.Iterator[*Chunk]
	sample   *sampleIterator
	metadata *birch.Document
	document *birch.Document
	pipe     chan *birch.Document
	catcher  erc.Collector
	flatten  bool
}

func (iter *combinedIterator) Close(ctx context.Context) error {
	iter.closer()
	if iter.sample != nil {
		iter.sample.Close(ctx)
	}

	if iter.chunks != nil {
		iter.chunks.Close(ctx)
	}
	return iter.catcher.Resolve()
}

func (iter *combinedIterator) Metadata() *birch.Document { return iter.metadata }
func (iter *combinedIterator) Value() *birch.Document    { return iter.document }

func (iter *combinedIterator) Next(ctx context.Context) bool {
	select {
	case next, ok := <-iter.pipe:
		if !ok {
			return false
		}
		iter.document = next
		return true
	case <-ctx.Done():
		return false
	}
}

func (iter *combinedIterator) worker(ctx context.Context) {
	defer close(iter.pipe)
	var ok bool

	for iter.chunks.Next(ctx) {
		chunk := iter.chunks.Value()

		if iter.flatten {
			iter.sample, ok = chunk.Iterator(ctx).(*sampleIterator)
		} else {
			iter.sample, ok = chunk.StructuredIterator(ctx).(*sampleIterator)
		}
		if !ok {
			iter.catcher.Add(errors.New("programmer error"))
			return
		}
		if iter.metadata != nil {
			iter.metadata = chunk.GetMetadata()
		}

		for iter.sample.Next(ctx) {
			select {
			case iter.pipe <- iter.sample.Value():
				continue
			case <-ctx.Done():
				iter.catcher.Add(errors.New("operation aborted"))
				return
			}

		}
		iter.catcher.Add(iter.sample.Close(ctx))
	}
	iter.catcher.Add(iter.chunks.Close(ctx))
}
