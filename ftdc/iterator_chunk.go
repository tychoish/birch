package ftdc

import (
	"context"
	"io"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/erc"
)

// ChunkIterator is a simple iterator for reading off of an FTDC data
// source (e.g. file). The iterator processes chunks batches of
// metrics lazily, reading form the io.Reader every time the iterator
// is advanced.
//
// Use the iterator as follows:
//
//	iter := ReadChunks(ctx, file)
//
//	for iter.Next(ctx) {
//	    chunk := iter.Chunk()
//
//	    // <manipulate chunk>
//
//	}
//
//	if err := iter.Close(ctx); err != nil {
//	    return err
//	}
//
// You MUST call the Chunk() method no more than once per iteration.
//
// You shoule check the Err() method when iterator is complete to see
// if there were any issues encountered when decoding chunks.
type ChunkIterator struct {
	pipe    chan *Chunk
	next    *Chunk
	cancel  context.CancelFunc
	closed  bool
	catcher erc.Collector
}

// ReadChunks creates a ChunkIterator from an underlying FTDC data
// source.
func ReadChunks(ctx context.Context, r io.Reader) fun.Iterator[*Chunk] {
	iter := &ChunkIterator{pipe: make(chan *Chunk, 2)}

	ipc := make(chan *birch.Document)
	ctx, iter.cancel = context.WithCancel(ctx)

	go func() {
		iter.catcher.Add(readDiagnostic(ctx, r, ipc))
	}()

	go func() {
		iter.catcher.Add(readChunks(ctx, ipc, iter.pipe))
	}()

	return iter
}

// Next advances the iterator and returns true if the iterator has a
// chunk that is unprocessed. Use the Chunk() method to access the
// iterator.
func (iter *ChunkIterator) Next(ctx context.Context) bool {
	select {
	case next, ok := <-iter.pipe:
		if !ok {
			return false
		}
		iter.next = next
		return true
	case <-ctx.Done():
		return false
	}
}

// Chunk returns a copy of the chunk processed by the iterator. You
// must call Chunk no more than once per iteration. Additional
// accesses to Chunk will panic.
func (iter *ChunkIterator) Value() *Chunk {
	return iter.next
}

// Close releases resources of the iterator. Use this method to
// release those resources if you stop iterating before the iterator
// is exhausted. Canceling the context that you used to create the
// iterator has the same effect. Close returns a non-nil error if the
// iterator encountered any errors during iteration.
func (iter *ChunkIterator) Close(_ context.Context) error {
	iter.cancel()
	iter.closed = true
	return iter.catcher.Resolve()
}
