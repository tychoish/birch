package ftdc

import (
	"context"
	"io"
	"sync"

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
	*fun.Iterator[*Chunk]
	cancel  context.CancelFunc
	catcher erc.Collector
	wg      sync.WaitGroup
}

// ReadChunks creates a ChunkIterator from an underlying FTDC data
// source.
func ReadChunks(r io.Reader) *fun.Iterator[*Chunk] {
	pipe := make(chan *Chunk)
	ipc := make(chan *birch.Document)
	errob, errprod := fun.HF.ErrorCollector()

	wg := &fun.WaitGroup{}
	ch := fun.Blocking(pipe)
	return ch.Producer().
		PreHook(fun.Operation(func(ctx context.Context) {
			fun.Operation(func(ctx context.Context) {
				errob(readChunks(ctx, ipc, pipe))
			}).Add(ctx, wg)
			fun.Operation(func(ctx context.Context) {
				errob(readDiagnostic(ctx, r, ipc))
			}).Add(ctx, wg)
		}).Once()).
		IteratorWithHook(func(iter *fun.Iterator[*Chunk]) {

			errs, err := errprod.Block()
			if err != nil {
				errs = append(errs, err)
			}
			for idx := range errs {
				iter.AddError(errs[idx])
			}
		})
}

// Close releases resources of the iterator. Use this method to
// release those resources if you stop iterating before the iterator
// is exhausted. Canceling the context that you used to create the
// iterator has the same effect. Close returns a non-nil error if the
// iterator encountered any errors during iteration.
func (iter *ChunkIterator) Close() error {
	iter.cancel()
	iter.wg.Wait()
	return iter.catcher.Resolve()
}
