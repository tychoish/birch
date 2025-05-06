package jsonx

import (
	"context"
	"io"

	"github.com/tychoish/fun"
)

type documentIterImpl struct {
	idx     int
	doc     *Document
	current *Element
	err     error
}

func legacyIteratorConverter[V any, T interface {
	Next(context.Context) bool
	Value() V
}](iter T) fun.Generator[V] {
	var hasNext bool = true
	var zero V

	return func(ctx context.Context) (V, error) {
		if !hasNext {
			return zero, io.EOF
		}
		if cerr := ctx.Err(); cerr != nil {
			return zero, cerr
		}
		hasNext = iter.Next(ctx)
		if !hasNext {
			return zero, io.EOF
		}
		return iter.Value(), nil
	}
}

func (iter *documentIterImpl) Next(ctx context.Context) bool {
	if iter.idx+1 > iter.doc.Len() || ctx.Err() != nil {
		return false
	}

	iter.current = iter.doc.elems[iter.idx].Copy()
	iter.idx++

	return true
}

func (iter *documentIterImpl) Value() *Element { return iter.current }
func (iter *documentIterImpl) Close() error    { return nil }

type arrayIterImpl struct {
	idx     int
	array   *Array
	current *Value
	err     error
}

func (iter *arrayIterImpl) Next(ctx context.Context) bool {
	if iter.idx+1 > iter.array.Len() || ctx.Err() != nil {
		return false
	}

	iter.current = iter.array.elems[iter.idx].Copy()
	iter.idx++

	return true
}

func (iter *arrayIterImpl) Value() *Value { return iter.current }
func (iter *arrayIterImpl) Close() error  { return nil }
