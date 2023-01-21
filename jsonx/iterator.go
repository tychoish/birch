package jsonx

import "context"

type documentIterImpl struct {
	idx     int
	doc     *Document
	current *Element
	err     error
}

func (iter *documentIterImpl) Next(ctx context.Context) bool {
	if iter.idx+1 > iter.doc.Len() || ctx.Err() != nil {
		return false
	}

	iter.current = iter.doc.elems[iter.idx].Copy()
	iter.idx++

	return true
}

func (iter *documentIterImpl) Value() *Element               { return iter.current }
func (iter *documentIterImpl) Close(_ context.Context) error { return nil }

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

func (iter *arrayIterImpl) Value() *Value                 { return iter.current }
func (iter *arrayIterImpl) Close(_ context.Context) error { return nil }
