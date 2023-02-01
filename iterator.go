// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package birch

import (
	"context"

	"github.com/tychoish/birch/bsonerr"
)

// ElementIterator facilitates iterating over a bson.Document.
type elementIterator struct {
	d     *Document
	index int
	elem  *Element
	err   error
}

// Next fetches the next element of the document, returning whether or not the next element was able
// to be fetched. If true is returned, then call Element to get the element. If false is returned,
// call Err to check if an error occurred.
func (itr *elementIterator) Next(ctx context.Context) bool {
	if itr.index >= len(itr.d.elems) || ctx.Err() != nil {
		return false
	}

	e := itr.d.elems[itr.index]

	_, err := e.Validate()
	if err != nil {
		itr.err = err
		return false
	}

	itr.elem = e
	itr.index++

	return true
}

// Element returns the current element of the Iterator. The pointer that it returns will
// _always_ be the same for a given Iterator.
func (itr *elementIterator) Value() *Element               { return itr.elem }
func (itr *elementIterator) Close(_ context.Context) error { return itr.err }

// readerIterator facilitates iterating over a bson.Reader.
type readerIterator struct {
	r    Reader
	pos  uint32
	end  uint32
	elem *Element
	err  error
}

// newReaderIterator constructors a new readerIterator over a given Reader.
func newReaderIterator(r Reader) (*readerIterator, error) {
	itr := new(readerIterator)

	if len(r) < 5 {
		return nil, errTooSmall
	}

	givenLength := readi32(r[0:4])
	if len(r) < int(givenLength) {
		return nil, bsonerr.InvalidLength
	}

	itr.r = r
	itr.pos = 4
	itr.end = uint32(givenLength)

	return itr, nil
}

// Next fetches the next element of the Reader, returning whether or not the next element was able
// to be fetched. If true is returned, then call Element to get the element. If false is returned,
// call Err to check if an error occurred.
func (itr *readerIterator) Next(ctx context.Context) bool {
	if itr.pos >= itr.end {
		itr.err = bsonerr.InvalidReadOnlyDocument
		return false
	}

	if itr.r[itr.pos] == '\x00' {
		return false
	}
	if ctx.Err() != nil {
		return false
	}

	elemStart := itr.pos
	itr.pos++
	n, err := itr.r.validateKey(itr.pos, itr.end)
	itr.pos += n

	if err != nil {
		itr.err = err
		return false
	}

	itr.elem = &Element{
		value: &Value{
			start:  elemStart,
			offset: itr.pos,
			data:   itr.r,
		},
	}

	n, err = itr.elem.value.validate(true)
	itr.pos += n

	if err != nil {
		itr.err = err
		return false
	}

	return true
}

// Element returns the current element of the readerIterator. The pointer that it returns will
// _always_ be the same for a given readerIterator.
func (itr *readerIterator) Value() *Element                 { return itr.elem }
func (itr *readerIterator) Close(ctx context.Context) error { return itr.err }

// arrayIterator facilitates iterating over a bson.Array.
type arrayIterator struct {
	array *Array
	pos   uint
	elem  *Value
	err   error
}

func newArrayIterator(a *Array) *arrayIterator {
	return &arrayIterator{array: a}
}

// Next fetches the next value in the Array, returning whether or not it could be fetched successfully. If true is
// returned, call Value to get the value. If false is returned, call Err to check if an error occurred.
func (iter *arrayIterator) Next(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	val, err := iter.array.Lookup(iter.pos)
	if err != nil {
		// error if out of bounds
		// don't assign iter.err
		return false
	}

	_, err = val.validate(false)
	if err != nil {
		iter.err = err
		return false
	}

	iter.elem = val
	iter.pos++

	return true
}

// Value returns the current value of the arrayIterator. The pointer returned will _always_ be the same for a given
// arrayIterator. The returned value will be nil if this function is called before the first successful call to Next().
func (iter *arrayIterator) Value() *Value                 { return iter.elem }
func (iter *arrayIterator) Close(_ context.Context) error { return iter.err }
