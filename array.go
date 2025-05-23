// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package birch

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/tychoish/birch/bsonerr"
	"github.com/tychoish/birch/bsontype"
	"github.com/tychoish/birch/elements"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/dt"
)

// Array represents an array in BSON. The methods of this type are more
// expensive than those on Document because they require potentially updating
// multiple keys to ensure the array stays valid at all times.
type Array struct {
	doc *Document
}

// NewArray creates a new array with the specified value.
func NewArray(values ...*Value) *Array {
	doc := DC.Make(len(values))
	for _, v := range values {
		doc.Append(&Element{value: v})
	}

	return &Array{doc: doc}
}

// MakeArray creates a new array with the size hint (capacity)
// specified.
func MakeArray(size int) *Array { return &Array{doc: DC.Make(size)} }

// Len returns the number of elements in the array.
func (a *Array) Len() int {
	return len(a.doc.elems)
}

// Reset clears all elements from the array.
func (a *Array) Reset() {
	a.doc.Reset()
}

// Validate ensures that the array's underlying BSON is valid. It returns the the number of bytes
// in the underlying BSON if it is valid or an error if it isn't.
func (a *Array) Validate() (uint32, error) {
	size := uint32(4 + 1)

	for i, elem := range a.doc.elems {
		n, err := elem.value.validate(false)
		if err != nil {
			return 0, err
		}

		// type
		size++
		// key
		size += uint32(len(strconv.Itoa(i))) + 1
		// value
		size += n
	}

	return size, nil
}

// LookupErr returns the value at the specified index, returning an
// OutOfBounds error if that element doesn't exist.
func (a *Array) Lookup(index uint) (*Value, error) {
	v, ok := a.doc.ElementAtOK(index)
	if !ok {
		return nil, bsonerr.OutOfBounds
	}

	return v.value, nil
}

// LookupElement returns the element at the specified index,
// returning an OutOfBounds error if that element doesn't exist.
func (a *Array) LookupElemdent(index uint) (*Element, error) {
	v, ok := a.doc.ElementAtOK(index)
	if !ok {
		return nil, bsonerr.OutOfBounds
	}

	return v, nil
}

func (a *Array) findElementForStrKey(key ...string) *Element {
	if len(key) == 0 || a == nil {
		return nil
	}

	idx, err := strconv.Atoi(key[0])
	if err != nil {
		return nil
	}
	val, err := a.Lookup(uint(idx))
	if err != nil || val == nil {
		return nil
	}

	if len(key) == 1 {
		return EC.Value(key[0], val)
	}

	if sd, ok := val.MutableDocumentOK(); ok {
		if el, err := sd.Search(key[1:]...); err == nil {
			return el
		}
	}
	if sar, ok := val.MutableArrayOK(); ok {
		return sar.findElementForStrKey(key[1:]...)
	}

	return nil
}

func (a *Array) lookupTraverse(index uint, keys ...string) (*Value, error) {
	value, err := a.Lookup(index)
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return value, nil
	}

	switch value.Type() {
	case bsontype.EmbeddedDocument:
		element, err := value.MutableDocument().Search(keys...)
		if err != nil {
			return nil, err
		}

		return element.Value(), nil
	case bsontype.Array:
		index, err := strconv.ParseUint(keys[0], 10, 0)
		if err != nil {
			return nil, bsonerr.InvalidArrayKey
		}

		val, err := value.MutableArray().lookupTraverse(uint(index), keys[1:]...)
		if err != nil {
			return nil, err
		}

		return val, nil
	default:
		return nil, bsonerr.InvalidDepthTraversal
	}
}

// Append adds the given values to the end of the array. It returns a reference to itself.
func (a *Array) Append(values ...*Value) *Array {
	a.doc.Append(elemsFromValues(values)...)

	return a
}

// Set replaces the value at the given index with the parameter value. It panics if the index is
// out of bounds.
func (a *Array) Set(index uint, value *Value) *Array {
	if index >= uint(len(a.doc.elems)) {
		panic(bsonerr.OutOfBounds)
	}

	a.doc.elems[index] = &Element{value}

	return a
}

// Extend adds the values from the second array to the first array,
// returning the original array for chaining.
func (a *Array) Extend(ar2 *Array) *Array { a.doc.Append(ar2.doc.elems...); return a }

// ExtendFromDocument adds the values from the elements in the
// document returning the array for chaining.
func (a *Array) ExtendFromDocument(doc *Document) *Array { a.doc.Append(doc.elems...); return a }

// Delete removes the value at the given index from the array.
func (a *Array) Delete(index uint) *Value {
	if index >= uint(len(a.doc.elems)) {
		return nil
	}

	elem := a.doc.elems[index]
	a.doc.elems = append(a.doc.elems[:index], a.doc.elems[index+1:]...)

	return elem.value
}

// String implements the fmt.Stringer interface.
func (a *Array) String() string {
	bufbuf := dt.NewSlice(bufpool.Get())
	defer bufpool.Put(bufbuf)
	buf := bytes.NewBuffer(bufbuf)

	buf.WriteString("bson.Array[")

	for idx, elem := range a.doc.elems {
		if idx > 0 {
			buf.WriteString(", ")
		}

		fmt.Fprintf(buf, "%s", elem.value.Interface())
	}

	buf.WriteByte(']')

	return buf.String()
}

// writeByteSlice handles serializing this array to a slice of bytes starting
// at the given start position.
func (a *Array) writeByteSlice(start uint, size uint32, b []byte) (int64, error) {
	var total int64

	pos := start

	if len(b) < int(start)+int(size) {
		return 0, errTooSmall
	}

	n, err := elements.Int32.Encode(start, b, int32(size))
	total += int64(n)
	pos += uint(n)

	if err != nil {
		return total, err
	}

	for i, elem := range a.doc.elems {
		b[pos] = elem.value.data[elem.value.start]
		total++
		pos++

		key := []byte(strconv.Itoa(i))
		key = append(key, 0)
		copy(b[pos:], key)
		total += int64(len(key))
		pos += uint(len(key))

		n, err := elem.writeElement(false, pos, b)
		total += n
		pos += uint(n)

		if err != nil {
			return total, err
		}
	}

	n, err = elements.Byte.Encode(pos, b, '\x00')
	total += int64(n)

	if err != nil {
		return total, err
	}

	return total, nil
}

// MarshalBSON implements the Marshaler interface.
func (a *Array) MarshalBSON() ([]byte, error) {
	size, err := a.Validate()
	if err != nil {
		return nil, err
	}

	b := dt.NewSlice(bufpool.Make())
	b.Grow(int(size))

	if _, err = a.writeByteSlice(0, size, b); err != nil {
		return nil, err
	}

	return b, nil
}

// Iterator returns a ArrayIterator that can be used to iterate through the
// elements of this Array.
func (a *Array) Iterator() *fun.Stream[*Value] {
	return newArrayIterator(a)
}
