// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package birch

import (
	"bytes"
	"fmt"
	"io"

	"github.com/tychoish/birch/bsonerr"
	"github.com/tychoish/birch/elements"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/adt"
	"github.com/tychoish/fun/dt"
)

var bufpool = adt.DefaultBufferPool()

// Document is a mutable ordered map that compactly represents a BSON document.
type Document struct {
	elems []*Element

	cache      DocumentMap
	cacheValid bool
}

// ReadDocument will create a Document using the provided slice of bytes. If the
// slice of bytes is not a valid BSON document, this method will return an error.
func ReadDocument(b []byte) (*Document, error) {
	doc := new(Document)

	if err := doc.UnmarshalBSON(b); err != nil {
		return nil, err
	}

	return doc, nil
}

// Copy makes a shallow copy of this document.
func (d *Document) Copy() *Document {
	doc := &Document{elems: make([]*Element, len(d.elems), cap(d.elems))}

	copy(doc.elems, d.elems)

	return doc
}

// Len returns the number of elements in the document.
func (d *Document) Len() int { return len(d.elems) }

// Append adds each element to the end of the document, in order. If a nil element is passed
// as a parameter this method will panic. To change this behavior to silently
// ignore a nil element, set IgnoreNilInsert to true on the Document.
//
// If a nil element is inserted and this method panics, it does not remove the
// previously added elements.
func (d *Document) Append(elems ...*Element) *Document {
	for _, elem := range elems {
		if elem == nil {
			panic(bsonerr.NilElement)
		}

		d.elems = append(d.elems, elem)
	}
	d.cacheValid = false
	return d
}

// AppendOmitEmpty adds all non-empty values to the document, and has
// no impact otherwise.
func (d *Document) AppendOmitEmpty(elems ...*Element) *Document {
	for _, elem := range elems {
		if elem == nil || elem.Value().IsEmpty() {
			continue
		}

		d.Append(elem)
	}

	d.cacheValid = false
	return d
}

// Set replaces an element of a document. If an element with a matching key is
// found, the element will be replaced with the one provided. If the document
// does not have an element with that key, the element is appended to the
// document instead. If a nil element is passed as a parameter this method will
// panic.
//
// If a nil element is inserted and this method panics.
func (d *Document) Set(elem *Element) *Document {
	if elem == nil {
		panic(bsonerr.NilElement)
	}

	for idx, e := range d.elems {
		if elem.Key() == e.Key() {
			d.elems[idx] = elem
			return d
		}
	}

	d.elems = append(d.elems, elem)
	d.cacheValid = false

	return d
}

// Delete removes the keys from the Document. The deleted element is
// returned. If the key does not exist, then nil is returned and the delete is
// a no-op.
func (d *Document) Delete(key string) *Element {
	for idx := range d.elems {
		if d.elems[idx].Key() == key {
			elem := d.elems[idx]
			d.elems = append(d.elems[:idx], d.elems[idx+1:]...)
			return elem
		}
	}
	d.cacheValid = false
	return nil
}

// ElementAt retrieves the element at the given index in a
// Document. It panics if the index is out-of-bounds.
func (d *Document) ElementAt(index uint) *Element {
	return d.elems[index]
}

// ElementAtOK is the same as ElementAt, but returns a boolean instead of panicking.
func (d *Document) ElementAtOK(index uint) (*Element, bool) {
	if index >= uint(len(d.elems)) {
		return nil, false
	}

	return d.ElementAt(index), true
}

// Iterator creates an Iterator for this document and returns it.
func (d *Document) Iterator() *fun.Stream[*Element] {
	return fun.MakeStream(legacyIteratorConverter[*Element, *elementIterator](&elementIterator{d: d}))
}

// Extend merges a second document into the document. It may produce a
// document with duplicate keys.
func (d *Document) Extend(d2 *Document) *Document { d.Append(d2.elems...); return d }

// Reset clears a document so it can be reused. This method clears references
// to the underlying pointers to elements so they can be garbage collected.
func (d *Document) Reset() {
	for idx := range d.elems {
		d.elems[idx] = nil
	}
	d.cacheValid = false
	d.elems = d.elems[:0]
}

// Search iterates through the keys in the document, returning the
// element with the matching key, and nil othe
func (d *Document) Search(keys ...string) (*Element, error) {
	if d == nil || len(keys) == 0 {
		return nil, bsonerr.ElementNotFound
	}

	elem := d.findElemForKey(keys[0])
	if elem == nil {
		return nil, bsonerr.ElementNotFound
	}

	if len(keys) == 1 {
		return elem, nil
	}

	if sd, ok := elem.Value().MutableDocumentOK(); ok {
		return sd.Search(keys[1:]...)
	}
	if ar, ok := elem.Value().MutableArrayOK(); ok {
		if em := ar.findElementForStrKey(keys[1:]...); em != nil {
			return em, nil
		}
	}

	return nil, bsonerr.ElementNotFound
}

func (d *Document) findElemForKey(key string) *Element {
	for idx := range d.elems {
		if d.elems[idx].Key() == key {
			return d.elems[idx]
		}
	}
	return nil
}

// Validate validates the document and returns its total size.
func (d *Document) Validate() (uint32, error) {
	if d == nil {
		return 0, bsonerr.NilDocument
	}

	// Header and Footer
	var size uint32 = 4 + 1

	for _, elem := range d.elems {
		n, err := elem.Validate()
		if err != nil {
			return 0, err
		}

		size += n
	}

	return size, nil
}

// WriteTo implements the io.WriterTo interface.
//
// TODO(skriptble): We can optimize this by having creating implementations of
// writeByteSlice that write directly to an io.Writer instead.
func (d *Document) WriteTo(w io.Writer) (int64, error) {
	b, err := d.MarshalBSON()
	if err != nil {
		return 0, err
	}

	n, err := w.Write(b)

	return int64(n), err
}

// writeByteSlice handles serializing this document to a slice of bytes starting
// at the given start position.
func (d *Document) writeByteSlice(start uint, size uint32, b []byte) (int64, error) {
	var total int64
	var pos uint
	pos = start
	if len(b) < int(start)+int(size) {
		return 0, errTooSmall
	}
	n, err := elements.Int32.Encode(start, b, int32(size))
	total += int64(n)
	pos += uint(n)

	if err != nil {
		return total, err
	}

	for _, elem := range d.elems {
		n, err := elem.writeElement(true, pos, b)
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
func (d *Document) MarshalBSON() ([]byte, error) {
	size, err := d.Validate()
	if err != nil {
		return nil, err
	}

	b := make([]byte, size)
	_, err = d.writeByteSlice(0, size, b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// UnmarshalBSON implements the Unmarshaler interface.
func (d *Document) UnmarshalBSON(b []byte) error {
	iter, err := Reader(b).Iterator()
	if err != nil {
		return err
	}

	d.elems = make([]*Element, 0, 128)

	for iter.Next(iterCtx) {
		d.elems = append(d.elems, iter.Value())
	}

	return iter.Close()
}

// ReadFrom will read one BSON document from the given io.Reader.
func (d *Document) ReadFrom(r io.Reader) (int64, error) {
	var total int64

	sizeBuf := dt.NewSlice(bufpool.Get())
	sizeBuf.Grow(4)
	defer bufpool.Put(sizeBuf)

	n, err := io.ReadFull(r, sizeBuf)
	total += int64(n)

	if err != nil {
		return total, err
	}

	givenLength := readi32(sizeBuf)

	b := dt.NewSlice(bufpool.Make())
	b.Grow(int(givenLength))

	copy(b[0:4], sizeBuf)
	n, err = io.ReadFull(r, b[4:])
	total += int64(n)

	if err != nil {
		return total, err
	}

	return total, d.UnmarshalBSON(b)
}

// String implements the fmt.Stringer interface.
func (d *Document) String() string {
	buf := &bytes.Buffer{}

	buf.WriteString("bson.Document{")

	for idx, elem := range d.elems {
		if idx > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(elem.String())
	}

	buf.WriteByte('}')

	return buf.String()
}

// Unmarshal attempts to read the bson document into the interface
// value provided, with the semantics dependent on the input type. The
// semantics are loose (particularly for sized integers), work
// reasonably well for simple map types and will not always round
// trip. While unmarshal will overwrite values in an existing input
// structure, it will not delete other values, and will avoid writing
// fields in the document which cannot be easily converted. While this
// method does not work with arbitrary types that do not implement
// DocumentUnmarshaler, it does not use reflection.
func (d *Document) Unmarshal(into any) error {
	switch out := into.(type) {
	case DocumentUnmarshaler:
		return out.UnmarshalDocument(d)
	case Unmarshaler:
		raw, err := d.MarshalBSON()
		if err != nil {
			return err
		}
		return out.UnmarshalBSON(raw)
	case map[string]string:
		for _, elem := range d.elems {
			if val, ok := elem.value.StringValueOK(); ok {
				out[elem.Key()] = val
			}
		}
	case map[string]any:
		for _, elem := range d.elems {
			out[elem.Key()] = elem.value.Interface()
		}
	case map[any]any:
		for _, elem := range d.elems {
			out[elem.Key()] = elem.value.Interface()
		}
	default:
		// TODO consider falling back to reflection
		return fmt.Errorf("cannot unmarshal into %T", into)
	}
	return nil
}
