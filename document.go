// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package birch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/tychoish/birch/bsonerr"
	"github.com/tychoish/birch/elements"
	"github.com/tychoish/birch/jsonx"
	"github.com/tychoish/fun"
)

var bufPool = &sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}

func getBuf() *bytes.Buffer { return bufPool.Get().(*bytes.Buffer) }
func putBuf(buf *bytes.Buffer) {
	if buf.Cap() > 64<<10 {
		return
	}

	buf.Reset()
	bufPool.Put(buf)
}

var iterCtx = context.Background()

// Document is a mutable ordered map that compactly represents a BSON document.
type Document struct {
	// The default behavior or Append, Prepend, and Replace is to panic on the
	// insertion of a nil element. Setting IgnoreNilInsert to true will instead
	// silently ignore any nil paramet()ers to these methods.
	IgnoreNilInsert bool
	elems           []*Element
}

// ReadDocument will create a Document using the provided slice of bytes. If the
// slice of bytes is not a valid BSON document, this method will return an error.
func ReadDocument(b []byte) (*Document, error) {
	var doc = new(Document)

	if err := doc.UnmarshalBSON(b); err != nil {
		return nil, err
	}

	return doc, nil
}

// Copy makes a shallow copy of this document.
func (d *Document) Copy() *Document {
	doc := &Document{
		IgnoreNilInsert: d.IgnoreNilInsert,
		elems:           make([]*Element, len(d.elems), cap(d.elems)),
	}

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
			if d.IgnoreNilInsert {
				continue
			}
			panic(bsonerr.NilElement)
		}

		d.elems = append(d.elems, elem)
	}

	return d
}

// AppendOmitEmpty adds all non-empty values to the document, and has
// no impact otherwise.
func (d *Document) AppendOmitEmpty(elems ...*Element) *Document {
	for _, elem := range elems {
		if elem.Value().IsEmpty() {
			continue
		}

		d.Append(elem)
	}
	return d
}

// Prepend adds each element to the beginning of the document, in
// order. If a nil element is passed as a parameter this method will
// panic.
//
// If a nil element is inserted and this method panics, it does not remove the
// previously added elements.
func (d *Document) Prepend(elems ...*Element) *Document {

	// In order to insert the prepended elements in order we need to make space
	// at the front of the elements slice.
	d.elems = append(d.elems, elems...)
	copy(d.elems[len(elems):], d.elems)

	remaining := len(elems)

	for idx, elem := range elems {
		if elem == nil {
			if d.IgnoreNilInsert {
				// Having nil elements in a document would be problematic.
				copy(d.elems[idx:], d.elems[idx+1:])
				d.elems[len(d.elems)-1] = nil
				d.elems = d.elems[:len(d.elems)-1]

				continue
			}
			// Not very efficient, but we're about to blow up so ¯\_(ツ)_/¯
			for j := idx; j < remaining; j++ {
				copy(d.elems[j:], d.elems[j+1:])
				d.elems[len(d.elems)-1] = nil
				d.elems = d.elems[:len(d.elems)-1]
			}
			panic(bsonerr.NilElement)
		}
		remaining--

		d.elems[idx] = elem
	}

	return d
}

// Set replaces an element of a document. If an element with a matching key is
// found, the element will be replaced with the one provided. If the document
// does not have an element with that key, the element is appended to the
// document instead. If a nil element is passed as a parameter this method will
// panic. To change this behavior to silently ignore a nil element, set
// IgnoreNilInsert to true on the Document.
//
// If a nil element is inserted and this method panics, it does not remove the
// previously added elements.
func (d *Document) Set(elem *Element) *Document {
	if elem == nil {
		if d.IgnoreNilInsert {
			return d
		}

		panic(bsonerr.NilElement)
	}

	for idx, e := range d.elems {
		if elem.Key() == e.Key() {
			d.elems[idx] = elem
			return d
		}
	}

	d.elems = append(d.elems, elem)

	return d
}

// Delete removes the keys from the Document. The deleted element is
// returned. If the key does not exist, then nil is returned and the delete is
// a no-op. The same is true if something along the depth tree does not exist
// or is not a traversable type.
func (d *Document) Delete(key string) *Element {
	for idx := range d.elems {
		if d.elems[idx].Key() == key {
			elem := d.elems[idx]
			d.elems = append(d.elems[:idx], d.elems[idx+1:]...)
			return elem
		}
	}

	return nil
}

// ElementAt retrieves the element at the given index in a Document. It panics if the index is
// out-of-bounds.
//
// TODO(skriptble): This method could be variadic and return the element at the
// provided depth.
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
func (d *Document) Iterator() fun.Iterator[*Element] {
	return newIterator(d)
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

// WriteToSlice will serialize this document to the byte slice,
// starting at the specified index of the buffer.
func (d *Document) WriteToSlice(start uint, output []byte) (int64, error) {
	var total int64

	size, err := d.Validate()
	if err != nil {
		return total, err
	}

	n, err := d.writeByteSlice(start, size, output)
	total += n

	if err != nil {
		return total, err
	}

	return total, nil
}

// writeByteSlice handles serializing this document to a slice of bytes starting
// at the given start position.
func (d *Document) writeByteSlice(start uint, size uint32, b []byte) (int64, error) {
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

	return iter.Close(iterCtx)
}

// ReadFrom will read one BSON document from the given io.Reader.
func (d *Document) ReadFrom(r io.Reader) (int64, error) {

	var total int64

	sizeBuf := make([]byte, 4)
	n, err := io.ReadFull(r, sizeBuf)
	total += int64(n)

	if err != nil {
		return total, err
	}

	givenLength := readi32(sizeBuf)
	b := make([]byte, givenLength)
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

	buf := getBuf()
	defer putBuf(buf)

	buf.Write([]byte("bson.Document{"))

	for idx, elem := range d.elems {
		if idx > 0 {
			buf.Write([]byte(", "))
		}

		fmt.Fprintf(buf, "%s", elem)
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
	case map[string]any:
		for _, elem := range d.elems {
			out[elem.Key()] = elem.value.Interface()
		}
	case map[string]string:
		for _, elem := range d.elems {
			if val, ok := elem.value.StringValueOK(); ok {
				out[elem.Key()] = val
			}
		}
	case map[string]int:
		for _, elem := range d.elems {
			if val, ok := elem.value.IntOK(); ok {
				out[elem.Key()] = val
			}
		}
	case map[string]int32:
		for _, elem := range d.elems {
			// TODO: consider being permissive about small int32s
			if val, ok := elem.value.Int32OK(); ok {
				out[elem.Key()] = val
			}
		}
	case map[string]int64:
		for _, elem := range d.elems {
			// be permissive about reading bson int32s
			// into int64s:
			if val, ok := elem.value.IntOK(); ok {
				out[elem.Key()] = int64(val)
			}
		}
	// TODO: add support for uints
	case map[string]time.Time:
		for _, elem := range d.elems {
			if val, ok := elem.value.TimeOK(); ok {
				out[elem.Key()] = val
			}
		}
	case map[string]time.Duration:
		for _, elem := range d.elems {
			if val, ok := elem.value.IntOK(); ok {
				out[elem.Key()] = time.Duration(int64(val))
			}
		}
	case map[string]DocumentUnmarshaler:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableDocumentOK(); ok {
				out[elem.Key()] = val
			}
		}
	case map[string]*jsonx.Document:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableDocumentOK(); ok {
				out[elem.Key()] = val.toJSON()
			}
		}
	case map[string]*jsonx.Value:
		for _, elem := range d.elems {
			out[elem.Key()] = elem.value.toJSON()
		}
	case map[string][]string:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]string, 0, val.Len())
				for _, e := range val.doc.elems {
					if str, ok := e.value.StringValueOK(); ok {
						value = append(value, str)
					}
				}
				out[elem.Key()] = value
			} else if str, ok := elem.value.StringValueOK(); ok {
				out[elem.Key()] = []string{str}
			}
		}
	case map[string][]any:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]any, 0, val.Len())
				for _, e := range val.doc.elems {
					value = append(value, e.value.Interface())
				}
				out[elem.Key()] = value
			} else {
				out[elem.Key()] = []any{elem.value.Interface()}
			}
		}
	case map[string][]int:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]int, 0, val.Len())
				for _, e := range val.doc.elems {
					if num, ok := e.value.IntOK(); ok {
						value = append(value, num)
					}
				}
				out[elem.Key()] = value
			} else if num, ok := elem.value.IntOK(); ok {
				out[elem.Key()] = []int{num}
			}
		}
	case map[string][]int32:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]int32, 0, val.Len())
				for _, e := range val.doc.elems {
					if num, ok := e.value.Int32OK(); ok {
						value = append(value, num)
					}
				}
				out[elem.Key()] = value
			} else if num, ok := elem.value.Int32OK(); ok {
				out[elem.Key()] = []int32{num}
			}
		}
	case map[string][]int64:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]int64, 0, val.Len())
				for _, e := range val.doc.elems {
					if num, ok := e.value.IntOK(); ok {
						value = append(value, int64(num))
					}
				}
				out[elem.Key()] = value
			} else if num, ok := elem.value.IntOK(); ok {
				out[elem.Key()] = []int64{int64(num)}
			}
		}
	case map[string][]time.Time:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]time.Time, 0, val.Len())
				for _, e := range val.doc.elems {
					if ts, ok := e.value.TimeOK(); ok {
						value = append(value, ts)
					}
				}
				out[elem.Key()] = value
			} else if ts, ok := elem.value.TimeOK(); ok {
				out[elem.Key()] = []time.Time{ts}
			}
		}
	case map[string][]time.Duration:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]time.Duration, 0, val.Len())
				for _, e := range val.doc.elems {
					if dur, ok := e.value.IntOK(); ok {
						value = append(value, time.Duration(dur))
					}
				}
				out[elem.Key()] = value
			} else if dur, ok := elem.value.IntOK(); ok {
				out[elem.Key()] = []time.Duration{time.Duration(dur)}
			}
		}
	case map[string][]DocumentUnmarshaler:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]DocumentUnmarshaler, 0, val.Len())
				for _, e := range val.doc.elems {
					if doc, ok := e.value.MutableDocumentOK(); ok {
						value = append(value, doc)
					}
				}
				out[elem.Key()] = value
			} else if doc, ok := elem.value.MutableDocumentOK(); ok {
				out[elem.Key()] = []DocumentUnmarshaler{doc}
			}
		}
	case map[string][]*jsonx.Document:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]*jsonx.Document, 0, val.Len())
				for _, e := range val.doc.elems {
					if doc, ok := e.value.MutableDocumentOK(); ok {
						value = append(value, doc.toJSON())
					}
				}
				out[elem.Key()] = value
			} else if doc, ok := elem.value.MutableDocumentOK(); ok {
				out[elem.Key()] = []*jsonx.Document{doc.toJSON()}
			}
		}
	case map[string][]*jsonx.Value:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]*jsonx.Value, 0, val.Len())
				for _, e := range val.doc.elems {
					value = append(value, e.value.toJSON())
				}
				out[elem.Key()] = value
			} else {
				out[elem.Key()] = []*jsonx.Value{elem.value.toJSON()}
			}
		}
	case map[any]any:
		for _, elem := range d.elems {
			out[elem.Key()] = elem.value.Interface()
		}
	case map[any][]any:
		for _, elem := range d.elems {
			if val, ok := elem.value.MutableArrayOK(); ok {
				value := make([]any, 0, val.Len())
				for _, e := range val.doc.elems {
					value = append(value, e.value.Interface())
				}
				out[elem.Key()] = value
			} else {
				out[elem.Key()] = []any{elem.value.Interface()}
			}
		}
	case DocumentUnmarshaler:
		return out.UnmarshalDocument(d)
	case Unmarshaler:
		raw, err := d.MarshalBSON()
		if err != nil {
			return err
		}
		return out.UnmarshalBSON(raw)
	default:
		// TODO consider falling back to reflection
		return fmt.Errorf("cannot unmarshal into %T", into)
	}
	return nil
}
