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

	"errors"

	"github.com/tychoish/birch/bsonerr"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/dt"
)

var errValidateDone = errors.New("validation loop complete")

// Reader is a wrapper around a byte slice. It will interpret the slice as a
// BSON document. Most of the methods on Reader are low cost and are meant for
// simple operations that are run a few times. Because there is no metadata
// stored all methods run in O(n) time. If a more efficient lookup method is
// necessary then the Document type should be used.
type Reader []byte

// NewFromIOReader reads in a document from the given io.Reader and constructs a bson.Reader from
// it.
func NewFromIOReader(r io.Reader) (Reader, error) {
	if r == nil {
		return nil, bsonerr.NilReader
	}

	lengthBytes := dt.NewSlice(bufpool.Get())
	lengthBytes.Grow(4)
	defer bufpool.Put(lengthBytes)

	count, err := io.ReadFull(r, lengthBytes[:])
	if err != nil {
		return nil, err
	}

	if count < 4 {
		return nil, errTooSmall
	}

	length := readi32(lengthBytes[:])
	if length < 0 {
		return nil, bsonerr.InvalidLength
	}

	reader := make([]byte, length)

	copy(reader, lengthBytes[:])

	count, err = io.ReadFull(r, reader[4:])
	if err != nil {
		return nil, err
	}

	if int32(count) != length-4 {
		return nil, bsonerr.InvalidLength
	}

	return reader, nil
}

// Validate validates the document. This method only validates the first document in
// the slice, to validate other documents, the slice must be resliced.
func (r Reader) Validate() (size uint32, err error) {
	return r.readElements(func(elem *Element) error {
		var err error
		switch elem.value.Type() {
		case '\x03':
			_, err = elem.value.ReaderDocument().Validate()
		case '\x04':
			_, err = elem.value.ReaderArray().Validate()
		}
		return err
	})
}

// validateKey will ensure the key is valid and return the length of the key
// including the null terminator.
func (r Reader) validateKey(pos, end uint32) (uint32, error) {
	// Read a CString, return the length, including the '\x00'
	var total uint32

	for ; pos < end && r[pos] != '\x00'; pos++ {
		total++
	}

	if pos == end || r[pos] != '\x00' {
		return total, bsonerr.InvalidKey
	}
	total++

	return total, nil
}

// RecursiveLookup search the document, potentially recursively, for the given key. If
// there are multiple keys provided, this method will recurse down, as long as
// the top and intermediate nodes are either documents or arrays. If any key
// except for the last is not a document or an array, an error will be returned.
//
// TODO(skriptble): Implement better error messages.
//
// TODO(skriptble): Determine if this should return an error on empty key and
// key not found.
func (r Reader) RecursiveLookup(key ...string) (*Element, error) {
	if len(key) < 1 {
		return nil, bsonerr.EmptyKey
	}

	var elem *Element

	_, err := r.readElements(func(e *Element) error {
		if key[0] == e.Key() {
			if len(key) > 1 {
				switch e.value.Type() {
				case '\x03':
					e, err := e.value.ReaderDocument().RecursiveLookup(key[1:]...)
					if err != nil {
						return err
					}
					elem = e
					return errValidateDone
				case '\x04':
					e, err := e.value.ReaderArray().RecursiveLookup(key[1:]...)
					if err != nil {
						return err
					}
					elem = e
					return errValidateDone
				default:
					return bsonerr.InvalidDepthTraversal
				}
			}
			elem = e
			return errValidateDone
		}
		return nil
	})

	if elem == nil && err == nil {
		return nil, bsonerr.ElementNotFound
	}

	return elem, err
}

// ElementAt searches for a retrieves the element at the given index. This
// method will validate all the elements up to and including the element at
// the given index.
func (r Reader) ElementAt(index uint) (*Element, error) {
	var (
		current uint
		elem    *Element
	)

	_, err := r.readElements(func(e *Element) error {
		if current != index {
			current++
			return nil
		}
		elem = e
		return errValidateDone
	})
	if err != nil {
		return nil, err
	}

	if elem == nil {
		return nil, bsonerr.OutOfBounds
	}

	return elem, nil
}

// Iterator returns a ReaderIterator that can be used to iterate through the
// elements of this Reader.
func (r Reader) Iterator() (*fun.Stream[*Element], error) {
	iter, err := newReaderIterator(r)
	if err != nil {
		return nil, err
	}

	return legacyIteratorConverter[*Element](iter).Stream(), nil
}

// String implements the fmt.Stringer interface.
func (r Reader) String() string {
	var buf bytes.Buffer

	buf.Write([]byte("bson.Reader{"))

	idx := 0

	_, _ = r.readElements(func(elem *Element) error {
		if idx > 0 {
			buf.Write([]byte(", "))
		}
		fmt.Fprintf(&buf, "%s", elem)
		idx++
		return nil
	})

	buf.WriteByte('}')

	return buf.String()
}

// MarshalBSON implements the bsoncodec.Marshaler interface.
//
// This method does not copy the bytes from r.
func (r Reader) MarshalBSON() ([]byte, error) {
	if _, err := r.Validate(); err != nil {
		return nil, err
	}

	return r, nil
}

// readElements is an internal method used to traverse the document. It will
// validate the document and the underlying elements. If the provided function
// is non-nil it will be called for each element. If `errValidateDone` is returned
// from the function, this method will return. This method will return nil when
// the function returns errValidateDone, in all other cases a non-nil error will
// be returned by this method.
func (r Reader) readElements(f func(e *Element) error) (uint32, error) {
	if len(r) < 5 {
		return 0, errTooSmall
	}
	// TODO(skriptble): We could support multiple documents in the same byte
	// slice without reslicing if we have pos as a parameter and use that to
	// get the length of the document.
	givenLength := readi32(r[0:4])
	if len(r) < int(givenLength) || givenLength < 0 {
		return 0, bsonerr.InvalidLength
	}

	pos := uint32(4)
	end := uint32(givenLength)

	var (
		elemStart    uint32
		elemValStart uint32
		elem         *Element
	)

	for {
		if pos >= end {
			// We've gone off the end of the buffer and we're missing
			// a null terminator.
			return pos, bsonerr.InvalidReadOnlyDocument
		}

		if r[pos] == '\x00' {
			break
		}

		elemStart = pos
		pos++
		n, err := r.validateKey(pos, end)
		pos += n

		if err != nil {
			return pos, err
		}

		elemValStart = pos
		elem = newElement(elemStart, elemValStart)
		elem.value.data = r
		n, err = elem.value.validate(true)
		pos += n

		if err != nil {
			return pos, err
		}

		if f != nil {
			err = f(elem)
			if err != nil {
				if err == errValidateDone {
					break
				}

				return pos, err
			}
		}
	}

	// The size is always 1 larger than the position, since position is 0
	// indexed.
	return pos + 1, nil
}

// readi32 is a helper function for reading an int32 from slice of bytes.
func readi32(b []byte) int32 {
	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
	return int32(b[0]) | int32(b[1])<<8 | int32(b[2])<<16 | int32(b[3])<<24
}
