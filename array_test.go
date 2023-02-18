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
	"strconv"
	"testing"

	"github.com/tychoish/birch/bsonerr"
)

func TestArray(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		t.Run("Nil Insert", func(t *testing.T) {
			func() {
				defer func() {
					r := recover()
					if r != bsonerr.NilElement {
						t.Errorf("Did not received expected error from panic. got %#v; want %#v", r, bsonerr.NilElement)
					}
				}()
				a := NewArray()
				a.Append(nil)
			}()
		})
		testCases := []struct {
			name   string
			values [][]*Value
			want   []byte
		}{
			{"one-one", tapag.oneOne(), tapag.oneOneBytes(0)},
			{"two-one", tapag.twoOne(), tapag.twoOneAppendBytes(0)},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				a := NewArray()
				for _, elems := range tc.values {
					a.Append(elems...)
				}

				got, err := a.MarshalBSON()
				if err != nil {
					t.Errorf("Received an unexpected error while marshaling BSON: %s", err)
				}
				if !bytes.Equal(got, tc.want) {
					t.Errorf("Output from Append is not correct. got %#v; want %#v", got, tc.want)
				}
			})
		}
	})
	t.Run("Lookup", func(t *testing.T) {
		testCases := []struct {
			name string
			a    *Array
			key  uint
			want *Value
			err  error
		}{
			{
				"first",
				NewArray(VC.Null()),
				0,
				&Value{start: 0, offset: 2, data: []byte{0xa, 0x0}},
				nil,
			},
			{
				"not-found",
				NewArray(VC.Null()),
				1,
				nil,
				bsonerr.OutOfBounds,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := tc.a.Lookup(tc.key)
				if err != tc.err {
					t.Errorf("Returned error does not match. got %#v; want %#v", err, tc.err)
				}
				if !valueEqual(got, tc.want) {
					t.Errorf("Returned element does not match expected element. got %#v; want %#v", got, tc.want)
				}
			})
		}
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("empty key", func(t *testing.T) {
			d := DC.Elements()
			var want *Element
			got := d.Delete("")
			if got != want {
				t.Errorf("Delete should return nil element when deleting with empty key. got %#v, want %#v", got, want)
			}
		})
		testCases := []struct {
			name string
			a    *Array
			key  uint
			want *Value
		}{
			{
				"first",
				NewArray(VC.Null()),
				0,
				&Value{start: 0, offset: 2},
			},
			{
				"not-found",
				NewArray(VC.Null()),
				1,
				nil,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				got := tc.a.Delete(tc.key)
				if !valueEqual(got, tc.want) {
					t.Errorf("Returned element does not match expected element. got %#v; want %#v", got, tc.want)
				}
			})
		}
	})
	t.Run("Iterator", func(t *testing.T) {
		iteratorTests := []struct {
			name   string
			values [][]*Value
		}{
			{"one-one", tapag.oneOne()},
			{"two-one", tapag.twoOne()},
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for _, tc := range iteratorTests {
			t.Run(tc.name, func(t *testing.T) {
				a := NewArray()
				for _, elems := range tc.values {
					a.Append(elems...)
				}

				iter := a.Iterator()

				for _, elem := range tc.values {
					if !iter.Next(ctx) {
						t.Errorf("ArrayIterator.Next() returned false")
					}

					if err := iter.Close(); err != nil {
						t.Errorf("ArrayIterator.Err() returned non-nil error: %s", err)
					}

					for _, val := range elem {
						got := iter.Value()
						if !valueEqual(got, val) {
							t.Errorf("Returned element does not match expected element. got %#v; want %#v", got, val)
						}
					}
				}

				if iter.Next(ctx) {
					t.Errorf("ArrayIterator.Next() returned true. expected false")
				}

				if err := iter.Close(); err != nil {
					t.Errorf("ArrayIterator.Err() returned non-nil error: %s", err)
				}
			})
		}
	})
	t.Run("Constructors", func(t *testing.T) {
		t.Run("Make", func(t *testing.T) {
			ar := MakeArray(42)
			if 0 != ar.Len() {
				t.Error("values should be equal")
			}
			if 42 != cap(ar.doc.elems) {
				t.Error("values should be equal")
			}
		})
	})
	t.Run("Reset", func(t *testing.T) {
		ar := NewArray(VC.Int(42))
		if 1 != ar.Len() {
			t.Error("values should be equal")
		}
		ar.Reset()
		if 0 != ar.Len() {
			t.Error("values should be equal")
		}
	})
	t.Run("Validate", func(t *testing.T) {
		t.Run("Passing", func(t *testing.T) {
			ar := NewArray(VC.Int(42), VC.Int(84))
			ln, err := ar.Validate()
			if err != nil {
				t.Fatal(err)
			}
			if ln == 0 {
				t.Fatal("truth assertion failed", ln)
			}
		})
		t.Run("Fail", func(t *testing.T) {
			ar := NewArray(&Value{})
			ln, err := ar.Validate()
			if err == nil {
				t.Fatal("error should not be nill")
			}
			if ln != 0 {
				t.Fatal("should be zero")
			}
		})
		t.Run("Marshal", func(t *testing.T) {
			_, err := NewArray(&Value{}).MarshalBSON()
			if err == nil {
				t.Error("error should not be nil")
			}
		})

	})
	t.Run("Lookup", func(t *testing.T) {
		t.Run("FindValue", func(t *testing.T) {
			ar := NewArray(VC.Int(42), VC.Int(84))

			if val, err := ar.Lookup(0); err != nil || val.Int() != 42 {
				t.Error("values should be equal")
			}
			if val, err := ar.Lookup(1); err != nil || val.Int() != 84 {
				t.Error("values should be equal")
			}
		})
		t.Run("MissingValue", func(t *testing.T) {
			ar := NewArray(VC.Int(42), VC.Int(84))

			_, err := ar.Lookup(3)
			if err == nil {
				t.Error("expected error")
			}
		})
	})
	t.Run("InterfaceExport", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			ar := NewArray()
			if len(ar.Interface()) != 0 {
				t.Errorf("length should be %d", 0)
			}
		})
		t.Run("Value", func(t *testing.T) {
			slice := NewArray(VC.Int(42), VC.Int(84)).Interface()

			if len(slice) != 2 {
				t.Errorf("length should be %d", 2)
			}
			if slice[0].(int32) != 42 {
				t.Fatalf("values are not equal %v and %v", slice[0], 42)
			}
			if slice[1].(int32) != 84 {
				t.Fatalf("values are not equal %v and %v", slice[1], 84)
			}
		})
	})
	t.Run("String", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			ar := NewArray()
			if "bson.Array[]" != ar.String() {
				t.Error("values should be equal")
			}
		})
		t.Run("Content", func(t *testing.T) {
			ar := NewArray(VC.String("hello"), VC.String("world"))
			if "bson.Array[hello, world]" != ar.String() {
				t.Error("values should be equal")
			}
		})
	})
	t.Run("Set", func(t *testing.T) {
		t.Run("OutOfBounds", func(t *testing.T) {
			ar := NewArray()
			func() {
				defer func() {
					if p := recover(); p == nil {
						t.Fatal("expected panic")
					}
				}()
				ar.Set(10, VC.String("hi"))
			}()
		})
		t.Run("Empty", func(t *testing.T) {
			ar := NewArray()
			func() {
				defer func() {
					if p := recover(); p == nil {
						t.Fatal("expected panic")
					}
				}()
				ar.Set(0, VC.String("hi"))
			}()
		})
		t.Run("Replace", func(t *testing.T) {
			ar := NewArray(VC.Int(42))
			if val, err := ar.Lookup(0); err != nil || 42 != val.Interface().(int32) {
				t.Fatalf("values are not equal %v and %v", 42, val.Interface())
			}
			ar.Set(0, VC.Int(84))
			if val, err := ar.Lookup(0); err != nil || 84 != val.Interface().(int32) {
				t.Fatalf("values are not equal %v and %v", 42, val.Interface())
			}
		})
	})
}

type testArrayPrependAppendGenerator struct{}

var tapag testArrayPrependAppendGenerator

func (testArrayPrependAppendGenerator) oneOne() [][]*Value {
	return [][]*Value{
		{VC.Double(3.14159)},
	}
}

func (testArrayPrependAppendGenerator) oneOneBytes(index uint) []byte {
	a := []byte{
		// size
		0x0, 0x0, 0x0, 0x0,
		// type
		0x1,
	}

	// key
	a = append(a, []byte(strconv.FormatUint(uint64(index), 10))...)
	a = append(a, 0)

	a = append(a,
		// value
		0x6e, 0x86, 0x1b, 0xf0, 0xf9, 0x21, 0x9, 0x40,
		// null terminator
		0x0,
	)

	a[0] = byte(len(a))

	return a
}

func (testArrayPrependAppendGenerator) twoOne() [][]*Value {
	return [][]*Value{
		{VC.Double(1.234)},
		{VC.Double(5.678)},
	}
}

func (testArrayPrependAppendGenerator) twoOneAppendBytes(index uint) []byte {
	a := []byte{
		// size
		0x0, 0x0, 0x0, 0x0,
		// type
		0x1,
	}

	// key
	a = append(a, []byte(strconv.FormatUint(uint64(index), 10))...)
	a = append(a, 0)

	a = append(a,
		// value
		0x58, 0x39, 0xb4, 0xc8, 0x76, 0xbe, 0xf3, 0x3f,
		// type
		0x1,
	)

	// key
	a = append(a, []byte(strconv.FormatUint(uint64(index+1), 10))...)
	a = append(a, 0)

	a = append(a,
		// value
		0x83, 0xc0, 0xca, 0xa1, 0x45, 0xb6, 0x16, 0x40,
		// null terminator
		0x0,
	)

	a[0] = byte(len(a))

	return a
}

func (testArrayPrependAppendGenerator) twoOnePrependBytes(index uint) []byte {
	a := []byte{
		// size
		0x0, 0x0, 0x0, 0x0,
		// type
		0x1,
	}

	// key
	a = append(a, []byte(strconv.FormatUint(uint64(index), 10))...)
	a = append(a, 0)

	a = append(a,
		// value
		0x83, 0xc0, 0xca, 0xa1, 0x45, 0xb6, 0x16, 0x40,
		// type
		0x1,
	)

	// key
	a = append(a, []byte(strconv.FormatUint(uint64(index+1), 10))...)
	a = append(a, 0)

	a = append(a,
		// value
		0x58, 0x39, 0xb4, 0xc8, 0x76, 0xbe, 0xf3, 0x3f,
		// null terminator
		0x0,
	)

	a[0] = byte(len(a))

	return a
}

func ExampleArray() {
	internalVersion := "1234567"

	f := func(appName string) *Array {
		arr := NewArray()
		arr.Append(
			VC.DocumentFromElements(
				EC.String("name", "mongo-go-driver"),
				EC.String("version", internalVersion),
			),
			VC.DocumentFromElements(
				EC.String("type", "darwin"),
				EC.String("architecture", "amd64"),
			),
			VC.String("go1.9.2"),
		)

		if appName != "" {
			arr.Append(VC.DocumentFromElements(EC.String("name", appName)))
		}

		return arr
	}

	buf, err := f("hello-world").MarshalBSON()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(buf)

	// Output: [154 0 0 0 3 48 0 52 0 0 0 2 110 97 109 101 0 16 0 0 0 109 111 110 103 111 45 103 111 45 100 114 105 118 101 114 0 2 118 101 114 115 105 111 110 0 8 0 0 0 49 50 51 52 53 54 55 0 0 3 49 0 46 0 0 0 2 116 121 112 101 0 7 0 0 0 100 97 114 119 105 110 0 2 97 114 99 104 105 116 101 99 116 117 114 101 0 6 0 0 0 97 109 100 54 52 0 0 2 50 0 8 0 0 0 103 111 49 46 57 46 50 0 3 51 0 27 0 0 0 2 110 97 109 101 0 12 0 0 0 104 101 108 108 111 45 119 111 114 108 100 0 0 0]
}
