package birch

import (
	"sort"

	"github.com/tychoish/birch/bsonerr"
	"github.com/tychoish/birch/bsontype"
)

// MarshalDocument satisfies the DocumentMarshaler interface, and
// returns the document itself.
func (d *Document) MarshalDocument() (*Document, error) { return d, nil }

// UnmarshalDocument satisfies the DocumentUnmarshaler interface and
// appends the elements of the input document to the underlying
// document. If the document is populated this could result in a
// document that has multiple identical keys.
func (d *Document) UnmarshalDocument(in *Document) error {
	iter := in.Iterator()

	for iter.Next(iterCtx) {
		d.Append(iter.Value())
	}

	return iter.Close()
}

// ExportMap converts the values of the document to a map of strings
// to interfaces, recursively, using the Value.Interface() method.
func (d *Document) ExportMap() map[string]any {
	out := make(map[string]any, d.Len())

	iter := d.Iterator()

	for iter.Next(iterCtx) {
		elem := iter.Value()
		out[elem.Key()] = elem.Value().Interface()
	}

	return out
}

// Elements is a representation of a slice of elements, and implements
// the sort.Interface to support ordering the keys of a document.
type Elements []*Element

func (c Elements) Len() int      { return len(c) }
func (c Elements) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c Elements) Less(i, j int) bool {
	ik := c[i].Key()
	jk := c[j].Key()

	if ik != jk {
		return ik < jk
	}

	it := c[i].value.Type()
	jt := c[j].value.Type()

	if it != jt {
		return it < jt
	}

	switch it {
	case bsontype.Double:
		return c[i].Value().Double() < c[j].Value().Double()
	case bsontype.String:
		return c[i].Value().StringValue() < c[j].Value().StringValue()
	case bsontype.ObjectID:
		return c[i].Value().ObjectID().Hex() < c[j].Value().ObjectID().Hex()
	case bsontype.DateTime:
		return c[i].Value().Time().Before(c[j].Value().Time())
	case bsontype.Int32:
		return c[i].Value().Int32() < c[j].Value().Int32()
	case bsontype.Int64:
		return c[i].Value().Int64() < c[j].Value().Int64()
	default:
		return false
	}
}

// Copy returns a new Elements slice with the same underlying
// Elements. The copy is "shallow."
func (c Elements) Copy() Elements {
	out := make(Elements, len(c))
	for idx := range c {
		out[idx] = c[idx]
	}

	return out
}

// Elements provides access to a slice of the Elements in the
// document. Mutating this list will mutate the content of the
// document.
func (d *Document) Elements() Elements {
	return d.elems
}

// Sorted returns a new document containing a (shallow copy) of the
// elements from the source document ordered according to their value.
func (d *Document) Sorted() *Document {
	elems := d.Elements().Copy()

	sort.Stable(elems)

	return DC.Elements(elems...)
}

// LookupElement iterates through the elements in a document looking
// for one with the correct key and returns that element. It is NOT
// recursive. When the element is not defined, the return value
// is nil.
func (d *Document) LookupElement(key string) *Element {
	iter := d.Iterator()

	for iter.Next(iterCtx) {
		elem := iter.Value()

		if elem.Key() == key {
			return elem
		}
	}

	return nil
}

// Lookup iterates through the elements in a document looking
// for one with the correct key and returns the value for that key. It
// is NOT recursive. When the element is not present.
func (d *Document) Lookup(key string) *Value {
	elem := d.LookupElement(key)
	if elem == nil {
		return nil
	}

	return elem.value
}

// LookupElementErr iterates through the elements in a document looking
// for one with the correct key and returns the Element for that key. It
// is NOT recursive. When the element is not defined, it returns a
// ElementNotFound error.
func (d *Document) LookupElementErr(key string) (*Element, error) {
	elem := d.LookupElement(key)
	if elem == nil {
		return nil, bsonerr.ElementNotFound
	}

	return elem, nil
}

// LookupErr iterates through the elements in a document looking
// for one with the correct key and returns the value for that key. It
// is NOT recursive. When the element is not defined, it returns a
// ElementNotFound error.
func (d *Document) LookupErr(key string) (*Value, error) {
	elem := d.LookupElement(key)
	if elem == nil {
		return nil, bsonerr.ElementNotFound
	}

	return elem.value, nil
}
