package birch

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/tychoish/fun/ft"
)

// DC is a convenience variable provided for access to the DocumentConstructor methods.
var DC DocumentConstructor

// DCE is a convenience variable provided for access to the DocumentConstructorError methods.
var DCE DocumentConstructorError

// DocumentConstructor is used as a namespace for document constructor
// functions. Constructor methods may panic in cases when invalid
// input would cause them to error when using the DocumentConstructorError
type DocumentConstructor struct{}

// DocumentConstructor is used as a namespace for document constructor
// functions. These methods return errors rather than panicing in the
// case of invalid input
type DocumentConstructorError struct{}

// New returns an empty document.
func (DocumentConstructor) New() *Document { return DC.Make(0) }

// Make returns a document with the underlying storage
// allocated as specified. Provides some efficiency when building
// larger documents iteratively.
func (DocumentConstructor) Make(n int) *Document { return &Document{elems: make([]*Element, 0, n)} }

// Elements returns a document initialized with the elements passed as
// arguments.
func (DocumentConstructor) Elements(elems ...*Element) *Document {
	return DC.Make(len(elems)).Append(elems...)
}

// ElementsOmitEmpty crates a document with all non-empty values.
func (DocumentConstructor) ElementsOmitEmpty(elems ...*Element) *Document {
	return DC.Make(len(elems)).AppendOmitEmpty(elems...)
}

// Reader constructs a document from a bson reader, which is a wrapper
// around a byte slice representation of a bson document. Reader
// panics if there is a problem reading the document.
func (DocumentConstructor) Reader(r Reader) *Document {
	return ft.Must(DCE.Reader(r))
}

// ReaderErr constructs a document from a bson reader, which is a wrapper
// around a byte slice representation of a bson document. Reader
// returns an error if there is a problem reading the document.
func (DocumentConstructorError) Reader(r Reader) (*Document, error) {
	return ReadDocument(r)
}

// ReadFrom builds a document reading a bytes sequence from an
// io.Reader, panicing if there's a problem reading from the reader.
func (DocumentConstructor) ReadFrom(in io.Reader) *Document {
	doc, err := DCE.ReadFrom(in)
	if err == io.EOF {
		return nil
	}

	if err != nil {
		panic(err)
	}

	return doc
}

// ReadFromErr builds a document reading a bytes sequence from an
// io.Reader, returning an error if there's a problem reading from the
// reader.
func (DocumentConstructorError) ReadFrom(in io.Reader) (*Document, error) {
	doc := DC.New()

	_, err := doc.ReadFrom(in)
	if err == io.EOF {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return doc, nil
}

func (DocumentConstructor) Marshaler(in Marshaler) *Document {
	return ft.Must(DCE.Marshaler(in))
}

func (DocumentConstructorError) Marshaler(in Marshaler) (*Document, error) {
	data, err := in.MarshalBSON()
	if err != nil {
		return nil, err
	}

	return DCE.Reader(data)
}

func (DocumentConstructor) MapString(in map[string]string) *Document {
	out := DC.Make(len(in))

	for k, v := range in {
		out.Append(EC.String(k, v))
	}

	return out
}

func (DocumentConstructor) MapInterface(in map[string]any) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Interface(k, v))
	}

	return out
}

func (DocumentConstructorError) MapInterface(in map[string]any) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := ECE.Interface(k, v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructorError) MapInterfaceInterface(in map[any]any) (*Document, error) {
	out := DC.Make(len(in))
	for k, v := range in {
		elem, err := ECE.Interface(bestStringAttempt(k), v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapInterfaceInterface(in map[any]any) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Interface(bestStringAttempt(k), v))
	}
	return out
}

func (DocumentConstructor) Interface(value any) *Document {
	var (
		doc *Document
		err error
	)

	switch t := value.(type) {
	case map[string]string:
		doc = DC.MapString(t)
	case map[string]any:
		doc = DC.MapInterface(t)
	case map[any]any:
		doc = DC.Make(len(t))
		for k, v := range t {
			doc.Append(EC.Interface(bestStringAttempt(k), v))
		}
	case *Element:
		doc = DC.Elements(t)
	case *Document:
		doc = t
	case Reader:
		doc, err = DCE.Reader(t)
	case DocumentMarshaler:
		doc, err = t.MarshalDocument()
	case Marshaler:
		doc, err = DCE.Marshaler(t)
	case []*Element:
		doc = DC.Elements(t...)
	}

	if err != nil || doc == nil {
		return DC.New()
	}

	return doc
}

func (DocumentConstructorError) Interface(value any) (*Document, error) {
	switch t := value.(type) {
	case map[string]string:
		return DC.MapString(t), nil
	case map[string]any:
		return DCE.MapInterface(t)
	case map[any]any:
		return DCE.MapInterfaceInterface(t)
	case Reader:
		return DCE.Reader(t)
	case *Element:
		return DC.Elements(t), nil
	case []*Element:
		return DC.Elements(t...), nil
	case *Document:
		return t, nil
	case DocumentMarshaler:
		return t.MarshalDocument()
	case Marshaler:
		return DCE.Marshaler(t)
	default:
		return nil, fmt.Errorf("value '%s' is of type '%T' which is not convertable to a document.", t, t)
	}
}

func (ElementConstructor) Marshaler(key string, val Marshaler) *Element {
	elem, err := ECE.Marshaler(key, val)
	if err != nil {
		panic(err)
	}

	return elem
}

func (ElementConstructorError) Marshaler(key string, val Marshaler) (*Element, error) {
	doc, err := val.MarshalBSON()
	if err != nil {
		return nil, err
	}

	return EC.SubDocumentFromReader(key, doc), nil
}

func (ElementConstructor) DocumentMarshaler(key string, val DocumentMarshaler) *Element {
	return EC.SubDocument(key, ft.Must(val.MarshalDocument()))
}

func (ElementConstructorError) DocumentMarshaler(key string, val DocumentMarshaler) (*Element, error) {
	doc, err := val.MarshalDocument()
	if err != nil {
		return nil, err
	}

	return EC.SubDocument(key, doc), nil
}

func (ElementConstructor) Int(key string, i int) *Element {
	if i < math.MaxInt32 {
		return EC.Int32(key, int32(i))
	}

	return EC.Int64(key, int64(i))
}

func (ElementConstructor) SliceString(key string, in []string) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.String(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInterface(key string, in []any) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Interface(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructorError) SliceInterface(key string, in []any) (*Element, error) {
	vals := make([]*Value, 0, len(in))

	for idx := range in {
		elem, err := VCE.Interface(in[idx])
		if err != nil {
			return nil, err
		}

		if elem != nil {
			vals = append(vals, elem)
		}
	}

	return EC.Array(key, NewArray(vals...)), nil
}

func (ElementConstructor) SliceInt64(key string, in []int64) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int64(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInt32(key string, in []int32) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int32(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceFloat64(key string, in []float64) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Double(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceFloat32(key string, in []float32) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Double(float64(in[idx]))
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInt(key string, in []int) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceTime(key string, in []time.Time) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Time(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceDuration(key string, in []time.Duration) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int64(int64(in[idx]))
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) Duration(key string, t time.Duration) *Element {
	return EC.Int64(key, int64(t))
}

func (ValueConstructor) Int(in int) *Value {
	return EC.Int("", in).value
}

func (ValueConstructor) Interface(in any) *Value {
	return EC.Interface("", in).value
}

func (ValueConstructorError) Interface(in any) (*Value, error) {
	elem, err := ECE.Interface("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) Marshaler(in Marshaler) *Value {
	return EC.Marshaler("", in).value
}

func (ValueConstructorError) Marshaler(in Marshaler) (*Value, error) {
	elem, err := ECE.Marshaler("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) DocumentMarshaler(in DocumentMarshaler) *Value {
	return EC.DocumentMarshaler("", in).value
}

func (ValueConstructorError) DocumentMarshaler(in DocumentMarshaler) (*Value, error) {
	elem, err := ECE.DocumentMarshaler("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) Duration(t time.Duration) *Value {
	return VC.Int64(int64(t))
}

func (ValueConstructor) MapString(in map[string]string) *Value {
	return EC.SubDocument("", DC.MapString(in)).value
}

func (ValueConstructor) MapInterface(in map[string]any) *Value {
	return EC.SubDocument("", DC.MapInterface(in)).value
}

func (ValueConstructor) MapInterfaceInterface(in map[any]any) *Value {
	return EC.SubDocument("", DC.MapInterfaceInterface(in)).value
}

func (ValueConstructorError) MapInterface(in map[string]any) (*Value, error) {
	doc, err := DCE.MapInterface(in)
	if err != nil {
		return nil, err
	}

	return EC.SubDocument("", doc).value, nil
}

func (ValueConstructor) SliceString(in []string) *Value   { return EC.SliceString("", in).value }
func (ValueConstructor) SliceInt(in []int) *Value         { return EC.SliceInt("", in).value }
func (ValueConstructor) SliceInt64(in []int64) *Value     { return EC.SliceInt64("", in).value }
func (ValueConstructor) SliceInt32(in []int32) *Value     { return EC.SliceInt32("", in).value }
func (ValueConstructor) SliceFloat64(in []float64) *Value { return EC.SliceFloat64("", in).value }
func (ValueConstructor) SliceFloat32(in []float32) *Value { return EC.SliceFloat32("", in).value }
func (ValueConstructor) SliceTime(in []time.Time) *Value  { return EC.SliceTime("", in).value }
func (ValueConstructor) SliceDuration(in []time.Duration) *Value {
	return EC.SliceDuration("", in).value
}
func (ValueConstructor) SliceInterface(in []any) *Value {
	return EC.SliceInterface("", in).value
}

func (ValueConstructorError) SliceInterface(in []any) (*Value, error) {
	elem, err := ECE.SliceInterface("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}
