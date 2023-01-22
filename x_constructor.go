package birch

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/tychoish/birch/jsonx"
)

// DC is a convenience variable provided for access to the DocumentConstructor methods.
var DC DocumentConstructor

// DocumentConstructor is used as a namespace for document constructor functions.
type DocumentConstructor struct{}

// New returns an empty document.
func (DocumentConstructor) New() *Document { return DC.Make(0) }

// Make returns a document with the underlying storage
// allocated as specified. Provides some efficiency when building
// larger documents iteratively.
func (DocumentConstructor) Make(n int) *Document {
	return &Document{
		elems: make([]*Element, 0, n),
	}
}

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
	doc, err := DC.ReaderErr(r)
	if err != nil {
		panic(err)
	}

	return doc
}

// ReaderErr constructs a document from a bson reader, which is a wrapper
// around a byte slice representation of a bson document. Reader
// returns an error if there is a problem reading the document.
func (DocumentConstructor) ReaderErr(r Reader) (*Document, error) {
	return ReadDocument(r)
}

// ReadFrom builds a document reading a bytes sequence from an
// io.Reader, panicing if there's a problem reading from the reader.
func (DocumentConstructor) ReadFrom(in io.Reader) *Document {
	doc, err := DC.ReadFromErr(in)
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
func (DocumentConstructor) ReadFromErr(in io.Reader) (*Document, error) {
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
	doc, err := DC.MarshalerErr(in)
	if err != nil {
		panic(err)
	}

	return doc
}

func (DocumentConstructor) MarshalerErr(in Marshaler) (*Document, error) {
	data, err := in.MarshalBSON()
	if err != nil {
		return nil, err
	}

	return DC.ReaderErr(data)
}

func (DocumentConstructor) MapString(in map[string]string) *Document {
	out := DC.Make(len(in))

	for k, v := range in {
		out.Append(EC.String(k, v))
	}

	return out
}

func (DocumentConstructor) MapJSONX(in map[string]*jsonx.Document) *Document {
	out := DC.Make(len(in))

	for k, v := range in {
		out.Append(EC.JSONX(k, v))
	}

	return out
}

func (DocumentConstructor) MapInterface(in map[string]interface{}) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Interface(k, v))
	}

	return out
}

func (DocumentConstructor) MapInterfaceErr(in map[string]interface{}) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := EC.InterfaceErr(k, v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapInterfaceInterfaceErr(in map[interface{}]interface{}) (*Document, error) {
	out := DC.Make(len(in))
	for k, v := range in {
		elem, err := EC.InterfaceErr(bestStringAttempt(k), v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapInterfaceInterface(in map[interface{}]interface{}) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Interface(bestStringAttempt(k), v))
	}
	return out
}

func (DocumentConstructor) MapInt64(in map[string]int64) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Int64(k, v))
	}

	return out
}

func (DocumentConstructor) MapInt32(in map[string]int32) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Int32(k, v))
	}

	return out
}

func (DocumentConstructor) MapFloat64(in map[string]float64) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Double(k, v))
	}

	return out
}

func (DocumentConstructor) MapFloat32(in map[string]float32) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Double(k, float64(v)))
	}

	return out
}

func (DocumentConstructor) MapInt(in map[string]int) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Int(k, v))
	}

	return out
}

func (DocumentConstructor) MapTime(in map[string]time.Time) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Time(k, v))
	}

	return out
}

func (DocumentConstructor) MapDuration(in map[string]time.Duration) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Duration(k, v))
	}

	return out
}

func (DocumentConstructor) MapMarshaler(in map[string]Marshaler) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.Marshaler(k, v))
	}

	return out
}

func (DocumentConstructor) MapMarshalerErr(in map[string]Marshaler) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := EC.MarshalerErr(k, v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapSliceMarshaler(in map[string][]Marshaler) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceMarshaler(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceMarshalerErr(in map[string][]Marshaler) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := EC.SliceMarshalerErr(k, v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapDocumentMarshaler(in map[string]DocumentMarshaler) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.DocumentMarshaler(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceJSONX(in map[string][]*jsonx.Document) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceJSONX(k, v))
	}

	return out
}

func (DocumentConstructor) MapDocumentMarshalerErr(in map[string]DocumentMarshaler) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := EC.DocumentMarshalerErr(k, v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapSliceDocumentMarshaler(in map[string][]DocumentMarshaler) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceDocumentMarshaler(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceDocumentMarshalerErr(in map[string][]DocumentMarshaler) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := EC.SliceDocumentMarshalerErr(k, v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapSliceString(in map[string][]string) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceString(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceInterface(in map[string][]interface{}) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceInterface(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceInterfaceErr(in map[string][]interface{}) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := EC.SliceInterfaceErr(k, v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapInterfaceSliceInterface(in map[interface{}][]interface{}) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceInterface(bestStringAttempt(k), v))
	}

	return out
}

func (DocumentConstructor) MapInterfaceSliceInterfaceErr(in map[interface{}][]interface{}) (*Document, error) {
	out := DC.Make(len(in))

	for k, v := range in {
		elem, err := EC.SliceInterfaceErr(bestStringAttempt(k), v)
		if err != nil {
			return nil, err
		}

		if elem != nil {
			out.Append(elem)
		}
	}

	return out, nil
}

func (DocumentConstructor) MapSliceInt64(in map[string][]int64) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceInt64(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceInt32(in map[string][]int32) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceInt32(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceFloat64(in map[string][]float64) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceFloat64(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceFloat32(in map[string][]float32) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceFloat32(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceInt(in map[string][]int) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceInt(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceDuration(in map[string][]time.Duration) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceDuration(k, v))
	}

	return out
}

func (DocumentConstructor) MapSliceTime(in map[string][]time.Time) *Document {
	out := DC.Make(len(in))
	for k, v := range in {
		out.Append(EC.SliceTime(k, v))
	}

	return out
}

func (DocumentConstructor) Interface(value interface{}) *Document {
	var (
		doc *Document
		err error
	)

	switch t := value.(type) {
	case map[string]string:
		doc = DC.MapString(t)
	case map[string][]string:
		doc = DC.MapSliceString(t)
	case map[string]interface{}:
		doc = DC.MapInterface(t)
	case map[string][]interface{}:
		doc = DC.MapSliceInterface(t)
	case map[string]DocumentMarshaler:
		doc, err = DC.MapDocumentMarshalerErr(t)
	case map[string][]DocumentMarshaler:
		doc, err = DC.MapSliceDocumentMarshalerErr(t)
	case map[string]Marshaler:
		doc, err = DC.MapMarshalerErr(t)
	case map[string][]Marshaler:
		doc, err = DC.MapSliceMarshalerErr(t)
	case map[string]int64:
		doc = DC.MapInt64(t)
	case map[string][]int64:
		doc = DC.MapSliceInt64(t)
	case map[string]int32:
		doc = DC.MapInt32(t)
	case map[string][]int32:
		doc = DC.MapSliceInt32(t)
	case map[string]int:
		doc = DC.MapInt(t)
	case map[string][]int:
		doc = DC.MapSliceInt(t)
	case map[string]time.Time:
		doc = DC.MapTime(t)
	case map[string][]time.Time:
		doc = DC.MapSliceTime(t)
	case map[string]time.Duration:
		doc = DC.MapDuration(t)
	case map[string][]time.Duration:
		doc = DC.MapSliceDuration(t)
	case map[interface{}]interface{}:
		doc = DC.Make(len(t))
		for k, v := range t {
			doc.Append(EC.Interface(bestStringAttempt(k), v))
		}
	case *Element:
		doc = DC.Elements(t)
	case *Document:
		doc = t
	case Reader:
		doc, err = DC.ReaderErr(t)
	case DocumentMarshaler:
		doc, err = t.MarshalDocument()
	case Marshaler:
		doc, err = DC.MarshalerErr(t)
	case []*Element:
		doc = DC.Elements(t...)
	}

	if err != nil || doc == nil {
		return DC.New()
	}

	return doc
}

func (DocumentConstructor) InterfaceErr(value interface{}) (*Document, error) {
	switch t := value.(type) {
	case map[string]string, map[string][]string, map[string]int64, map[string][]int64, map[string]int32, map[string][]int32, map[string]int, map[string][]int, map[string]time.Time, map[string][]time.Time, map[string]time.Duration, map[string][]time.Duration:
		return DC.Interface(t), nil
	case map[string]Marshaler:
		return DC.MapMarshalerErr(t)
	case map[string][]Marshaler:
		return DC.MapSliceMarshalerErr(t)
	case map[string]DocumentMarshaler:
		return DC.MapDocumentMarshalerErr(t)
	case map[string][]DocumentMarshaler:
		return DC.MapSliceDocumentMarshalerErr(t)
	case map[string]interface{}:
		return DC.MapInterfaceErr(t)
	case map[string][]interface{}:
		return DC.MapSliceInterfaceErr(t)
	case map[interface{}]interface{}:
		return DC.MapInterfaceInterfaceErr(t)
	case map[interface{}][]interface{}:
		return DC.MapInterfaceSliceInterfaceErr(t)
	case Reader:
		return DC.ReaderErr(t)
	case *Element:
		return DC.Elements(t), nil
	case []*Element:
		return DC.Elements(t...), nil
	case *Document:
		return t, nil
	case DocumentMarshaler:
		return t.MarshalDocument()
	case Marshaler:
		return DC.MarshalerErr(t)
	default:
		return nil, fmt.Errorf("value '%s' is of type '%T' which is not convertable to a document.", t, t)
	}
}

func (ElementConstructor) Marshaler(key string, val Marshaler) *Element {
	elem, err := EC.MarshalerErr(key, val)
	if err != nil {
		panic(err)
	}

	return elem
}

func (ElementConstructor) MarshalerErr(key string, val Marshaler) (*Element, error) {
	doc, err := val.MarshalBSON()
	if err != nil {
		return nil, err
	}

	return EC.SubDocumentFromReader(key, doc), nil
}

func (ElementConstructor) DocumentMarshaler(key string, val DocumentMarshaler) *Element {
	doc, err := val.MarshalDocument()
	if err != nil {
		panic(err)
	}

	return EC.SubDocument(key, doc)
}

func (ElementConstructor) DocumentMarshalerErr(key string, val DocumentMarshaler) (*Element, error) {
	doc, err := val.MarshalDocument()
	if err != nil {
		return nil, err
	}

	return EC.SubDocument(key, doc), nil
}

func (ElementConstructor) JSONXErr(key string, val *jsonx.Document) (*Element, error) {
	doc, err := DC.JSONXErr(val)
	if err != nil {
		return nil, err
	}
	return EC.SubDocument(key, doc), nil
}

func (ElementConstructor) JSONX(key string, val *jsonx.Document) *Element {
	return EC.SubDocument(key, DC.JSONX(val))
}

func (ElementConstructor) Int(key string, i int) *Element {
	if i < math.MaxInt32 {
		return EC.Int32(key, int32(i))
	}

	return EC.Int64(key, int64(i))
}

func (ElementConstructor) SliceJSONX(key string, in []*jsonx.Document) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.JSONX(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceString(key string, in []string) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.String(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInterface(key string, in []interface{}) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Interface(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInterfaceErr(key string, in []interface{}) (*Element, error) {
	vals := make([]*Value, 0, len(in))

	for idx := range in {
		elem, err := VC.InterfaceErr(in[idx])
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

func (ElementConstructor) SliceMarshaler(key string, in []Marshaler) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Marshaler(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceMarshalerErr(key string, in []Marshaler) (*Element, error) {
	vals := make([]*Value, 0, len(in))

	for idx := range in {
		val, err := VC.MarshalerErr(in[idx])
		if err != nil {
			return nil, err
		}

		if val != nil {
			vals = append(vals, val)
		}
	}

	return EC.Array(key, NewArray(vals...)), nil
}

func (ElementConstructor) SliceDocumentMarshaler(key string, in []DocumentMarshaler) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.DocumentMarshaler(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceDocumentMarshalerErr(key string, in []DocumentMarshaler) (*Element, error) {
	vals := make([]*Value, 0, len(in))

	for idx := range in {
		val, err := VC.DocumentMarshalerErr(in[idx])
		if err != nil {
			return nil, err
		}

		if val != nil {
			vals = append(vals, val)
		}
	}

	return EC.Array(key, NewArray(vals...)), nil
}

func (ElementConstructor) Duration(key string, t time.Duration) *Element {
	return EC.Int64(key, int64(t))
}

func (ValueConstructor) Int(in int) *Value {
	return EC.Int("", in).value
}

func (ValueConstructor) Interface(in interface{}) *Value {
	return EC.Interface("", in).value
}

func (ValueConstructor) InterfaceErr(in interface{}) (*Value, error) {
	elem, err := EC.InterfaceErr("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) Marshaler(in Marshaler) *Value {
	return EC.Marshaler("", in).value
}

func (ValueConstructor) MarshalerErr(in Marshaler) (*Value, error) {
	elem, err := EC.MarshalerErr("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) DocumentMarshaler(in DocumentMarshaler) *Value {
	return EC.DocumentMarshaler("", in).value
}

func (ValueConstructor) DocumentMarshalerErr(in DocumentMarshaler) (*Value, error) {
	elem, err := EC.DocumentMarshalerErr("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) JSONXErr(in *jsonx.Document) (*Value, error) {
	elem, err := EC.JSONXErr("", in)

	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) JSONX(in *jsonx.Document) *Value {
	return EC.JSONX("", in).value
}

func (ValueConstructor) Duration(t time.Duration) *Value {
	return VC.Int64(int64(t))
}

func (ValueConstructor) MapString(in map[string]string) *Value {
	return EC.SubDocument("", DC.MapString(in)).value
}

func (ValueConstructor) MapSliceString(in map[string][]string) *Value {
	return EC.SubDocument("", DC.MapSliceString(in)).value
}

func (ValueConstructor) MapInt(in map[string]int) *Value {
	return EC.SubDocument("", DC.MapInt(in)).value
}

func (ValueConstructor) MapInt32(in map[string]int32) *Value {
	return EC.SubDocument("", DC.MapInt32(in)).value
}

func (ValueConstructor) MapInt64(in map[string]int64) *Value {
	return EC.SubDocument("", DC.MapInt64(in)).value
}

func (ValueConstructor) MapFloat32(in map[string]float32) *Value {
	return EC.SubDocument("", DC.MapFloat32(in)).value
}

func (ValueConstructor) MapFloat64(in map[string]float64) *Value {
	return EC.SubDocument("", DC.MapFloat64(in)).value
}

func (ValueConstructor) MapSliceInt(in map[string][]int) *Value {
	return EC.SubDocument("", DC.MapSliceInt(in)).value
}

func (ValueConstructor) MapSliceInt32(in map[string][]int32) *Value {
	return EC.SubDocument("", DC.MapSliceInt32(in)).value
}

func (ValueConstructor) MapSliceInt64(in map[string][]int64) *Value {
	return EC.SubDocument("", DC.MapSliceInt64(in)).value
}

func (ValueConstructor) MapSliceFloat32(in map[string][]float32) *Value {
	return EC.SubDocument("", DC.MapSliceFloat32(in)).value
}

func (ValueConstructor) MapSliceFloat64(in map[string][]float64) *Value {
	return EC.SubDocument("", DC.MapSliceFloat64(in)).value
}

func (ValueConstructor) MapInterface(in map[string]interface{}) *Value {
	return EC.SubDocument("", DC.MapInterface(in)).value
}

func (ValueConstructor) MapInterfaceSliceInterface(in map[interface{}][]interface{}) *Value {
	return EC.SubDocument("", DC.MapInterfaceSliceInterface(in)).value
}

func (ValueConstructor) MapInterfaceInterface(in map[interface{}]interface{}) *Value {
	return EC.SubDocument("", DC.MapInterfaceInterface(in)).value
}

func (ValueConstructor) MapSliceInterface(in map[string][]interface{}) *Value {
	return EC.SubDocument("", DC.MapSliceInterface(in)).value
}

func (ValueConstructor) MapMarshaler(in map[string]Marshaler) *Value {
	return EC.SubDocument("", DC.MapMarshaler(in)).value
}

func (ValueConstructor) MapSliceMarshaler(in map[string][]Marshaler) *Value {
	return EC.SubDocument("", DC.MapSliceMarshaler(in)).value
}

func (ValueConstructor) MapTime(in map[string]time.Time) *Value {
	return EC.SubDocument("", DC.MapTime(in)).value
}

func (ValueConstructor) MapSliceTime(in map[string][]time.Time) *Value {
	return EC.SubDocument("", DC.MapSliceTime(in)).value
}

func (ValueConstructor) MapDuration(in map[string]time.Duration) *Value {
	return EC.SubDocument("", DC.MapDuration(in)).value
}

func (ValueConstructor) MapSliceDuration(in map[string][]time.Duration) *Value {
	return EC.SubDocument("", DC.MapSliceDuration(in)).value
}

func (ValueConstructor) MapInterfaceErr(in map[string]interface{}) (*Value, error) {
	doc, err := DC.MapInterfaceErr(in)
	if err != nil {
		return nil, err
	}

	return EC.SubDocument("", doc).value, nil
}

func (ValueConstructor) MapSliceInterfaceErr(in map[string][]interface{}) (*Value, error) {
	doc, err := DC.MapSliceInterfaceErr(in)
	if err != nil {
		return nil, err
	}

	return EC.SubDocument("", doc).value, nil
}

func (ValueConstructor) MapMarshalerErr(in map[string]Marshaler) (*Value, error) {
	doc, err := DC.MapMarshalerErr(in)
	if err != nil {
		return nil, err
	}

	return EC.SubDocument("", doc).value, nil
}

func (ValueConstructor) MapSliceMarshalerErr(in map[string][]Marshaler) (*Value, error) {
	doc, err := DC.MapSliceMarshalerErr(in)
	if err != nil {
		return nil, err
	}

	return EC.SubDocument("", doc).value, nil
}

func (ValueConstructor) MapJSONX(in map[string]*jsonx.Document) *Value {
	doc := DC.Make(len(in))
	for k, v := range in {
		doc.Append(EC.JSONX(k, v))
	}
	return EC.SubDocument("", doc).value
}

func (ValueConstructor) MapSliceJSONX(in map[string][]*jsonx.Document) *Value {
	doc := DC.Make(len(in))
	for k, v := range in {
		doc.Append(EC.SliceJSONX(k, v))
	}
	return EC.SubDocument("", doc).value
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

func (ValueConstructor) SliceMarshaler(in []Marshaler) *Value { return EC.SliceMarshaler("", in).value }
func (ValueConstructor) SliceInterface(in []interface{}) *Value {
	return EC.SliceInterface("", in).value
}

func (ValueConstructor) SliceMarshalerErr(in []Marshaler) (*Value, error) {
	elem, err := EC.SliceMarshalerErr("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}

func (ValueConstructor) SliceInterfaceErr(in []interface{}) (*Value, error) {
	elem, err := EC.SliceInterfaceErr("", in)
	if err != nil {
		return nil, err
	}

	return elem.value, nil
}
