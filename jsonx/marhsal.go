package jsonx

import (
	"encoding/json"
	"fmt"

	"errors"
)

func (d *Document) MarshalJSON() ([]byte, error) {
	out := []byte{'{'}

	first := true
	for _, elem := range d.elems {
		if !first {
			out = append(out, ',')
		}

		out = append(out, []byte(fmt.Sprintf(`"%s":`, elem.key))...)

		val, err := elem.value.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("problem marshaling value for key %q: %w", elem.key, err)
		}

		out = append(out, val...)

		first = false
	}

	return append(out, '}'), nil
}

func (a *Array) MarshalJSON() ([]byte, error) {
	if a == nil {
		return nil, errors.New("cannot marshal nil array")
	}

	out := []byte{'['}

	first := true
	for idx, elem := range a.elems {
		if !first {
			out = append(out, ',')
		}

		val, err := elem.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("problem marshaling array value for index %d: %w", idx, err)
		}

		out = append(out, val...)

		first = false
	}

	return append(out, ']'), nil
}

func (v *Value) MarshalJSON() ([]byte, error) {
	if v == nil {
		return nil, errors.New("cannot marshal nil value")
	}

	switch v.t {
	case String:
		return writeJSONString([]byte(fmt.Sprintf(`%s`, v.value))), nil
	case NumberDouble, NumberInteger, Number:
		switch v.value.(type) {
		case int64, int32, int:
			return []byte(fmt.Sprintf(`%d`, v.value)), nil
		case float64, float32:
			return []byte(fmt.Sprintf(`%f`, v.value)), nil
		default:
			return nil, fmt.Errorf("unsupported number type %T", v.value)
		}
	case Null:
		return []byte("null"), nil
	case Bool:
		switch bv := v.value.(type) {
		case bool:
			if bv {
				return []byte("true"), nil
			}

			return []byte("false"), nil
		default:
			return nil, fmt.Errorf("unsupported bool type %T", bv)
		}
	case ArrayValue, ObjectValue:
		switch obj := v.value.(type) {
		case json.Marshaler:
			return obj.MarshalJSON()
		default:
			return nil, fmt.Errorf("unsupported object value type %T", obj)
		}
	default:
		return nil, fmt.Errorf("unknown type=%s", v.t)
	}
}
