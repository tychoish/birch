package jsonx

import (
	"encoding/json"
	"fmt"

	"errors"

	"github.com/tychoish/birch/jsonx/internal"
)

func (d *Document) UnmarshalJSON(in []byte) error {
	res, err := internal.ParseBytes(in)
	if err != nil {
		return fmt.Errorf("problem parsing raw json: %w", err)
	}

	if !res.IsObject() {
		return errors.New("cannot unmarshal values or arrays into Documents")
	}

	res.ForEach(func(key, value internal.Result) bool {
		var val *Value
		val, err = getValueForResult(value)
		if err != nil {
			return false
		}

		d.Append(EC.Value(key.Str, val))
		return true
	})

	if err != nil {
		return err
	}

	return nil
}

func (a *Array) UnmarshalJSON(in []byte) error {
	res, err := internal.ParseBytes(in)
	if err != nil {
		return fmt.Errorf("problem parsing raw json: %w", err)
	}

	if !res.IsArray() {
		return errors.New("cannot unmarshal a non-arrays into an array")
	}

	for _, item := range res.Array() {
		val, err := getValueForResult(item)
		if err != nil {
			return err
		}

		a.Append(val)
	}

	return nil
}

func (v *Value) UnmarshalJSON(in []byte) error {
	res, err := internal.ParseBytes(in)
	if err != nil {
		return fmt.Errorf("problem parsing raw json: %w", err)
	}

	out, err := getValueForResult(res)
	if err != nil {
		return err
	}

	v.value = out.value
	v.t = out.t
	return nil
}

///////////////////////////////////
//
// Internal

func getValueForResult(value internal.Result) (*Value, error) {
	switch {
	case value.Type == internal.String:
		return VC.String(value.Str), nil
	case value.Type == internal.Null:
		return VC.Nil(), nil
	case value.Type == internal.True:
		return VC.Boolean(true), nil
	case value.Type == internal.False:
		return VC.Boolean(false), nil
	case value.Type == internal.Number:
		num := json.Number(value.String())
		if igr, err := num.Int64(); err == nil {
			return VC.Int(int(igr)), nil
		} else if df, err := num.Float64(); err == nil {
			return VC.Float64(df), nil
		}

		return nil, fmt.Errorf("number value [%s] is invalid [%+v]", value.Str, value)
	case value.IsArray():
		source := value.Array()
		array := AC.Make(len(source))

		for _, elem := range source {
			val, err := getValueForResult(elem)
			if err != nil {
				return nil, err
			}

			array.Append(val)
		}

		return VC.Array(array), nil
	case value.IsObject():
		var err error
		var doc = DC.New()

		value.ForEach(func(key, value internal.Result) bool {
			var val *Value
			val, err = getValueForResult(value)
			if err != nil {
				err = fmt.Errorf("problem with subdocument at key %q: %w", key.Str, err)
				return false
			}
			doc.Append(EC.Value(key.Str, val))
			return true
		})

		if err != nil {
			return nil, err
		}

		return VC.Object(doc), nil
	default:
		return nil, fmt.Errorf("unknown json value type '%s'", value.Type)
	}
}
