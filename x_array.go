package birch

import "github.com/tychoish/fun/irt"

// Interface returns a slice of any typed values for every
// element in the array using the Value.Interface() method to
// export. the values.
func (a *Array) Interface() []any {
	return irt.Collect(irt.Convert(a.Iterator(), func(v *Value) any { return v.Interface() }))
}
