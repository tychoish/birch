package birch

import "context"

// Interface returns a slice of interface{} typed values for every
// element in the array using the Value.Interface() method to
// export. the values.
func (a *Array) Interface() []interface{} {
	out := make([]interface{}, 0, a.Len())
	iter := a.Iterator()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for iter.Next(ctx) {
		out = append(out, iter.Value().Interface())
	}

	return out
}
