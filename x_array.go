package birch

// Interface returns a slice of any typed values for every
// element in the array using the Value.Interface() method to
// export. the values.
func (a *Array) Interface() []any {
	out := make([]any, 0, a.Len())
	iter := a.Iterator()

	for iter.Next(iterCtx) {
		out = append(out, iter.Value().Interface())
	}

	return out
}
