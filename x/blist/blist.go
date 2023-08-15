package blist

import (
	"github.com/tychoish/birch"
	"github.com/tychoish/birch/bsonerr"
	"github.com/tychoish/fun/dt"
)

type Document struct {
	dt.List[*birch.Element]
}

func (d *Document) Len() int { return d.Len() }
func (d *Document) Append(elems ...*birch.Element) *Document {
	for idx := range elems {
		if elems[idx] == nil {
			panic(bsonerr.NilElement)
		}

		d.PushBack(elems[idx])
	}
	return d
}
func (d *Document) AppendOmitEmpty(elems ...*birch.Element) *Document {
	for idx := range elems {
		if elems[idx] == nil || elems[idx].Value().IsEmpty() {
			continue
		}
		d.PushBack(elems[idx])
	}
	return d
}
func (d *Document) Prepend(elems ...*birch.Element) *Document {
	for idx := range elems {
		if elems[idx] == nil {
			panic(bsonerr.NilElement)
		}
		d.PushFront(elems[idx])
	}
	return d
}

func (d *Document) Set(elem *birch.Element) *Document {
	for e := d.Front(); e.OK(); e = e.Next() {
		if e.Value().Key() != elem.Key() {
			continue
		}
		e.Set(elem)
		break
	}
	return d
}

func (d *Document) Delete(key string) *birch.Element {
	for e := d.Front(); e.OK(); e = e.Next() {
		if e.Value().Key() != key {
			continue
		}
		if e.Remove() {
			return e.Value()
		}
	}
	return nil
}

func (d *Document) Validate() (uint32, error) {
	if d == nil {
		return 0, bsonerr.NilDocument
	}

	// header and footer
	var size uint32 = 4 + 1

	for e := d.Front(); e.OK(); e = e.Next() {
		n, err := e.Value().Validate()
		if err != nil {
			return 0, err
		}

		size += n
	}

	return size, nil
}
