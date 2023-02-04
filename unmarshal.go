package birch

import (
	"fmt"

	"github.com/tychoish/fun/erc"
)

// DocumentMap is a map-based view of a document, used for
// key-lookups, and providing another option in document construction.
type DocumentMap map[string]*Element

// Copy returns a new map with the same values as the source map, but
// in a different map object that callers have ownership over.
func (dm DocumentMap) Copy() DocumentMap {
	out := make(DocumentMap, len(dm))
	for key := range dm {
		out[key] = dm[key]
	}
	return out
}

// Validate checks the validity of each element in the DocumentMap,
// and ensures that every key in the map matches the key value of the
// element itself.
func (dm DocumentMap) Validate() error {
	ec := &erc.Collector{}
	for key := range dm {
		elem := dm[key]
		ekey := elem.Key()

		erc.Whenf(ec, ekey != key, "map key %q not equal to element key %q", key, ekey)

		if _, err := elem.Validate(); err != nil {
			ec.Add(fmt.Errorf("for mapKey=%q, invalid element: %w", ekey, err))
		}
	}

	return ec.Resolve()
}

func (dm DocumentMap) MarshalDocument() (*Document, error) {
	if err := dm.Validate(); err != nil {
		return nil, err
	}

	doc := DC.Make(len(dm))
	for key := range dm {
		doc.Append(dm[key])
	}
	return doc, nil
}

func (dm DocumentMap) UnmarshalDocument(in *Document) error {
	iter := in.Iterator()
	for iter.Next(iterCtx) {
		elem := iter.Value()
		if _, err := elem.Validate(); err != nil {
			return err
		}
		dm[elem.Key()] = elem
	}
	return iter.Close(iterCtx)
}

func (dm DocumentMap) MarshalBSON() ([]byte, error) {
	doc, err := dm.MarshalDocument()
	if err != nil {
		return nil, err
	}
	return doc.MarshalBSON()

}

func (dm DocumentMap) UnmarshalBSON(in []byte) error {
	iter, err := Reader(in).Iterator()
	if err != nil {
		return err

	}
	for iter.Next(iterCtx) {
		elem := iter.Value()
		if _, err := elem.Validate(); err != nil {
			return err
		}
		dm[elem.Key()] = elem
	}

	return nil
}

// Map returns a map-based view of the document. If a key appears in a
// document more than once only the first occurrence is
// included. birch.Document's cache the document map, so as logn as
// the document doesn't change rebuilding the map is cheap.
//
// The object returned contains pointers to the underlying elements of
// the document, but is a copy of the cached map, and multiple calls
// to Map() will return different maps.
func (d *Document) Map() DocumentMap {
	d.refreshCache()
	return d.cache.Copy()
}

func (d *Document) refreshCache() {
	if d.cacheValid {
		return
	}

	out := make(DocumentMap, len(d.elems))
	for idx := range d.elems {
		key := d.elems[idx].Key()
		if _, ok := out[key]; ok {
			continue
		}
		out[key] = d.elems[idx]
	}

	d.cache = out
	d.cacheValid = true
}
