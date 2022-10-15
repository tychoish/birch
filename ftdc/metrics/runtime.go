package metrics

import (
	"github.com/tychoish/birch"
)

func (r *Runtime) MarshalDocument() (*birch.Document, error) {
	doc := birch.DC.Elements(
		birch.EC.Int("id", r.ID),
		birch.EC.Time("ts", r.Timestamp),
		birch.EC.Int("pid", r.PID))

	if r.Golang != nil {
		doc.Append(birch.EC.DocumentMarshaler("golang", r.Golang))
	}

	if r.System != nil {
		doc.Append(birch.EC.DocumentMarshaler("system", r.System))
	}

	if r.Process != nil {
		doc.Append(birch.EC.DocumentMarshaler("process", r.Process))
	}

	return doc, nil
}
