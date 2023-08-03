package hdrhist

import (
	"encoding/json"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/x/ftdc/util"
	"github.com/tychoish/fun/dt/hdrhist"
)

type (
	Histogram struct{ *hdrhist.Histogram }
	Snapshot  struct{ *hdrhist.Snapshot }
)

func (h *Histogram) MarshalDocument() (*birch.Document, error) {
	ss := h.Export()

	return birch.DC.Make(5).Append(
		birch.EC.Int64("lowest", ss.LowestTrackableValue),
		birch.EC.Int64("highest", ss.HighestTrackableValue),
		birch.EC.Int64("figures", ss.SignificantFigures),
		birch.EC.SliceInt64("counts", ss.Counts),
	), nil
}

func (h *Histogram) MarshalBSON() ([]byte, error) { return birch.MarshalDocumentBSON(h) }
func (h *Histogram) MarshalJSON() ([]byte, error) { return json.Marshal(h.Export()) }

func (h *Histogram) UnmarshalBSON(in []byte) error {
	s := &hdrhist.Snapshot{}
	if err := util.GlobalUnmarshaler()(in, s); err != nil {
		return err
	}

	*h = Histogram{hdrhist.Import(s)}
	return nil
}

func (h *Histogram) UnmarshalJSON(in []byte) error {
	s := &hdrhist.Snapshot{}
	if err := json.Unmarshal(in, s); err != nil {
		return err
	}

	*h = Histogram{hdrhist.Import(s)}
	return nil
}
