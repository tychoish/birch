package ftdc

import (
	"context"
	"io"
	"sync"
	"time"

	"errors"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/bsontype"
	"github.com/tychoish/birch/x/ftdc/util"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/erc"
)

func (c *Chunk) exportMatrix() map[string]any {
	out := make(map[string]any)
	for _, m := range c.Metrics {
		out[m.Key()] = m.getSeries()
	}
	return out
}

func (c *Chunk) export() (*birch.Document, error) {
	doc := birch.DC.Make(len(c.Metrics))
	sample := 0

	var elem *birch.Element
	var err error

	for i := 0; i < len(c.Metrics); i++ {
		elem, sample, err = rehydrateMatrix(c.Metrics, sample)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		doc.Append(elem)
	}

	return doc, nil
}

func (m *Metric) getSeries() any {
	switch m.originalType {
	case bsontype.Int64, bsontype.Timestamp:
		out := make([]int64, len(m.Values))
		copy(out, m.Values)
		return out
	case bsontype.Int32:
		out := make([]int32, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = int32(p)
		}
		return out
	case bsontype.Boolean:
		out := make([]bool, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = p != 0
		}
		return out
	case bsontype.Double:
		out := make([]float64, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = restoreFloat(p)
		}
		return out
	case bsontype.DateTime:
		out := make([]time.Time, len(m.Values))
		for idx, p := range m.Values {
			out[idx] = timeEpocMs(p)
		}
		return out
	default:
		return nil
	}
}

type matrixIterator struct {
	fun.Iterator[*birch.Document]
	chunks   fun.Iterator[*Chunk]
	closer   context.CancelFunc
	metadata *birch.Document
	document *birch.Document
	catcher  erc.Collector
	reflect  bool
	wg       sync.WaitGroup
}

func (iter *matrixIterator) Close(ctx context.Context) error {
	if iter.chunks != nil {
		iter.catcher.Add(iter.chunks.Close(ctx))
	}
	iter.catcher.Add(iter.Iterator.Close(ctx))
	fun.Wait(ctx, &iter.wg)
	return iter.catcher.Resolve()
}

func (iter *matrixIterator) Metadata() *birch.Document { return iter.metadata }
func (iter *matrixIterator) worker(ctx context.Context, pipe chan *birch.Document) {
	defer iter.wg.Done()
	defer close(pipe)

	var payload []byte
	var doc *birch.Document
	var err error

	for iter.chunks.Next(ctx) {
		chunk := iter.chunks.Value()

		if iter.reflect {
			payload, err = util.GlobalMarshaler()(chunk.exportMatrix())
			if err != nil {
				iter.catcher.Add(err)
				return
			}
			doc, err = birch.ReadDocument(payload)
			if err != nil {
				iter.catcher.Add(err)
				return
			}
		} else {
			doc, err = chunk.export()
			if err != nil {
				iter.catcher.Add(err)
				return
			}
		}

		select {
		case pipe <- doc:
			continue
		case <-ctx.Done():
			iter.catcher.Add(errors.New("operation aborted"))
			return
		}
	}
}
