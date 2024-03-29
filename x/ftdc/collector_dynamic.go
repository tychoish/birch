package ftdc

import (
	"bytes"
	"errors"
)

type dynamicCollector struct {
	maxSamples int
	chunks     []*batchCollector
	hash       string
	currentNum int
}

// NewDynamicCollector constructs a Collector that records metrics
// from documents, creating new chunks when either the number of
// samples collected exceeds the specified max sample count OR
// the schema changes.
//
// There is some overhead associated with detecting schema changes,
// particularly for documents with more complex schemas, so you may
// wish to opt for a simpler collector in some cases.
func NewDynamicCollector(maxSamples int) Collector {
	return &dynamicCollector{
		maxSamples: maxSamples,
		chunks: []*batchCollector{
			newBatchCollector(maxSamples),
		},
	}
}

func (c *dynamicCollector) Info() CollectorInfo {
	out := CollectorInfo{}
	for _, c := range c.chunks {
		info := c.Info()
		out.MetricsCount += info.MetricsCount
		out.SampleCount += info.SampleCount
	}
	return out
}

func (c *dynamicCollector) Reset() {
	c.chunks = []*batchCollector{newBatchCollector(c.maxSamples)}
	c.hash = ""
}

func (c *dynamicCollector) SetMetadata(in any) error {
	return (c.chunks[0].SetMetadata(in))
}

func (c *dynamicCollector) Add(in any) error {
	doc, err := readDocument(in)
	if err != nil {
		return err
	}
	if doc == nil {
		return errors.New("nil document")
	}

	if c.hash == "" {
		docHash, num := metricKeyHash(doc)
		c.hash = docHash
		c.currentNum = num
		return (c.chunks[0].Add(doc))
	}

	lastChunk := c.chunks[len(c.chunks)-1]

	docHash, _ := metricKeyHash(doc)
	if c.hash == docHash {
		return (lastChunk.Add(doc))
	}

	chunk := newBatchCollector(c.maxSamples)
	c.chunks = append(c.chunks, chunk)

	return (chunk.Add(doc))
}

func (c *dynamicCollector) Resolve() ([]byte, error) {
	buf := bytes.NewBuffer(bufpool.Make())
	for _, chunk := range c.chunks {
		out, err := chunk.Resolve()
		if err != nil {
			return nil, err
		}

		_, _ = buf.Write(out)
	}

	return buf.Bytes(), nil
}
