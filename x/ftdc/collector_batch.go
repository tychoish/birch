package ftdc

import (
	"bytes"
)

type batchCollector struct {
	maxSamples int
	chunks     []*betterCollector
}

// NewBatchCollector constructs a collector implementation that
// builds data chunks with payloads of the specified number of samples.
// This implementation allows you break data into smaller components
// for more efficient read operations.
func NewBatchCollector(maxSamples int) Collector {
	return newBatchCollector(maxSamples)
}

func newBatchCollector(size int) *batchCollector {
	return &batchCollector{
		maxSamples: size,
		chunks: []*betterCollector{
			{
				maxDeltas: size,
			},
		},
	}
}

func (c *batchCollector) Info() CollectorInfo {
	out := CollectorInfo{}
	for _, c := range c.chunks {
		info := c.Info()
		out.MetricsCount += info.MetricsCount
		out.SampleCount += info.SampleCount
	}
	return out
}

func (c *batchCollector) Reset() {
	c.chunks = []*betterCollector{{maxDeltas: c.maxSamples}}
}

func (c *batchCollector) SetMetadata(in any) error {
	return (c.chunks[0].SetMetadata(in))
}

func (c *batchCollector) Add(in any) error {
	doc, err := readDocument(in)
	if err != nil {
		return err
	}

	last := c.chunks[len(c.chunks)-1]
	if last.Info().SampleCount >= c.maxSamples {
		last = &betterCollector{maxDeltas: c.maxSamples}
		c.chunks = append(c.chunks, last)
	}

	return (last.Add(doc))
}

func (c *batchCollector) Resolve() ([]byte, error) {
	buf := &bytes.Buffer{}

	for _, chunk := range c.chunks {
		out, err := chunk.Resolve()
		if err != nil {
			return nil, err
		}

		_, _ = buf.Write(out)
	}

	return buf.Bytes(), nil
}
