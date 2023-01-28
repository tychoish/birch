package ftdc

import (
	"time"
)

type samplingCollector struct {
	minimumInterval time.Duration
	lastCollection  time.Time
	Collector
}

// NewSamplingCollector wraps a different collector implementation and
// provides an implementation of the Add method that skips collection
// of results if the specified minimumInterval has not elapsed since
// the last collection.
func NewSamplingCollector(minimumInterval time.Duration, collector Collector) Collector {
	return &samplingCollector{
		minimumInterval: minimumInterval,
		Collector:       collector,
	}
}

func (c *samplingCollector) Add(d any) error {
	if time.Since(c.lastCollection) < c.minimumInterval {
		return nil
	}

	c.lastCollection = time.Now()

	return (c.Collector.Add(d))
}
