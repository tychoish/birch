package ftdc

import (
	"context"

	"github.com/tychoish/fun/erc"
)

type bufferedCollector struct {
	Collector
	pipe    chan any
	catcher erc.Collector
	ctx     context.Context
}

// NewBufferedCollector wraps an existing collector with a buffer to
// normalize throughput to an underlying collector implementation.
func NewBufferedCollector(ctx context.Context, size int, coll Collector) Collector {
	c := &bufferedCollector{
		Collector: coll,
		pipe:      make(chan any, size),
		ctx:       ctx,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(c.pipe)
				if len(c.pipe) != 0 {
					for in := range c.pipe {
						c.catcher.Push(c.Collector.Add(in))
					}
				}

				return
			case in := <-c.pipe:
				c.catcher.Push(c.Collector.Add(in))
			}
		}
	}()
	return c
}

func (c *bufferedCollector) Add(in any) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case c.pipe <- in:
		return nil
	}
}

func (c *bufferedCollector) Resolve() ([]byte, error) {
	if !c.catcher.Ok() {
		return nil, c.catcher.Resolve()
	}

	return c.Collector.Resolve()
}
