package ftdc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSampleIterator(t *testing.T) {
	t.Run("CanceledContextCreator", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		chunk := &Chunk{
			nPoints: 2,
		}
		out := chunk.streamDocuments(ctx)
		if out == nil {
			t.Fatal("'out' should not be nil")
		}
		for {
			doc, ok := <-out
			if ok {
				t.Error("expected false")
			}
			if doc != nil {
				t.Error("expected nil doc")
			}
			break
		}

	})
	t.Run("CloserOperations", func(t *testing.T) {
		iter := &sampleIterator{}
		assert.Panics(t, func() {
			iter.Close()
		})
		counter := 0
		iter.closer = func() { counter++ }
		assert.NotPanics(t, func() {
			iter.Close()
		})
		if 1 != counter {
			t.Error("values should be equal")
		}

	})

}
