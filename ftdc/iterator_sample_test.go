package ftdc

import (
	"context"
	"testing"
)

func TestSampleIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("CloserOperations", func(t *testing.T) {
		iter := &sampleIterator{}
		func() {
			defer func() {
				if p := recover(); p == nil {
					t.Error("case should panic")
				}
			}()

			_ = iter.Close(ctx)
		}()
		counter := 0
		iter.closer = func() { counter++ }
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Error("case should not panic")
				}
			}()

			_ = iter.Close(ctx)
		}()
		if 1 != counter {
			t.Error("values should be equal")
		}

	})

}
