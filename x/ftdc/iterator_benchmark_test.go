package ftdc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkIterator(b *testing.B) {
	for _, test := range []struct {
		Name string
		Path string
	}{
		{
			Name: "PerfMockSmall",
			Path: "perf_metrics_small.ftdc",
		},
		{
			Name: "PerfMock",
			Path: "perf_metrics.ftdc",
		},
		{
			Name: "ServerStatus",
			Path: "metrics.ftdc",
		},
	} {
		b.Run(test.Name, func(b *testing.B) {
			file, err := os.Open(test.Path)
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err = file.Close(); err != nil {
					fmt.Println(err)
				}
			}()
			data, err := ioutil.ReadAll(file)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()

			b.Run("Chunk", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					iter := ReadChunks(bytes.NewBuffer(data))
					for val := range iter.Iterator() {
						if val == nil {
							b.Fatalf("%T value is nil", val)
						}
					}
				}
			})
			b.Run("Series", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					iter := ReadSeries(bytes.NewBuffer(data))
					for val := range iter.Iterator() {
						if val == nil {
							b.Fatalf("%T value is nil", val)
						}
					}
				}
			})
			b.Run("Matrix", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					iter := ReadMatrix(bytes.NewBuffer(data))
					for val := range iter.Iterator() {
						if val == nil {
							b.Fatalf("%T value is nil", val)
						}
					}
				}
			})
			b.Run("Structured", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					iter := ReadStructuredMetrics(bytes.NewBuffer(data))
					for val := range iter.Iterator() {
						if val == nil {
							b.Fatalf("%T value is nil", val)
						}
					}
				}
			})
			b.Run("Flattened", func(b *testing.B) {
				iter := ReadStructuredMetrics(bytes.NewBuffer(data))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					for val := range iter.Iterator() {
						if val == nil {
							b.Fatalf("%T value is nil", val)
						}
					}
				}
			})
		})
	}
}
