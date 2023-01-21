package ftdc

import (
	"bytes"
	"context"
	"encoding/csv"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/ftdc/testutil"
	"github.com/tychoish/fun"
)

func TestWriteCSVIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tmp := t.TempDir()

	t.Run("Write", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newChunk(10)))
		out := &bytes.Buffer{}
		err := WriteCSV(ctx, iter, out)
		if err != nil {
			t.Fatal(err)
		}

		lines := strings.Split(out.String(), "\n")
		if len(lines) != 12 {
			t.Errorf("length should be %d", 12)
		}
	})
	t.Run("ResuseIterPass", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newChunk(10)))
		err := DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		if err != nil {
			t.Fatal(err)
		}
		err = DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Dump", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newChunk(10)))
		err := DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("DumpMixed", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newMixedChunk(10)))
		err := DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("WriteWithSchemaChange", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newMixedChunk(10)))
		out := &bytes.Buffer{}
		err := WriteCSV(ctx, iter, out)

		if err == nil {
			t.Fatal("error should not be nill")
		}
	})
}

func TestReadCSVIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range []struct {
		Name   string
		Iter   fun.Iterator[*Chunk]
		Rows   int
		Fields int
	}{
		{
			Name:   "SimpleFlat",
			Iter:   produceMockChunkIter(ctx, 1000, func() *birch.Document { return testutil.RandFlatDocument(15) }),
			Rows:   1000,
			Fields: 15,
		},
		{
			Name:   "LargerFlat",
			Iter:   produceMockChunkIter(ctx, 1000, func() *birch.Document { return testutil.RandFlatDocument(50) }),
			Rows:   1000,
			Fields: 50,
		},
		{
			Name:   "Complex",
			Iter:   produceMockChunkIter(ctx, 1000, func() *birch.Document { return testutil.RandComplexDocument(20, 3) }),
			Rows:   1000,
			Fields: 100,
		},
		{
			Name:   "LargComplex",
			Iter:   produceMockChunkIter(ctx, 1000, func() *birch.Document { return testutil.RandComplexDocument(100, 10) }),
			Rows:   1000,
			Fields: 190,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteCSV(ctx, test.Iter, buf)
			if err != nil {
				t.Fatal(err)
			}

			out := &bytes.Buffer{}
			err = ConvertFromCSV(ctx, test.Rows, buf, out)
			if err != nil {
				t.Fatal(err)
			}

			iter := ReadMetrics(ctx, out)
			count := 0
			for iter.Next(ctx) {
				count++
				doc := iter.Value()
				if test.Fields != doc.Len() {
					t.Error("values should be equal")
				}
			}
			if test.Rows != count {
				t.Error("values should be equal")
			}
		})
	}
	t.Run("SchemaChangeGrow", func(t *testing.T) {
		buf := &bytes.Buffer{}
		csvw := csv.NewWriter(buf)
		if err := csvw.Write([]string{"a", "b", "c", "d"}); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 2; j++ {
			for i := 0; i < 10; i++ {
				if err := csvw.Write([]string{"1", "2", "3", "4"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := csvw.Write([]string{"1", "2", "3", "4", "5"}); err != nil {
				t.Fatal(err)
			}
		}
		csvw.Flush()

		if err := ConvertFromCSV(ctx, 1000, buf, &bytes.Buffer{}); err == nil {
			t.Error("error should be nil")
		}
	})
	t.Run("SchemaChangeShrink", func(t *testing.T) {
		buf := &bytes.Buffer{}
		csvw := csv.NewWriter(buf)
		if err := csvw.Write([]string{"a", "b", "c", "d"}); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 2; j++ {
			for i := 0; i < 10; i++ {
				if err := csvw.Write([]string{"1", "2", "3", "4"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := csvw.Write([]string{"1", "2"}); err != nil {
				t.Fatal(err)
			}
		}
		csvw.Flush()

		if err := ConvertFromCSV(ctx, 1000, buf, &bytes.Buffer{}); err == nil {
			t.Error("error should be nil")
		}
	})
}
