package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tychoish/birch/x/ftdc"
	"github.com/tychoish/birch/x/ftdc/testutil"
)

func TestCollectJSONOptions(t *testing.T) {
	for _, test := range []struct {
		name  string
		valid bool
		opts  CollectJSONOptions
	}{
		{
			name:  "Nil",
			valid: false,
		},
		{
			name:  "FileWithIoReader",
			valid: false,
			opts: CollectJSONOptions{
				FileName:    "foo",
				InputSource: &bytes.Buffer{},
			},
		},
		{
			name:  "JustIoReader",
			valid: true,
			opts: CollectJSONOptions{
				InputSource: &bytes.Buffer{},
			},
		},
		{
			name:  "JustFile",
			valid: true,
			opts: CollectJSONOptions{
				FileName: "foo",
			},
		},
		{
			name:  "FileWithFollow",
			valid: true,
			opts: CollectJSONOptions{
				FileName: "foo",
				Follow:   true,
			},
		},
		{
			name:  "ReaderWithFollow",
			valid: false,
			opts: CollectJSONOptions{
				InputSource: &bytes.Buffer{},
				Follow:      true,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.valid {
				if err := test.opts.validate(); err != nil {
					t.Error(err)
				}
			} else {
				if err := test.opts.validate(); err == nil {
					t.Error("error should be nil")
				}
			}
		})
	}
}

func makeJSONRandComplex(num int) ([][]byte, error) {
	out := [][]byte{}

	for i := 0; i < num; i++ {
		doc := testutil.RandComplexDocument(100, 2)
		data, err := json.Marshal(doc)
		if err != nil {
			fmt.Println(doc)
			return nil, err
		}
		out = append(out, data)
	}

	return out, nil
}

func writeStream(docs [][]byte, writer io.Writer) error {
	for _, doc := range docs {
		_, err := writer.Write(doc)
		if err != nil {
			return err
		}

		_, err = writer.Write([]byte("\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func TestCollectJSON(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	hundredDocs, err := makeJSONRandComplex(100)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("SingleReaderIdealCase", func(t *testing.T) {
		buf := &bytes.Buffer{}
		err = writeStream(hundredDocs, buf)
		if err != nil {
			t.Fatal(err)
		}

		reader := bytes.NewReader(buf.Bytes())

		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			FlushInterval: 100 * time.Millisecond,
			SampleCount:   1000,
			InputSource:   reader,
		}

		err = CollectJSONStream(ctx, opts)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("SingleReaderBotchedDocument", func(t *testing.T) {
		buf := &bytes.Buffer{}

		var docs [][]byte
		docs, err = makeJSONRandComplex(10)
		if err != nil {
			t.Fatal(err)
		}

		docs[2] = docs[len(docs)-1][1:] // break the last document

		err = writeStream(docs, buf)
		if err != nil {
			t.Fatal(err)
		}

		reader := bytes.NewReader(buf.Bytes())

		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			FlushInterval: 10 * time.Millisecond,
			InputSource:   reader,
			SampleCount:   100,
		}

		err = CollectJSONStream(ctx, opts)
		if err == nil {
			t.Error("error should not be nil")
		}
	})
	t.Run("ReadFromFile", func(t *testing.T) {
		fn := filepath.Join(dir, "json-read-file-one")
		var f *os.File
		f, err = os.Create(fn)
		if err != nil {
			t.Fatal(err)
		}

		if err := writeStream(hundredDocs, f); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			FileName:    fn,
			SampleCount: 100,
		}

		err = CollectJSONStream(ctx, opts)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("FollowFile", func(t *testing.T) {
		fn := filepath.Join(dir, "json-read-file-two")
		var f *os.File
		f, err = os.Create(fn)
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			if err := writeStream(hundredDocs, f); err != nil {
				t.Error(err)
				return
			}
			if err := f.Close(); err != nil {
				t.Error(err)
				return
			}
		}()

		ctx, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			SampleCount:   100,
			FlushInterval: 500 * time.Millisecond,
			FileName:      fn,
			Follow:        true,
		}

		err = CollectJSONStream(ctx, opts)
		if err == nil {
			t.Fatal("error should not be nil")
		}
		if !strings.Contains(err.Error(), "operation aborted") {
			t.Error("unexpected error", err)
		}
	})
	t.Run("RoundTrip", func(t *testing.T) {
		inputs := []map[string]any{
			{
				"one":   int64(1),
				"other": int64(43),
			},
			{
				"one":   int64(33),
				"other": int64(41),
			},
			{
				"one":   int64(1),
				"other": int64(41),
			},
		}

		var (
			doc  []byte
			docs [][]byte
		)

		for _, in := range inputs {
			doc, err = json.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			docs = append(docs, doc)
		}
		if len(docs) != 3 {
			t.Fatalf("lengths of %d and %d are not expected", len(docs), 3)
		}

		buf := &bytes.Buffer{}

		if err := writeStream(docs, buf); err != nil {
			t.Fatal(err)
		}

		reader := bytes.NewReader(buf.Bytes())

		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, "roundtrip"),
			FlushInterval:    time.Second,
			SampleCount:      50,
			InputSource:      reader,
		}
		ctx := context.Background()

		err = CollectJSONStream(ctx, opts)
		if err != nil {
			t.Error(err)
		}

		_, err := os.Stat(filepath.Join(dir, "roundtrip.0"))
		if os.IsNotExist(err) {
			t.Error("file should exist")
		}

		fn, err := os.Open(filepath.Join(dir, "roundtrip.0"))
		if err != nil {
			t.Fatal(err)
		}

		idx := -1
		for s := range ftdc.ReadMetrics(fn).Iterator() {
			idx++

			if 2 != s.Len() {
				t.Error("values should be equal")
			}
			for k, v := range inputs[idx] {
				out := s.Lookup(k)
				if int(v.(int64)) != int(out.Interface().(int32)) {
					t.Fatalf("values are not equal %v and %v", v, out.Interface())
				}
			}
		}
		if 2 != idx {
			t.Error("values should be equal")
		} // zero indexed
	})
}
