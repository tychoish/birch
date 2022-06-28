package metrics

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tychoish/birch/ftdc"
)

func GetDirectoryOfFile() string {
	_, file, _, _ := runtime.Caller(1)

	return filepath.Dir(file)
}

func TestCollectRuntime(t *testing.T) {
	dir := t.TempDir()

	t.Run("Collection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := CollectOptions{
			SkipProcess: true,
			SkipSystem:  true,
		}
		doc := opts.generate(ctx, 1)
		if 4 != doc.Len() {
			t.Error("values should be equal")
		}
	})

	t.Run("CollectData", func(t *testing.T) {
		opts := CollectOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("sysinfo.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			SampleCount:        10,
			FlushInterval:      time.Second,
			CollectionInterval: time.Millisecond,
			SkipProcess:        true,
			SkipSystem:         true,
		}
		var cancel context.CancelFunc
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := CollectRuntime(ctx, opts); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ReadStructuredData", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		files, err := ioutil.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) < 1 {
			t.Error("expected true")
		}

		total := 0
		for idx, info := range files {
			t.Run(fmt.Sprintf("FileNo.%d", idx), func(t *testing.T) {
				path := filepath.Join(dir, info.Name())
				var f *os.File
				f, err = os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
				}()
				iter := ftdc.ReadStructuredMetrics(ctx, f)
				counter := 0
				for iter.Next() {
					counter++
					doc := iter.Document()
					if doc == nil {
						t.Fatalf("%T value is nil", doc)
					}
					if 4 != doc.Len() {
						t.Fatalf("unqueal %v and %v", 4, doc.Len())
					}
				}
				if err := iter.Err(); err != nil {
					t.Fatal(err)
				}
				total += counter
			})
		}
	})
	t.Run("ReadFlattenedData", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		files, err := ioutil.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) < 1 {
			t.Error("expected true")
		}

		total := 0
		for idx, info := range files {
			t.Run(fmt.Sprintf("FileNo.%d", idx), func(t *testing.T) {
				path := filepath.Join(dir, info.Name())
				var f *os.File
				f, err = os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
				}()
				iter := ftdc.ReadMetrics(ctx, f)
				counter := 0
				for iter.Next() {
					counter++
					doc := iter.Document()
					if doc == nil {
						t.Fatalf("%T value is nil", doc)
					}
					if 15 != doc.Len() {
						t.Fatalf("unqueal %v and %v", 15, doc.Len())
					}
				}
				if err := iter.Err(); err != nil {
					t.Fatal(err)
				}
				total += counter
			})
		}
	})
	t.Run("CollectAllData", func(t *testing.T) {
		if strings.Contains(os.Getenv("EVR_TASK_ID"), "race") {
			t.Skip("evergreen environment inconsistent")
		}
		// this test runs without the skips, which are
		// expected to be less reliable in different environment
		opts := CollectOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("complete.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			SampleCount:        100,
			FlushInterval:      time.Second,
			CollectionInterval: time.Millisecond,
		}
		var cancel context.CancelFunc
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := CollectRuntime(ctx, opts); err != nil {
			t.Fatal(err)
		}
	})
}
