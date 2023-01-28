package metrics

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/tychoish/birch"
	"github.com/tychoish/birch/x/ftdc"
	"github.com/tychoish/birch/jsonx"
)

// CollectJSONOptions specifies options for a JSON2FTDC collector. You
// must specify EITHER an input Source as a reader or a file
// name.
type CollectJSONOptions struct {
	OutputFilePrefix string
	SampleCount      int
	FlushInterval    time.Duration
	InputSource      io.Reader `json:"-"`
	FileName         string
	Follow           bool
}

func (opts CollectJSONOptions) validate() error {
	bothSpecified := (opts.InputSource == nil && opts.FileName == "")
	neitherSpecifed := (opts.InputSource != nil && opts.FileName != "")

	if bothSpecified || neitherSpecifed {
		return errors.New("must specify exactly one of input source and filename")
	}

	if opts.Follow && opts.FileName == "" {
		return errors.New("follow option must not be specified with a file reader")
	}

	return nil
}

func (opts CollectJSONOptions) getSource(ctx context.Context) (<-chan *birch.Document, <-chan error) {
	out := make(chan *birch.Document)
	errs := make(chan error, 2)

	switch {
	case opts.InputSource != nil:
		go func() {
			defer func() {
				if p := recover(); p != nil {
					errs <- fmt.Errorf("json metrics collector: %v", p)
				}
			}()
			defer close(errs)

			stream := bufio.NewScanner(opts.InputSource)

			for stream.Scan() {
				jd, err := jsonx.DCE.Bytes(stream.Bytes())
				if err != nil {
					errs <- err
					return
				}
				doc, err := birch.DCE.JSONX(jd)
				if err != nil {
					errs <- err
					return
				}

				out <- doc
			}
		}()
	case opts.FileName != "" && !opts.Follow:
		go func() {
			defer func() {
				if p := recover(); p != nil {
					errs <- fmt.Errorf("json metrics collector: %v", p)
				}
			}()
			defer close(errs)
			f, err := os.Open(opts.FileName)
			if err != nil {
				errs <- fmt.Errorf("problem opening data file %s: %w", opts.FileName, err)
				return
			}
			defer func() { errs <- f.Close() }()

			stream := bufio.NewScanner(f)
			for stream.Scan() {
				jd, err := jsonx.DCE.Bytes(stream.Bytes())
				if err != nil {
					errs <- err
					return
				}

				doc, err := birch.DCE.JSONX(jd)
				if err != nil {
					errs <- err
					return
				}

				select {
				case out <- doc:
				case <-ctx.Done():
				}
			}
		}()
	case opts.FileName != "" && opts.Follow:
		go func() {
			defer func() {
				if p := recover(); p != nil {
					errs <- fmt.Errorf("json metrics collector: %v", p)
				}
			}()
			defer close(errs)

			if err := follow(ctx, opts.FileName, func(in string) error {
				jd, err := jsonx.DCE.Bytes([]byte(in))
				if err != nil {
					return err
				}

				doc, err := birch.DCE.JSONX(jd)
				if err != nil {
					return err
				}

				select {
				case out <- doc:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}); err != nil {
				errs <- err
				return
			}
		}()
	default:
		errs <- errors.New("invalid collect options")
		close(errs)
	}
	return out, errs
}

// CollectJSONStream provides a blocking process that reads new-line
// separated JSON documents from a file and creates FTDC data from
// these sources.
//
// The Options structure allows you to define the collection intervals
// and also specify the source. The collector supports reading
// directly from an arbitrary IO reader, or from a file. The "follow"
// option allows you to watch the end of a file for new JSON
// documents, a la "tail -f".
func CollectJSONStream(ctx context.Context, opts CollectJSONOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}

	outputCount := 0
	collector := ftdc.NewDynamicCollector(opts.SampleCount)

	flushTimer := time.NewTimer(opts.FlushInterval)
	defer flushTimer.Stop()

	flusher := func() error {
		fn := fmt.Sprintf("%s.%d", opts.OutputFilePrefix, outputCount)

		if info := collector.Info(); info.SampleCount == 0 {
			flushTimer.Reset(opts.FlushInterval)
			return nil
		}

		output, err := collector.Resolve()
		if err != nil {
			return fmt.Errorf("problem resolving ftdc data: %w", err)
		}

		if err = ioutil.WriteFile(fn, output, 0600); err != nil {
			return fmt.Errorf("problem writing data to file %s: %w", fn, err)
		}

		outputCount++

		collector.Reset()

		flushTimer.Reset(opts.FlushInterval)

		return nil
	}

	docs, errs := opts.getSource(ctx)

	for {
		select {
		case <-ctx.Done():
			return errors.New("operation aborted")
		case err := <-errs:
			if err == nil || errors.Is(err, io.EOF) {
				if err := flusher(); err != nil {
					return fmt.Errorf("problem flushing: %w", err)
				}
			}
			return err
		case doc := <-docs:
			if err := collector.Add(doc); err != nil {
				return fmt.Errorf("problem collecting results: %w", err)
			}
		case <-flushTimer.C:
			if err := flusher(); err != nil {
				return fmt.Errorf("problem flushing results at the end of the file: %w", err)
			}
		}
	}
}

func follow(ctx context.Context, filename string, handler func(string) error) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	if err := watcher.Add(filename); err != nil {
		return err
	}

	stream := bufio.NewScanner(bufio.NewReader(file))
	for {
		select {
		case <-ctx.Done():
			return errors.New("operation aborted")
		case event := <-watcher.Events:
			if event.Op&fsnotify.Write == fsnotify.Write {
				for stream.Scan() {
					if err := handler(stream.Text()); err != nil {
						return err
					}
				}
				if err := stream.Err(); err != nil {
					return err
				}
			}
		case err := <-watcher.Errors:
			return err
		}
	}
}
