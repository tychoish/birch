package metrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/papertrail/go-tail/follower"
	"github.com/pkg/errors"
	"github.com/tychoish/birch"
	"github.com/tychoish/birch/ftdc"
	"github.com/tychoish/birch/jsonx"
	"github.com/tychoish/grip/recovery"
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
			defer recovery.LogStackTraceAndContinue("collect json metrics")

			stream := bufio.NewScanner(opts.InputSource)
			defer close(errs)

			for stream.Scan() {
				jd, err := jsonx.DC.BytesErr(stream.Bytes())
				if err != nil {
					errs <- err
					return
				}

				doc, err := birch.DC.JSONXErr(jd)
				if err != nil {
					errs <- err
					return
				}

				out <- doc
			}
		}()
	case opts.FileName != "" && !opts.Follow:
		go func() {
			defer recovery.LogStackTraceAndContinue("collect json metrics")
			defer close(errs)
			f, err := os.Open(opts.FileName)
			if err != nil {
				errs <- errors.Wrapf(err, "problem opening data file %s", opts.FileName)
				return
			}
			defer func() { errs <- f.Close() }()
			stream := bufio.NewScanner(f)

			for stream.Scan() {
				jd, err := jsonx.DC.BytesErr(stream.Bytes())
				if err != nil {
					errs <- err
					return
				}

				doc, err := birch.DC.JSONXErr(jd)
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
			defer recovery.LogStackTraceAndContinue("collect json metrics")
			defer close(errs)

			tail, err := follower.New(opts.FileName, follower.Config{
				Reopen: true,
			})
			if err != nil {
				errs <- errors.Wrapf(err, "problem setting up file follower of '%s'", opts.FileName)
				return
			}
			defer func() {
				tail.Close()
				errs <- tail.Err()
			}()

			for line := range tail.Lines() {
				jd, err := jsonx.DC.BytesErr([]byte(line.String()))
				if err != nil {
					errs <- err
					return
				}

				doc, err := birch.DC.JSONXErr(jd)
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
			return errors.Wrapf(err, "problem writing data to file %s", fn)
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
			if err == nil || errors.Cause(err) == io.EOF {
				return errors.Wrap(flusher(), "problem flushing results at the end of the file")
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
