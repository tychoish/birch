package ftdc

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/tychoish/birch"
)

type writerCollector struct {
	writer    io.WriteCloser
	collector *streamingDynamicCollector
}

func NewWriterCollector(chunkSize int, writer io.WriteCloser) io.WriteCloser {
	return &writerCollector{
		writer: writer,
		collector: &streamingDynamicCollector{
			output:             writer,
			streamingCollector: newStreamingCollector(chunkSize, writer),
		},
	}
}

func (w *writerCollector) Write(in []byte) (int, error) {
	doc, err := birch.ReadDocument(in)
	if err != nil {
		return 0, fmt.Errorf("problem reading bson document: %w", err)
	}
	return len(in), errors.Wrap(w.collector.Add(doc), "problem adding document to collector")
}

func (w *writerCollector) Close() error {
	if err := FlushCollector(w.collector, w.writer); err != nil {
		return fmt.Errorf("problem flushing documents to collector: %w", err)
	}

	return errors.Wrap(w.writer.Close(), "problem closing underlying writer")
}
