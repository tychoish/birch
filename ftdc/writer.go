package ftdc

import (
	"fmt"
	"io"

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
	if err := w.collector.Add(doc); err != nil {
		return 0, fmt.Errorf("problem adding document to collector: %w", err)
	}
	return len(in), nil
}

func (w *writerCollector) Close() error {
	if err := FlushCollector(w.collector, w.writer); err != nil {
		return fmt.Errorf("problem flushing documents to collector: %w", err)
	}

	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("problem closing underlying writer: %w", err)
	}
	return nil
}
