package ftdc

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"errors"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/bsontype"
	"github.com/tychoish/fun"
)

func (c *Chunk) getFieldNames() []string {
	fieldNames := make([]string, len(c.Metrics))
	for idx, m := range c.Metrics {
		fieldNames[idx] = m.Key()
	}
	return fieldNames
}

func (c *Chunk) getRecord(i int) []string {
	fields := make([]string, len(c.Metrics))
	for idx, m := range c.Metrics {
		switch m.originalType {
		case bsontype.Double, bsontype.Int32, bsontype.Int64, bsontype.Boolean, bsontype.Timestamp:
			fields[idx] = strconv.FormatInt(m.Values[i], 10)
		case bsontype.DateTime:
			fields[idx] = time.Unix(m.Values[i]/1000, 0).Format(time.RFC3339)
		}
	}
	return fields
}

// WriteCSV exports the contents of a stream of chunks as CSV. Returns
// an error if the number of metrics changes between points, or if
// there are any errors writing data.
func WriteCSV(ctx context.Context, iter *fun.Stream[*Chunk], writer io.Writer) error {
	var numFields int
	csvw := csv.NewWriter(writer)
	for iter.Next(ctx) {
		if ctx.Err() != nil {
			return errors.New("operation aborted")
		}
		chunk := iter.Value()
		if numFields == 0 {
			fieldNames := chunk.getFieldNames()
			if err := csvw.Write(fieldNames); err != nil {
				return fmt.Errorf("problem writing field names: %w", err)
			}
			numFields = len(fieldNames)
		} else if numFields != len(chunk.Metrics) {
			return errors.New("unexpected schema change detected")
		}

		for i := 0; i < chunk.nPoints; i++ {
			record := chunk.getRecord(i)
			if err := csvw.Write(record); err != nil {
				return fmt.Errorf("problem writing csv record %d of %d: %w", i, chunk.nPoints, err)
			}
		}
		csvw.Flush()
		if err := csvw.Error(); err != nil {
			return fmt.Errorf("problem flushing csv data: %w", err)
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("problem reading chunks: %w", err)
	}

	return nil
}

func getCSVFile(prefix string, count int) (io.WriteCloser, error) {
	fn := fmt.Sprintf("%s.%d.csv", prefix, count)
	writer, err := os.Create(fn)
	if err != nil {
		return nil, fmt.Errorf("provlem opening file %s: %w", fn, err)
	}
	return writer, nil
}

// DumpCSV writes a sequence of chunks to CSV files, creating new
// files if the iterator detects a schema change, using only the
// number of fields in the chunk to detect schema changes. DumpCSV
// writes a header row to each file.
//
// The file names are constructed as "prefix.<count>.csv".
func DumpCSV(ctx context.Context, iter *fun.Stream[*Chunk], prefix string) error {
	var (
		err       error
		writer    io.WriteCloser
		numFields int
		fileCount int
		csvw      *csv.Writer
	)
	for iter.Next(ctx) {
		if ctx.Err() != nil {
			return errors.New("operation aborted")
		}

		if writer == nil {
			writer, err = getCSVFile(prefix, fileCount)
			if err != nil {
				return err
			}
			csvw = csv.NewWriter(writer)
			fileCount++
		}

		chunk := iter.Value()
		if numFields == 0 {
			fieldNames := chunk.getFieldNames()
			if err = csvw.Write(fieldNames); err != nil {
				return fmt.Errorf("problem writing field names: %w", err)
			}
			numFields = len(fieldNames)
		} else if numFields != len(chunk.Metrics) {
			if err = writer.Close(); err != nil {
				return fmt.Errorf("problem flushing and closing file: %w", err)
			}

			writer, err = getCSVFile(prefix, fileCount)
			if err != nil {
				return err
			}

			csvw = csv.NewWriter(writer)
			fileCount++

			// now dump header
			fieldNames := chunk.getFieldNames()
			if err := csvw.Write(fieldNames); err != nil {
				return fmt.Errorf("problem writing field names: %w", err)
			}
			numFields = len(fieldNames)
		}

		for i := 0; i < chunk.nPoints; i++ {
			record := chunk.getRecord(i)
			if err := csvw.Write(record); err != nil {
				return fmt.Errorf("problem writing csv record %d of %d: %w", i, chunk.nPoints, err)
			}
		}
		csvw.Flush()
		if err := csvw.Error(); err != nil {
			return fmt.Errorf("problem flushing csv data: %w", err)
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("problem reading chunks: %w", err)
	}

	if writer == nil {
		return nil
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("problem writing files to disk: %w", err)

	}
	return nil
}

// ConvertFromCSV takes an input stream and writes ftdc compressed
// data to the provided output writer.
//
// If the number of fields changes in the CSV fields, the first field
// with the changed number of fields becomes the header for the
// subsequent documents in the stream.
func ConvertFromCSV(ctx context.Context, bucketSize int, input io.Reader, output io.Writer) error {
	csvr := csv.NewReader(input)

	header, err := csvr.Read()
	if err != nil {
		return fmt.Errorf("problem reading error: %w", err)
	}

	collector := NewStreamingDynamicCollector(bucketSize, output)

	defer func() {
		if err != nil && (!errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded)) {
			err = fmt.Errorf("omitting final flush, because of prior error: %w", err)
		}
		err = FlushCollector(collector, output)
	}()

	var record []string
	for {
		if ctx.Err() != nil {
			// this is weird so that the defer can work
			err = fmt.Errorf("operation aborted: %w", err)
			return err
		}

		record, err = csvr.Read()
		if err == io.EOF {
			// this is weird so that the defer can work
			err = nil
			return err
		}

		if err != nil {
			if pr, ok := err.(*csv.ParseError); ok && pr.Err == csv.ErrFieldCount {
				header = record
				continue
			}
			err = fmt.Errorf("problem parsing csv: %w", err)
			return err
		}
		if len(record) != len(header) {
			return errors.New("unexpected field count change")
		}

		elems := make([]*birch.Element, 0, len(header))
		for idx := range record {
			var val int
			val, err = strconv.Atoi(record[idx])
			if err != nil {
				continue
			}
			elems = append(elems, birch.EC.Int64(header[idx], int64(val)))
		}

		if err = collector.Add(birch.DC.Elements(elems...)); err != nil {
			return err
		}
	}
}
