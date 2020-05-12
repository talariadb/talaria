package bigquery

import (
	"bytes"
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Writer represents a writer for Google Cloud Storage.
type Writer struct {
	dataset string
	table   *bigquery.Table
	client  *bigquery.Client
	context context.Context
}

// New creates a new writer.
func New(project, dataset, table string) (*Writer, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Newf("bigquery: %v", err)
	}

	tableRef := client.Dataset(dataset).Table(table)
	return &Writer{
		dataset: dataset,
		table:   tableRef,
		client:  client,
		context: ctx,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, val []byte) error {
	source := bigquery.NewReaderSource(bytes.NewReader(val))
	source.FileConfig = bigquery.FileConfig{
		SourceFormat:        bigquery.DataFormat("ORC"),
		AutoDetect:          false,
		IgnoreUnknownValues: true,
		MaxBadRecords:       0,
	}

	// Run the loader
	loader := w.table.LoaderFrom(source)
	ctx := context.Background()
	job, err := loader.Run(ctx)
	if err != nil {
		return errors.Internal("bigquery: unable to run a job", err)
	}

	// Wait for the job to complete
	status, err := job.Wait(ctx)
	if err != nil {
		return errors.Internal("bigquery: unable to write", err)
	}
	if err := status.Err(); err != nil {
		return errors.Internal("bigquery: unable to write", err)
	}
	return nil
}

// Close closes the writer.
func (w *Writer) Close() error {
	return w.client.Close()
}
