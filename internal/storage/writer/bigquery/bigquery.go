package bigquery

import (
	"context"
	"runtime"

	"cloud.google.com/go/bigquery"
	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/writer/base"
)

// bqRow implements ValueSaver interface Save method.
type bqRow struct {
	values map[string]interface{}
}

// Writer represents a writer for Google Cloud Storage.
type Writer struct {
	*base.Writer
	dataset  string
	table    *bigquery.Table
	client   *bigquery.Client
	monitor  monitor.Monitor
	context  context.Context
	inserter *bigquery.Inserter
	buffer   chan []bigquery.ValueSaver
	queue    chan async.Task
}

// New creates a new writer.
func New(project, dataset, table, encoding, filter string, monitor monitor.Monitor, loader *script.Loader) (*Writer, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Newf("bigquery: %v", err)
	}
	encoderwriter, err := base.New(filter, encoding, loader)
	if err != nil {
		return nil, errors.Newf("bigquery: %v", err)
	}

	tableRef := client.Dataset(dataset).Table(table)
	inserter := tableRef.Inserter()
	inserter.SkipInvalidRows = true
	inserter.IgnoreUnknownValues = true
	w := &Writer{
		Writer:   encoderwriter,
		dataset:  dataset,
		table:    tableRef,
		inserter: inserter,
		client:   client,
		monitor:  monitor,
		context:  ctx,
		buffer:   make(chan []bigquery.ValueSaver, 65000),
		queue:    make(chan async.Task),
	}
	w.Process = w.process
	return w, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, blocks []block.Block) error {
	buffer, err := w.Writer.Encode(blocks)
	if err != nil {
		return err
	}
	blk, _ := block.FromBuffer(buffer)
	rows, _ := block.FromBlockBy(blk, blk.Schema())
	bqrows := make([]bigquery.ValueSaver, 0)
	for _, row := range rows {
		bqrow := &bqRow{
			values: row.Values,
		}
		bqrows = append(bqrows, bqrow)
	}
	if err := w.inserter.Put(w.context, bqrows); err != nil {
		return err
	}
	return nil
}

// Save impl for bigquery.ValueSaver interface
func (b *bqRow) Save() (map[string]bigquery.Value, string, error) {
	bqRow := make(map[string]bigquery.Value, len(b.values))
	for k, v := range b.values {
		bqRow[k] = v
	}
	return bqRow, "", nil
}

// Stream publishes the rows in real-time.
func (w *Writer) Stream(row block.Row) error {

	bqrow := &bqRow{
		values: row.Values,
	}
	rows := []bigquery.ValueSaver{bqrow}

	select {
	case w.buffer <- rows:
	default:
		return errors.New("bigquery: buffer is full")
	}

	return nil
}

// process will read from buffer and streams to bq
func (w *Writer) process(parent context.Context) error {
	async.Consume(parent, runtime.NumCPU()*8, w.queue)
	for batch := range w.buffer {
		select {
		case <-parent.Done():
			return parent.Err()
		default:
		}
		w.queue <- async.NewTask(func(ctx context.Context) (interface{}, error) {
			err := w.inserter.Put(ctx, batch)
			if err != nil {
				w.buffer <- batch
				return nil, err
			}
			return nil, nil
		})
	}
	return nil
}

// Close closes the writer.
func (w *Writer) Close() error {
	return w.client.Close()
}
