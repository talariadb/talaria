package bigquery

import (
	"context"
	"encoding/json"
	"runtime"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/base"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// bqRow implements ValueSaver interface Save method.
type bqRow struct {
	values map[string]interface{}
}

// Writer represents a writer for Google Cloud Storage.
type Writer struct {
	*base.Writer
	dataset           string
	table             *bigquery.Table
	client            *bigquery.Client
	managedClient     *managedwriter.Client
	managedStream     *managedwriter.ManagedStream
	messageDescriptor *protoreflect.MessageDescriptor
	monitor           monitor.Monitor
	context           context.Context
	queue             chan async.Task
	buffer            chan block.Row
}

// New creates a new writer.
func New(project, dataset, table, encoding, filter string, monitor monitor.Monitor) (*Writer, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Newf("bigquery: %v", err)
	}
	managedClient, err := managedwriter.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Newf("bigquery: %v", err)
	}
	encoderwriter, err := base.New(filter, encoding, monitor)
	if err != nil {
		return nil, errors.Newf("bigquery: %v", err)
	}

	tableRef := client.Dataset(dataset).Table(table)
	inserter := tableRef.Inserter()
	inserter.SkipInvalidRows = true
	inserter.IgnoreUnknownValues = true

	mt, _ := tableRef.Metadata(ctx)
	md, descriptorProto, err := setupDynamicDescriptors(mt.Schema)

	ms, err := managedClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(tableRef.ProjectID, tableRef.DatasetID, tableRef.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		Writer:            encoderwriter,
		dataset:           dataset,
		table:             tableRef,
		managedClient:     managedClient,
		managedStream:     ms,
		messageDescriptor: &md,
		monitor:           monitor,
		context:           ctx,
		queue:             make(chan async.Task),
		buffer:            make(chan block.Row, 65000),
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
	blk, err := block.FromBuffer(buffer)
	if err != nil {
		return err
	}
	rows, err := block.FromBlockBy(blk, blk.Schema())
	if err != nil {
		return err
	}
	filtered, err := w.Writer.Filter(rows)
	if err != nil {
		return err
	}

	rows, _ = filtered.([]block.Row)
	var result *managedwriter.AppendResult
	for _, row := range rows {
		v := row.Values
		v["ingested_at"] = v["ingested_at"].(time.Time).UnixMicro()
		j, _ := json.Marshal(v)
		message := dynamicpb.NewMessage(*w.messageDescriptor)
		if err = protojson.Unmarshal(j, message); err != nil {
			return err
		}
		b, err := proto.Marshal(message)
		if err != nil {
			return err
		}

		result, err = w.managedStream.AppendRows(w.context, [][]byte{b})
		if err != nil {
			return err
		}
	}
	o, err := result.GetResult(w.context)
	if err != nil {
		return err
	}
	if o != managedwriter.NoStreamOffset {
		return errors.Newf("offset mismatch, got %d want %d", o, managedwriter.NoStreamOffset)
	}
	return nil
}

// Stream publishes the rows in real-time.
func (w *Writer) Stream(row block.Row) error {

	filtered, err := w.Writer.Filter(row)
	// If message is filtered out, return nil
	if filtered == nil {
		return nil
	}
	if err != nil {
		return err
	}
	row, _ = filtered.(block.Row)

	w.buffer <- row

	return nil
}

// process will read from buffer and streams to bq
func (w *Writer) process(parent context.Context) error {
	async.Consume(parent, runtime.NumCPU()*8, w.queue)
	for row := range w.buffer {
		select {
		case <-parent.Done():
			return parent.Err()
		default:
		}
		w.queue <- w.addToQueue(row)
	}
	return nil
}

func (w *Writer) addToQueue(row block.Row) async.Task {
	return async.NewTask(func(ctx context.Context) (_ interface{}, err error) {
		v := row.Values
		r := new(bqRow)
		r.values = make(map[string]interface{})
		for k, v := range v {
			r.values[k] = v
		}
		r.values["ingested_at"] = r.values["ingested_at"].(time.Time).UnixMicro()
		j, _ := json.Marshal(r.values)
		message := dynamicpb.NewMessage(*w.messageDescriptor)
		if err := protojson.Unmarshal(j, message); err != nil {
			return nil, err
		}
		b, err := proto.Marshal(message)
		if err != nil {
			return nil, err
		}

		_, err = w.managedStream.AppendRows(w.context, [][]byte{b})
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
}

// setupDynamicDescriptors aids testing when not using a supplied proto
func setupDynamicDescriptors(schema bigquery.Schema) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, nil, err
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return nil, nil, err
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, errors.New("adapted descriptor is not a message descriptor")
	}
	return messageDescriptor, protodesc.ToDescriptorProto(messageDescriptor), nil
}

// Close closes the writer.
func (w *Writer) Close() error {
	if err := w.managedClient.Close(); err != nil {
		return err
	}
	if err := w.client.Close(); err != nil {
		return err
	}
	return nil
}
