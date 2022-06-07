package pubsub

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/grab/async"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/base"
	"google.golang.org/api/option"
)

// Writer to write and stream to PubSub
type Writer struct {
	*base.Writer
	client  *pubsub.Client
	topic   *pubsub.Topic
	monitor monitor.Monitor
	context context.Context
	buffer  chan []byte
	queue   chan async.Task
}

// New creates a new writer
func New(project, topic, encoding, filter string, monitor monitor.Monitor, opts ...option.ClientOption) (*Writer, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project, opts...)

	if err != nil {
		return nil, errors.Newf("pubsub: %v", err)
	}

	// Load encoder
	encoderWriter, err := base.New(filter, encoding, monitor)
	if err != nil {
		return nil, err
	}

	// Check if topic exists
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	topicRef := client.Topic(topic)
	ok, err := topicRef.Exists(ctx)

	if err != nil {
		return nil, errors.Newf("pubsub: %v", err)
	}
	if !ok {
		return nil, errors.New("pubsub: topic does not exist")
	}

	w := &Writer{
		topic:   topicRef,
		client:  client,
		Writer:  encoderWriter,
		monitor: monitor,
		context: ctx,
		buffer:  make(chan []byte, 65000),
		queue:   make(chan async.Task),
	}
	w.Process = w.process

	log.Println("pubsub: New writter successfully, topic ", topic)
	return w, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, blocks []block.Block) error {
	if len(blocks) == 0 {
		return nil
	}
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
	var results []*pubsub.PublishResult
	var resultErrors []error
	ctx := context.Background()
	for _, row := range rows {
		message, err := w.Writer.Encode(row)
		if err != nil {
			return err
		}
		result := w.topic.Publish(ctx, &pubsub.Message{
			Data: message,
		})
		results = append(results, result)
	}
	// The Get method blocks until a server-generated ID or
	// an error is returned for the published message.
	for _, res := range results {
		_, err := res.Get(ctx)
		if err != nil {
			resultErrors = append(resultErrors, err)
			fmt.Printf("Failed to publish: %v", err)
			continue
		}
	}
	return nil
}

// Stream encodes data and pushes it into buffer
func (w *Writer) Stream(row block.Row) error {
	filtered, err := w.Writer.Filter(row)
	// If message is filtered out, return nil
	if filtered == nil {
		return nil
	}
	if err != nil {
		return err
	}
	message, err := w.Writer.Encode(filtered)
	if err != nil {
		return err
	}

	select {
	case w.buffer <- message:
	default:
		return errors.New("pubsub: buffer is full")
	}
	return nil
}

// process will read from buffer and publish to PubSub
func (w *Writer) process(parent context.Context) error {
	async.Consume(parent, runtime.NumCPU()*8, w.queue)
	for message := range w.buffer {
		select {
		// Returns error if the parent context gets cancelled or Done() is not closed.
		// Err() returns nil if Done() is closed.
		case <-parent.Done():
			return parent.Err()
		default:
		}

		// Asynchronously adds a task to publish to Pub/Sub and check its response
		encoded := message
		w.queue <- async.NewTask(func(ctx context.Context) (interface{}, error) {
			result := w.topic.Publish(ctx, &pubsub.Message{
				Data: message,
			})
			return nil, w.processResult(result, encoded)
		})

	}
	return nil
}

// processResult will process the result from publish to check if there are errors
func (w *Writer) processResult(res *pubsub.PublishResult, message []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If stream hits error, repopulate message in buffer and return error
	if _, err := res.Get(ctx); err != nil {
		w.buffer <- message
		return err
	}
	return nil
}

// Close closes the Pub/Sub client
func (w *Writer) Close() error {
	return w.client.Close()
}
