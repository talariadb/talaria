package pubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/writer/base"
	"google.golang.org/api/option"
)

// Writer to write and streaming to PubSub
type Writer struct {
	client  *pubsub.Client
	topic   *pubsub.Topic
	Writer  *base.Writer
	monitor monitor.Monitor
	context context.Context
	buffer  chan []byte
	errs    chan error
}

// New creates a new writer
func New(project, topic, encoding, filter string, loader *script.Loader, monitor monitor.Monitor, opts ...option.ClientOption) (*Writer, error) {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, project, opts...)

	if err != nil {
		return nil, errors.Newf("pubsub: %v", err)
	}

	topicRef := client.Topic(topic)

	encoderWriter, err := base.New(filter, encoding, loader)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		topic:   topicRef,
		client:  client,
		Writer:  encoderWriter,
		monitor: monitor,
		context: ctx,
		buffer:  make(chan []byte, 65000),
		errs:    make(chan error, 1),
	}

	// Launch a goroutine that loops infinitely over buffer
	go w.process()

	return w, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key.Key, []byte) error {
	return nil // Noop
}

// Stream publishes data into PubSub topics
func (w *Writer) Stream(row block.Row) error {
	message, err := w.Writer.Encode(row)
	if err != nil {
		return err
	}

	// If message is filtered out, return nil
	if message == nil {
		return nil
	}

	select {
	case w.buffer <- message:
	default:
		w.monitor.Warning(errors.Internal("pubsub: buffer is full", err))
	}
	return nil
}

// process will read from buffer, encode the row, and publish to PubSub
func (w *Writer) process() error {

	ctx := context.Background()

	for message := range w.buffer {
		select {
		case err := <-w.errs:
			w.monitor.Warning(errors.Internal("pubsub: unable to stream, sleeping for 10 seconds before retrying", err))
			time.Sleep(10 * time.Second)
		default:
		}

		result := w.topic.Publish(ctx, &pubsub.Message{
			Data: message,
		})

		go w.processResult(result, message)
	}
	return nil
}

func (w *Writer) processResult(res *pubsub.PublishResult, message []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If stream hits error, send err to error channel and repopulate message in buffer
	if _, err := res.Get(ctx); err != nil {
		w.errs <- err
		w.buffer <- message
		return
	}
}

// Close closes the writer.
func (w *Writer) Close() error {
	return w.client.Close()
}
