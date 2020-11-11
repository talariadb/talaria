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
	"github.com/kelindar/talaria/internal/storage/writer/encoder"
)

// Writer to write and streaming to PubSub
type Writer struct {
	client  *pubsub.Client
	topic   *pubsub.Topic
	Writer  *encoder.Writer
	monitor monitor.Monitor
	context context.Context
	buffer  chan []byte
}

// New creates a new writer
func New(project, topic, encoding string, filter map[string]string, loader *script.Loader, monitor monitor.Monitor) (*Writer, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Newf("pubsub: %v", err)
	}

	topicRef := client.Topic(topic)

	encoderWriter, err := encoder.New(filter, encoding, loader)
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

	w.buffer <- message
	return nil
}

// process will read from buffer, encode the row, and publish to PubSub
func (w *Writer) process() error {
	errs := make(chan error, 1)
	ctx := context.Background()
	encounteredError := false

	for message := range w.buffer {

		if encounteredError {
			time.Sleep(15 * time.Second)
			encounteredError = false
		}

		result := w.topic.Publish(ctx, &pubsub.Message{
			Data: message,
		})

		go func(res *pubsub.PublishResult, message []byte) {
			_, err := res.Get(ctx)
			if err != nil {
				// If stream hits error, send err to error channel and repopulate message in buffer
				errs <- err
				w.buffer <- message
				return
			}
		}(result, message)

		select {
		case err := <-errs:
			encounteredError = true
			w.monitor.Warning(errors.Internal("pubsub: unable to stream", err))
		default:
			continue
		}
	}
	return nil
}

// Close closes the writer.
func (w *Writer) Close() error {
	return w.client.Close()
}
