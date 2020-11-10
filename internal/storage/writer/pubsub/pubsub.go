package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/writer/encoder"
)

// Writer to write and streaming to PubSub
type Writer struct {
	client  *pubsub.Client
	topic   *pubsub.Topic
	Writer  *encoder.Writer
	context context.Context
	buffer  chan []byte
}

// New creates a new writer
func New(project, topic, filter, encoding string, loader *script.Loader) (*Writer, error) {
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
		context: ctx,
		buffer:  make(chan []byte, 65000),
	}
	go w.process()

	return w, nil
}

// Write writes the data to the sink.
func (*Writer) Write(key.Key, []byte) error {
	return nil // Noop
}

// Stream publishes data into PubSub topics
func (w *Writer) Stream(row block.Row) error {
	message, err := w.Writer.Encode(row)
	if err != nil {
		return err
	}

	w.buffer <- message
	return nil
}

// process will read from buffer, encode the row, and publish to PubSub
func (w *Writer) process() error {
	errs := make(chan error, 1)
	ctx := context.Background()

	for message := range w.buffer {
		result := w.topic.Publish(ctx, &pubsub.Message{
			Data: message,
			Attributes: map[string]string{
				"origin": "talaria",
			},
		})

		go func(res *pubsub.PublishResult) {
			id, err := res.Get(ctx)
			if err != nil {
				// When stream is down - Process messages or stop - config
				errs <- err
				return
			}
			fmt.Println("Published message, msg ID:", id)
		}(result)

		select {
		case err := <-errs:
			return errors.Internal("pubsub: unable to stream", err)
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
