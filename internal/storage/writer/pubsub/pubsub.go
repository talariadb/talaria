package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/encoder"
)

// Writer to write and streaming to PubSub
type Writer struct {
	client  *pubsub.Client
	topic   *pubsub.Topic
	Writer  *encoder.Writer
	context context.Context
	buffer  chan *map[string]interface{}
	sent    chan bool
}

// New creates a new writer
func New(project string, topic string, filter string, encoding string) (*Writer, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Newf("pubsub: %v", err)
	}

	topicRef := client.Topic(topic)

	encoderWriter, err := encoder.New(filter, encoding)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		topic:   topicRef,
		client:  client,
		Writer:  encoderWriter,
		context: ctx,
		buffer:  make(chan *map[string]interface{}, 65000),
		sent:    make(chan bool, 65000),
	}
	go w.process()

	return w, nil
}

// Stream publishes data into PubSub topics
func (w *Writer) Stream(row *map[string]interface{}) error {
	w.buffer <- row

	for v := range w.sent {
		if v {
			return nil
		}
	}

	return errors.New("pubsub: no signal received to signify data sent")
}

// process will read from buffer, encode the row, and publish to PubSub
func (w *Writer) process() error {
	errs := make(chan error, 1)
	ctx := context.Background()

	for row := range w.buffer {

		encodedRow, err := w.Writer.Encode(row)

		if err != nil {
			return errors.Internal("pubsub: unable to encode row", err)
		}

		result := w.topic.Publish(ctx, &pubsub.Message{
			Data: encodedRow,
			Attributes: map[string]string{
				"origin": "talaria",
			},
		})

		w.sent <- true

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
