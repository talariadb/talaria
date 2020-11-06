package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/encoder"
)

// Writer to write and streaming to PubSub
type Writer struct {
	client  *pubsub.Client
	topic   *pubsub.Topic
	Writer  *encoder.Writer
	context context.Context
	buffer  chan *block.Row
}

// New creates a new streamer
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

	return &Writer{
		topic:   topicRef,
		client:  client,
		Writer:  encoderWriter,
		context: ctx,
		buffer:  make(chan *block.Row, 65000),
	}, nil
}

// Stream publishes data into PubSub topics
func (s *Writer) Stream(row *block.Row) {
	s.buffer <- row
}

func (s *Writer) process() error {

	errs := make(chan error, 1)
	ctx := context.Background()

	for row := range s.buffer {

		encodedRow, err := s.Writer.Encode(row)

		if err != nil {
			return errors.Internal("pubsub: unable to encode row", err)
		}

		result := s.topic.Publish(ctx, &pubsub.Message{
			Data: encodedRow,
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
}

// Close closes the writer.
func (s *Writer) Close() error {
	return s.client.Close()
}
