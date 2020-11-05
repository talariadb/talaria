package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Streamer to write to PubSub
type Streamer struct {
	endpoint string
	client   *pubsub.Client
	topic    *pubsub.Topic
	context  context.Context
}

// New creates a new streamer
func New(project, topic, filter, encoder string) (*Streamer, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Newf("pubsub: %v", err)
	}

	topicRef := client.Topic(topic)
	return &Streamer{
		topic:   topicRef,
		client:  client,
		context: ctx,
	}, nil
}

// Stream publishes data into PubSub topics
func (s *Streamer) Stream() error {
	ctx := context.Background()
	result := s.topic.Publish(ctx, &pubsub.Message{
		Data: []byte("Hello world!"),
		Attributes: map[string]string{
			"origin": "talaria",
		},
	})

	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("Get: %v", err)
	}
	fmt.Println("Published message with custom attributes: ", id)
	return nil
}

// Close closes the writer.
func (s *Streamer) Close() error {
	return s.client.Close()
}
