package nats

import (
	"encoding/json"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/nats-io/nats.go"
)

type Ingress struct {
	subscriber subscribe // The Nats subscriber.
	monitor    monitor.Monitor
	sinks      []config.Sink // The sink destinations which the subscription will sink to.
}

type subscriber struct {
	// The name of the queue group.
	queue string
	// The name of subject listening to.
	subject string
	// The jetstream context which provide jetstream api.
	jsContext nats.JetStreamContext
}

type subscribe interface {
	// Subscribe to defined subject from Nats server.
	Subscribe(handler nats.MsgHandler) (*nats.Subscription, error)
	Publish(msg []byte) (nats.PubAckFuture, error)
}

type Event map[string]interface{}

// New create new ingestion from nats jetstream to sinks.
func New(conf *config.NATS, monitor monitor.Monitor) (*Ingress, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, err
	}

	return &Ingress{
		subscriber: NewSubscriber(conf.Subject, conf.Queue, js),
		monitor:    monitor,
	}, nil
}

func NewSubscriber(subject, queue string, js nats.JetStreamContext) *subscriber {
	return &subscriber{
		subject:   subject,
		queue:     queue,
		jsContext: js,
	}
}

func (s *subscriber) Subscribe(handler nats.MsgHandler) (*nats.Subscription, error) {
	// Queuesubscribe automatically create ephemeral push based consumer with queue group defined
	sb, err := s.jsContext.QueueSubscribe(s.subject, s.queue, handler)
	if err != nil {

		return nil, err
	}
	return sb, nil
}

func (s *subscriber) Publish(msg []byte) (nats.PubAckFuture, error) {
	p, err := s.jsContext.PublishAsync(s.subject, msg)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// SubsribeThen will perform sink whenever there is event coming from stream.
func (i *Ingress) SubsribeHandler(handler func(b []map[string]interface{})) error {
	var block []map[string]interface{}

	_, err := i.subscriber.Subscribe(func(msg *nats.Msg) {
		if err := json.Unmarshal(msg.Data, &block); err != nil {
			i.monitor.Error(errors.Internal("nats: unable to unmarshal", err))
		}
		handler(block)
	})
	if err != nil {
		return err
	}
	return nil
}
