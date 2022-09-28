package nats

import (
	"encoding/json"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/nats-io/nats.go"
)

type Ingress struct {
	// jetstream exposed interface.
	jetstream JetstreamI
	monitor   monitor.Monitor
	conn      *nats.Conn
}

type jetstream struct {
	// The name of the queue group.
	queue   string // The name of subject listening to.
	subject string
	// The jetstream context which provide jetstream api.
	jsContext nats.JetStreamContext
}

type JetstreamI interface {
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
		jetstream: NewJetStream(conf.Subject, conf.Queue, js),
		monitor:   monitor,
		conn:      nc,
	}, nil
}

func NewJetStream(subject, queue string, js nats.JetStreamContext) *jetstream {
	return &jetstream{
		subject:   subject,
		queue:     queue,
		jsContext: js,
	}
}

func (s *jetstream) Subscribe(handler nats.MsgHandler) (*nats.Subscription, error) {
	// Queuesubscribe automatically create ephemeral push based consumer with queue group defined.
	sb, err := s.jsContext.QueueSubscribe(s.subject, s.queue, handler)
	if err != nil {
		return nil, err
	}
	return sb, nil
}

func (s *jetstream) Publish(msg []byte) (nats.PubAckFuture, error) {
	p, err := s.jsContext.PublishAsync(s.subject, msg)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// SubsribeHandler subscribes to specific subject and unmarshal the message into talaria's event type.
// The event message then will be used as the input of the handler function defined.
func (i *Ingress) SubsribeHandler(handler func(b []map[string]interface{})) error {
	var block []map[string]interface{}

	_, err := i.jetstream.Subscribe(func(msg *nats.Msg) {
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

// Close ingress
func (i *Ingress) Close() {
	i.conn.Close()
	return
}
