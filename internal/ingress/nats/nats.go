package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/nats-io/nats.go"
)

const (
	ctxTag = "NATS"
)

type Ingress struct {
	// jetstream exposed interface.
	jetstream JetstreamI
	monitor   monitor.Monitor
	conn      *nats.Conn
	queue     chan *nats.Msg
	cancel    context.CancelFunc
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
	nc, err := nats.Connect(fmt.Sprintf("%s:%d", conf.Host, conf.Port), nats.ReconnectHandler(func(_ *nats.Conn) {
		log.Println("Successfully renonnect")
	}), nats.ClosedHandler(func(nc *nats.Conn) {
		log.Printf("Connection close due to %q", nc.LastError())
		monitor.Error(nc.LastError())
	}), nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Got disconnected. Reason: %q\n", nc.LastError())
		monitor.Error(nc.LastError())
	}), nats.ErrorHandler(natsErrHandler))
	if err != nil {
		return nil, err
	}

	js, err := NewJetStream(conf.Subject, conf.Queue, nc)
	if err != nil {
		return nil, err
	}

	return &Ingress{
		jetstream: js,
		monitor:   monitor,
		conn:      nc,
		queue:     make(chan *nats.Msg, 100),
	}, nil
}

func natsErrHandler(nc *nats.Conn, sub *nats.Subscription, natsErr error) {
	log.Printf("error: %v\n", natsErr)
	if natsErr == nats.ErrSlowConsumer {
		pendingMsgs, _, err := sub.Pending()
		if err != nil {
			log.Printf("couldn't get pending messages: %v", err)
			return
		}
		log.Printf("Falling behind with %d pending messages on subject %q.\n",
			pendingMsgs, sub.Subject)
	}
}

// NewJetStream create Jetstream context
func NewJetStream(subject, queue string, nc *nats.Conn) (*jetstream, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	return &jetstream{
		subject:   subject,
		queue:     queue,
		jsContext: js,
	}, nil
}

// Subscribe to a subject in nats server
func (s *jetstream) Subscribe(handler nats.MsgHandler) (*nats.Subscription, error) {
	// Queuesubscribe automatically create ephemeral push based consumer with queue group defined.
	sb, err := s.jsContext.QueueSubscribe(s.subject, s.queue, handler)
	if err != nil {
		return nil, err
	}
	// set higher pending limits
	sb.SetPendingLimits(65536, (1<<18)*1024)
	_, b, _ := sb.PendingLimits()
	log.Println("nats: maximum pending limits (bytes): ", b)
	return sb, nil
}

// Publish message to the subject in nats server
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
	_, err := i.jetstream.Subscribe(func(msg *nats.Msg) {
		block := make([]map[string]interface{}, 0)
		if err := json.Unmarshal(msg.Data, &block); err != nil {
			i.monitor.Error(errors.Internal("nats: unable to unmarshal", err))
		}
		i.monitor.Count1(ctxTag, "NATS.subscribe.count")
		handler(block)
	})
	if err != nil {
		return err
	}
	return nil
}

// SubscribeHandlerWithPool process the message concurrently using goroutine pool.
// The message will be asynchornously executed to reduce the message process time to avoid being slow consumer.
func (i *Ingress) SubsribeHandlerWithPool(ctx context.Context, handler func(b []map[string]interface{})) error {
	// Initialze pool
	ctx, cancel := context.WithCancel(ctx)
	i.cancel = cancel

	i.initializeMemoryPool(ctx, handler)
	_, err := i.jetstream.Subscribe(func(msg *nats.Msg) {
		// Send the message to the queue
		i.queue <- msg
	})
	if err != nil {
		return err
	}
	return nil
}

// Initialze memory pool for fixed number of goroutine to process the message
func (i *Ingress) initializeMemoryPool(ctx context.Context, handler func(b []map[string]interface{})) {
	for n := 0; n < 100; n++ {
		go func(n int, ctx context.Context, queue chan *nats.Msg) {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-queue:
					// Wait for the message
					block := make([]map[string]interface{}, 0)
					if err := json.Unmarshal(msg.Data, &block); err != nil {
						i.monitor.Error(errors.Internal("nats: unable to unmarshal", err))
					}
					i.monitor.Count1(ctxTag, "NATS.subscribe.count")
					// asynchornously execute handler to reduce the message process time to avoid being slow consumer.
					handler(block)
				}
			}
		}(n, ctx, i.queue)
	}
}

// Close ingress
func (i *Ingress) Close() {
	i.conn.Close()
	i.cancel()
	return
}
