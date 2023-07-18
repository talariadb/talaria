package nats

import (
	"context"
	"encoding/json"
	"log"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/ingress/nats/jetstream"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/nats-io/nats.go"
)

const (
	ctxTag          = "NATS"
	tableNameHeader = "table"
)

type Ingress struct {
	JSClient   jetstream.Client
	queueGroup string
	subject    string
	monitor    monitor.Monitor
	queue      chan *nats.Msg
	cancel     context.CancelFunc
}

type Event map[string]interface{}

// New create new ingestion from nats jetstream to sinks.
func New(conf *config.NATS, monitor monitor.Monitor) (*Ingress, error) {
	jsClient, err := jetstream.New(conf, monitor)
	if err != nil {
		return nil, err
	}

	return &Ingress{
		JSClient:   *jsClient,
		monitor:    monitor,
		queueGroup: conf.Queue,
		subject:    conf.Subject,
		queue:      make(chan *nats.Msg, 100),
	}, nil
}

// SubsribeHandler subscribes to specific subject and unmarshal the message into talaria's event type.
// The event message then will be used as the input of the handler function defined.
func (i *Ingress) SubsribeHandler(handler func(b []map[string]interface{}, table string)) error {
	// Queuesubscribe automatically create ephemeral push based consumer with queue group defined.
	sb, err := i.JSClient.Context.QueueSubscribe(i.subject, i.queueGroup, func(msg *nats.Msg) {
		block := make([]map[string]interface{}, 0)
		if err := json.Unmarshal(msg.Data, &block); err != nil {
			i.monitor.Error(errors.Internal("nats: unable to unmarshal", err))
		}
		i.monitor.Count1(ctxTag, "NATS.subscribe.count")
		// Get table name from header
		table := msg.Header.Get(tableNameHeader)
		handler(block, table)
	})
	if err != nil {
		return err
	}
	// set higher pending limits
	sb.SetPendingLimits(65536, (1<<18)*1024)
	_, b, _ := sb.PendingLimits()
	log.Println("nats: maximum pending limits (bytes): ", b)
	return nil
}

// SubscribeHandlerWithPool process the message concurrently using goroutine pool.
// The message will be asynchornously executed to reduce the message process time to avoid being slow consumer.
func (i *Ingress) SubsribeHandlerWithPool(ctx context.Context, handler func(b []map[string]interface{}, split string)) error {
	// Initialze pool
	ctx, cancel := context.WithCancel(ctx)
	i.cancel = cancel

	i.initializeMemoryPool(ctx, handler)
	_, err := i.JSClient.Context.QueueSubscribe(i.subject, i.queueGroup, func(msg *nats.Msg) {
		i.queue <- msg
	})
	if err != nil {
		return err
	}
	return nil
}

// Initialze memory pool for fixed number of goroutine to process the message
func (i *Ingress) initializeMemoryPool(ctx context.Context, handler func(b []map[string]interface{}, split string)) {
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
					// Get table name from header
					table := msg.Header.Get(tableNameHeader)
					// asynchornously execute handler to reduce the message process time to avoid being slow consumer.
					handler(block, table)
				}
			}
		}(n, ctx, i.queue)
	}
}

// Close ingress
func (i *Ingress) Close() {
	i.JSClient.Server.Close()
	i.cancel()
	return
}
