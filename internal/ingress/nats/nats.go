package nats

import (
	"context"
	"encoding/json"
	"fmt"

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
	JSClient jetstream.Client
	monitor  monitor.Monitor
	cancel   context.CancelFunc
	split    []splitWriter
}

type Event map[string]interface{}

type splitWriter struct {
	subject    string
	table      string
	queueGroup string
	queue      chan *nats.Msg
}

// New create new ingestion from nats jetstream to sinks.
func New(conf *config.NATS, monitor monitor.Monitor) (*Ingress, error) {
	jsClient, err := jetstream.New(conf, monitor)
	if err != nil {
		return nil, err
	}

	split := make([]splitWriter, len(conf.Split))
	for i, s := range conf.Split {
		split[i] = splitWriter{subject: s.Subject, table: s.Table, queue: make(chan *nats.Msg, 100), queueGroup: s.QueueGroup}
	}

	return &Ingress{
		JSClient: *jsClient,
		monitor:  monitor,
		split:    split,
	}, nil
}

// SubsribeHandler subscribes to specific subject and unmarshal the message into talaria's event type.
// The event message then will be used as the input of the handler function defined.
func (i *Ingress) SubsribeHandler(handler func(b []map[string]interface{}, table string)) error {
	for _, s := range i.split {
		// Queuesubscribe automatically create ephemeral push based consumer with queue group defined.
		sb, err := i.JSClient.Context.QueueSubscribe(s.subject, s.queueGroup, func(msg *nats.Msg) {
			block := make([]map[string]interface{}, 0)
			if err := json.Unmarshal(msg.Data, &block); err != nil {
				i.monitor.Error(errors.Internal("nats: unable to unmarshal", err))
			}
			i.monitor.Count1(ctxTag, "NATS.subscribe.count")
			handler(block, s.table)
		})
		if err != nil {
			i.monitor.Error(err)
		}
		// set higher pending limits
		sb.SetPendingLimits(65536, (1<<18)*1024)
		i.monitor.Info(fmt.Sprintf("%s->%s split created", s.subject, s.table))
	}
	return nil
}

// SubscribeHandlerWithPool process the message concurrently using goroutine pool.
// The message will be asynchornously executed to reduce the message process time to avoid being slow consumer.
func (i *Ingress) SubsribeHandlerWithPool(ctx context.Context, handler func(b []map[string]interface{}, split string)) error {
	// Initialze pool
	ctx, cancel := context.WithCancel(ctx)
	i.cancel = cancel

	i.initializeMemoryPool(ctx, handler)
	for _, s := range i.split {
		queue := s.queue
		_, err := i.JSClient.Context.QueueSubscribe(s.subject, s.queueGroup, func(msg *nats.Msg) {
			queue <- msg
		})
		if err != nil {
			i.monitor.Error(fmt.Errorf("nats: queue subscribe error %v", err))
			continue
		}
		i.monitor.Info(fmt.Sprintf("%s->%s split created", s.subject, s.table))
	}
	return nil
}

// Initialze memory pool for fixed number of goroutine to process the message
func (i *Ingress) initializeMemoryPool(ctx context.Context, handler func(b []map[string]interface{}, split string)) {
	for _, s := range i.split {
		for n := 0; n < 100; n++ {
			go func(n int, ctx context.Context, queue chan *nats.Msg, table string, sub string) {
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
						i.monitor.Count1(ctxTag, "NATS.msg.count")
						// asynchornously execute handler to reduce the message process time to avoid being slow consumer.
						handler(block, table)
					}
				}
			}(n, ctx, s.queue, s.table, s.subject)
		}
	}
}

// Close ingress
func (i *Ingress) Close() {
	i.JSClient.Server.Close()
	i.cancel()
	return
}
