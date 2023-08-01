package jetstream

import (
	"fmt"
	"log"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/nats-io/nats.go"
)

type JSClient interface {
	Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error)
	QueueSubscribe(subj, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error)
	Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
	// PublishMsg publishes a Msg to JetStream.
	PublishMsg(m *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error)
	// AddStream creates a stream.
	AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error)
	// DeleteStream deletes a stream.
	DeleteStream(name string, opts ...nats.JSOpt) error
}

type NatsClient interface {
	Close()
}

type Client struct {
	Context JSClient
	Server  NatsClient
}

// Create new jetstream client.
func New(conf *config.NATS, monitor monitor.Monitor) (*Client, error) {
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

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	client := &Client{js, nc}

	return client, nil
}

func natsErrHandler(nc *nats.Conn, sub *nats.Subscription, natsErr error) {
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
