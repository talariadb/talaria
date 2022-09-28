package nats

import (
	"fmt"
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

const TEST_PORT = 8369

func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	return RunServerWithOptions(&opts)
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

func TestSubscribe(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	sUrl := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)

	conf := &config.NATS{
		URL:     sUrl,
		Subject: "event.talaria",
		Queue:   "talarias",
	}
	ingress, err := New(conf, monitor.NewNoop())
	assert.Nil(t, err)
	assert.NotNil(t, ingress)

	//Create stream
	jsCtx, err := ingress.conn.JetStream()
	assert.Nil(t, err)
	jsCtx.AddStream(&nats.StreamConfig{Name: "events", Subjects: []string{"event.>"}})

	dataCn := make(chan string)
	ingress.jetstream.Subscribe(func(msg *nats.Msg) {
		dataCn <- string(msg.Data)
	})

	p, err := ingress.jetstream.Publish([]byte("test"))
	assert.NotNil(t, p)
	assert.Nil(t, err)

	data := <-dataCn
	assert.NotEmpty(t, data)
}
