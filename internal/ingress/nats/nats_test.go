package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/config/env"
	"github.com/kelindar/talaria/internal/config/static"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

const TEST_PORT = 8369

var conf config.NATS

func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	return RunServerWithOptions(&opts)
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

func TestLoadNatsConfig(t *testing.T) {
	const refreshTime = 50 * time.Millisecond
	const waitTime = 100 * time.Millisecond
	nats := &config.NATS{
		Host:    "nats://127.0.0.1",
		Port:    TEST_PORT,
		Subject: "event.talaria",
		Queue:   "talarias",
	}

	os.Setenv("TALARIA_WRITERS_NATS_HOST", "nats://127.0.0.1")
	os.Setenv("TALARIA_WRITERS_NATS_PORT", fmt.Sprint(TEST_PORT))
	os.Setenv("TALARIA_WRITERS_NATS_SUBJECT", "event.talaria")
	os.Setenv("TALARIA_WRITERS_NATS_QUEUE", "talarias")

	cfg := config.Load(context.Background(), refreshTime, static.New(), env.New("TALARIA"))
	assert.Equal(t, nats, cfg().Writers.NATS)

	conf = *cfg().Writers.NATS
}

func TestSubscribe(t *testing.T) {
	s := RunServerOnPort(int(conf.Port))
	defer s.Shutdown()

	ingress, err := New(&conf, monitor.NewNoop())
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

func TestSubscribeHandler(t *testing.T) {

	s := RunServerOnPort(int(conf.Port))
	defer s.Shutdown()

	ingress, err := New(&conf, monitor.NewNoop())
	assert.Nil(t, err)
	assert.NotNil(t, ingress)

	//Create stream
	jsCtx, err := ingress.conn.JetStream()
	assert.Nil(t, err)
	jsCtx.AddStream(&nats.StreamConfig{Name: "events", Subjects: []string{"event.>"}})

	dataCn := make(chan []map[string]interface{})
	ingress.SubsribeHandler(func(block []map[string]interface{}) {
		dataCn <- block
	})
	test := []map[string]interface{}{{
		"event": "event1",
		"text":  "hi",
	}}
	b, _ := json.Marshal(test)

	p, err := ingress.jetstream.Publish(b)
	assert.NotNil(t, p)
	assert.Nil(t, err)

	block := <-dataCn
	assert.NotEmpty(t, block)
}
