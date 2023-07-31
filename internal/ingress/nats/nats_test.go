package nats

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/config/env"
	"github.com/kelindar/talaria/internal/config/s3"
	"github.com/kelindar/talaria/internal/config/static"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
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
		Host: "nats://127.0.0.1",
		Port: TEST_PORT,
		Split: []config.SplitWriter{
			{Subject: "event.talaria", QueueGroup: "talarias", Table: "event"},
		},
	}
	os.Setenv("NATS_URI", "file:///nats_test_config.yaml")

	s3Configurer := s3.New(logging.NewStandard())
	cfg := config.Load(context.Background(), refreshTime, static.New(), env.New("NATS"), s3Configurer)

	assert.Equal(t, nats, cfg().Writers.NATS)

	conf = *cfg().Writers.NATS
}

func TestSubscribeHandler(t *testing.T) {

	s := RunServerOnPort(int(conf.Port))
	defer s.Shutdown()

	ingress, err := New(&conf, monitor.NewNoop())
	assert.Nil(t, err)
	assert.NotNil(t, ingress)

	// Delete stream first in case exists
	ingress.JSClient.Context.DeleteStream("events")

	info, err := ingress.JSClient.Context.AddStream(&nats.StreamConfig{Name: "events", Subjects: []string{"event.>"}})
	assert.NoError(t, err)
	assert.NotNil(t, info)

	dataCn := make(chan []map[string]interface{})
	ingress.SubsribeHandler(func(block []map[string]interface{}, table string) {
		dataCn <- block
		assert.Equal(t, conf.Split[0].Table, table)
	})
	test := []map[string]interface{}{{
		"event": "event1",
		"text":  "hi",
	}}

	// Publish message
	msg := nats.NewMsg("event.talaria")
	b, _ := json.Marshal(test)
	msg.Data = b

	p, err := ingress.JSClient.Context.PublishMsg(msg)
	assert.NotNil(t, p)
	assert.Nil(t, err)

	block := <-dataCn
	assert.NotEmpty(t, block)
}

func TestSubscribeHandlerWithPool(t *testing.T) {

	s := RunServerOnPort(int(conf.Port))
	defer s.Shutdown()

	ingress, err := New(&conf, monitor.NewNoop())
	assert.Nil(t, err)
	assert.NotNil(t, ingress)

	// Delete stream first in case exists
	ingress.JSClient.Context.DeleteStream("events")

	info, err := ingress.JSClient.Context.AddStream(&nats.StreamConfig{Name: "events", Subjects: []string{"event.>"}})
	assert.NoError(t, err)
	assert.NotNil(t, info)

	dataCn := make(chan []map[string]interface{})
	ctx, cancel := context.WithCancel(context.Background())
	ingress.SubsribeHandlerWithPool(ctx, func(block []map[string]interface{}, table string) {
		dataCn <- block
		assert.Equal(t, conf.Split[0].Table, table)
	})
	test := []map[string]interface{}{{
		"event": "event1",
		"text":  "hi",
	}}

	// Publish message
	msg := nats.NewMsg("event.talaria")
	b, _ := json.Marshal(test)
	msg.Data = b

	p, err := ingress.JSClient.Context.PublishMsg(msg)
	assert.NotNil(t, p)
	assert.Nil(t, err)

	block := <-dataCn
	assert.NotEmpty(t, block)
	cancel()
}
