package nats

import (
	"fmt"
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestSubscribe(t *testing.T) {
	conf := &config.NATS{
		URL:     nats.DefaultURL,
		Subject: "event.talaria",
		Queue:   "talarias",
	}
	ingress, err := New(conf, monitor.NewNoop())
	assert.Nil(t, err)
	assert.NotNil(t, ingress)

	dataCn := make(chan string)
	go func(data chan string) {
		ingress.subscriber.Subscribe(func(msg *nats.Msg) {
			dataCn <- string(msg.Data)
		})
	}(dataCn)

	p, err := ingress.subscriber.Publish([]byte("test"))
	assert.NotNil(t, p)
	assert.Nil(t, err)

	data := <-dataCn
	fmt.Println(data)

	assert.NotEmpty(t, data)
}
