package client

import (
	"testing"
	"time"

	"github.com/myteksi/hystrix-go/hystrix"
	"github.com/stretchr/testify/assert"
)

func TestWithNetwork(t *testing.T) {
	client, _ := Dial("invalid", WithNonBlock(), WithNetwork(10*time.Second))
	assert.Equal(t, 10*time.Second, client.netconf.DialTimeout)
}

func TestWithLoadBalancer(t *testing.T) {
	client, _ := Dial("invalid", WithNonBlock(), WithLoadBalancer("round_robin"))
	assert.Equal(t, "round_robin", client.netconf.LoadBalancer)
}

func TestWithCircuit(t *testing.T) {
	client, _ := Dial("invalid", WithNonBlock(), WithCircuit(200*time.Millisecond, 500, 50))
	expectedCircuit := map[string]hystrix.CommandConfig{
		commandName: {
			Timeout:               200,
			MaxConcurrentRequests: 500,
			ErrorPercentThreshold: 50,
		},
	}
	assert.Equal(t, expectedCircuit, client.netconf.CircuitOptions)
}
