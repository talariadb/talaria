package streaming

import (
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	cfg := make([]config.Streaming, 0)
	cfg = append(cfg, config.Streaming{
		PubSubStream: &config.PubSubStream{
			Project: "gcp-project",
			Topic:   "my-topic",
			Filter:  "my-filter",
			Encoder: "csv",
		},
		RedisStream: nil,
	})

	cfg = append(cfg, config.Streaming{
		PubSubStream: nil,
		RedisStream: &config.RedisStream{
			Project: "redis-project",
			Topic:   "redis-topic",
		},
	})

	streamer := New(cfg,
		monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"),
	)

	assert.NotNil(t, streamer)

}
