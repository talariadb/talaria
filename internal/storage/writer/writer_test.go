package writer

import (
	"context"
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/stretchr/testify/assert"
)

func TestForCompaction(t *testing.T) {
	cfg := &config.Compaction{
		Sinks: []config.Sink{
			config.Sink{
				File: &config.FileSink{
					Directory: "./",
				},
			},
		},
	}

	compact, err := ForCompaction(context.Background(), cfg,
		monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"),
		disk.New(monitor.NewNoop()),
	)
	assert.NoError(t, err)
	assert.NotNil(t, compact)
}

func TestForStreaming(t *testing.T) {
	cfg := config.Streams{}
	compact, err := ForStreaming(cfg,
		monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"),
	)

	assert.Nil(t, err)
	assert.NotNil(t, compact)
}

func TestHash(t *testing.T) {

	row := map[string]interface{}{
		"a": 1654,
		"b": "hello world",
		"c": true,
	}

	h1 := hashOfRow(row)
	h2 := hashOfRow(row)
	assert.Equal(t, h1, h2)
}
