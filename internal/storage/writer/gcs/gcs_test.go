package gcs

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	c, err := New("test", "test", "", "", m)

	// TODO: Impove test
	assert.Nil(t, err)
	assert.Error(t, c.Write(key.Key("test"), nil))
}
