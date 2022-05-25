package bigquery

import (
	"testing"

	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	c, err := New("my-project-id", "mydataset", "mytable", "", "", m)

	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write([]byte("abc"), nil)
	})
}
