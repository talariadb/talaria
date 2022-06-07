package file

import (
	"os"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	c, err := New("testdata", "", "", m)
	defer func() { _ = os.RemoveAll("testdata") }()

	// TODO: Impove test
	assert.NotNil(t, c)
	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		c.Write(key.Key("test"), nil)
	})
}
