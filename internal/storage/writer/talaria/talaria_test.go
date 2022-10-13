package talaria

import (
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

func TestTalariaWriter(t *testing.T) {
	var timeout time.Duration = 5
	var concurrency int = 10
	var errorPercentage int = 50
	var maxSendMsgSize int = 32 * 1024 * 1024
	var maxRecvMsgSize int = 32 * 1024 * 1024
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	c, err := New("www.talaria.net:8043", "", "orc", m, &timeout, &concurrency, &errorPercentage, &maxSendMsgSize, &maxRecvMsgSize)

	// TODO: Impove test
	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write([]byte("hello"), []block.Block{})
	})

}
