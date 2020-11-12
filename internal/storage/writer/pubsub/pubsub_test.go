package pubsub

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/stretchr/testify/assert"
)

func TestStreamer(t *testing.T) {
	row := block.NewRow(nil, 0)
	row.Set("test", "Hello Talaria")

	c, _ := New("gcp-project", "talaria", "", "", nil, nil)

	assert.NoError(t, c.Stream(row))
}
