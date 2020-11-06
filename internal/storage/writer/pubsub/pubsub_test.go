package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamer(t *testing.T) {
	c, _ := New("airasia-datasciencesandbox-poc", "talaria", "", "")

	assert.Nil(t, c.Stream())
}
