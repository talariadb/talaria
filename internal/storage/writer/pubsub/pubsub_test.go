package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamer(t *testing.T) {
	data := make(map[string]interface{}, 1)

	data["test"] = "Hello Talaria"

	c, _ := New("gcp-project", "talaria", "", "")

	assert.Nil(t, c.Stream(&data))
}
