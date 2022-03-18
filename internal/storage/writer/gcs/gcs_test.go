package gcs

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	c, err := New("test", "test", "", "", nil)

	// TODO: Impove test
	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write(key.Key("test"), nil)
	})
}
