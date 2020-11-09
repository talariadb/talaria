package gcs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	c, err := New("test", "test")

	// TODO: Impove test
	assert.NotNil(t, c)
	assert.NoError(t, err)
	assert.Error(t, c.Write([]byte("abc"), []byte("hello")))
}
