package file

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	c, err := New("testdata")
	defer func() { _ = os.RemoveAll("testdata") }()

	// TODO: Impove test
	assert.NotNil(t, c)
	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		c.Write([]byte("abc"), []byte("hello"))
	})
}
