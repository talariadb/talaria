package file

import (
	"os"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	c, err := New("testdata", "", "", nil)
	defer func() { _ = os.RemoveAll("testdata") }()

	// TODO: Impove test
	assert.NotNil(t, c)
	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		c.Write(key.Key("test"), nil)
	})
}
