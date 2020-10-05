package talaria

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTalariaWriter(t *testing.T) {
	c, err := New("http://www.test.net")

	// TODO: Impove test
	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write([]byte("abc"), []byte("hello"))
	})
}
