package talaria

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTalariaWriter(t *testing.T) {
	c, err := New("www.talaria.net:8043", 5, 5, 10, 50, true)

	// TODO: Impove test
	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write([]byte("abc"), []byte("hello"))
	})
}
