package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {

	c, err := New("my-project-id", "mydataset", "mytable")

	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write([]byte("abc"), []byte("hello"))
	})
}
