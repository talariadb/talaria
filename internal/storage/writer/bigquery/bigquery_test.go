package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {

	c, err := New("my-project-id", "mydataset", "mytable")

	assert.NotNil(t, c)
	assert.NoError(t, err)
	assert.Error(t, c.Write([]byte("abc"), []byte("hello")))

}
