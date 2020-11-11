package encoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncoder(t *testing.T) {
	data := make(map[string]interface{}, 1)
	data["test"] = "Hello Talaria"

	c, _ := New(nil, "", nil)
	dataString, err := c.Encode(&data)

	assert.IsType(t, []byte{}, dataString)
	assert.Nil(t, err)
}
