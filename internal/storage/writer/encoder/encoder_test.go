package encoder

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/stretchr/testify/assert"
)

func TestEncoder(t *testing.T) {
	data := make(map[string]interface{}, 1)
	data["test"] = "Hello Talaria"

	row := block.Row{
		Values: data,
	}

	c, _ := New(nil, "", nil)
	dataString, err := c.Encode(row)

	assert.IsType(t, []byte{}, dataString)
	assert.Nil(t, err)

	enc, err := New(nil, "wrongEncoder", nil)
	assert.Nil(t, enc)
	assert.Error(t, err)
}

func TestFilter(t *testing.T) {
	data := make(map[string]interface{}, 1)
	data["test"] = "Hello Talaria"

	row := block.Row{
		Values: data,
	}

	filter := make(map[string]string, 1)
	filter["test"] = "Hi Talaria"

	enc1, _ := New(filter, "", nil)
	dataString, _ := enc1.Encode(row)

	assert.Nil(t, dataString)

	filter2 := make(map[string]string, 1)
	filter2["test2"] = "Hello Talaria"

	enc2, _ := New(filter2, "", nil)
	dataString2, _ := enc2.Encode(row)

	assert.Nil(t, dataString2)

	filter3 := make(map[string]string, 1)
	filter3["test"] = "Hello Talaria"

	enc3, _ := New(filter3, "", nil)
	dataString3, _ := enc3.Encode(row)

	assert.IsType(t, []byte{}, dataString3)
	assert.Equal(t, "{\"test\":\"Hello Talaria\"}", string(dataString3))
}
