package base

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/block"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	row := block.Row{
		Values: map[string]interface{}{
			"test": "Hello Talaria",
			"age":  30,
		},
	}

	filter := `function main(row) 
	return row['age'] > 10
end
	`

	loader := script.NewLoader(nil)
	enc1, _ := New(filter, "json", loader)
	dataString, err := enc1.Encode(row)

	assert.Equal(t, `{"age":30,"test":"Hello Talaria"}`, string(dataString))
	assert.NoError(t, err)
}

func TestEmptyFilter(t *testing.T) {
	loader := script.NewLoader(nil)
	enc1, err := New("", "json", loader)
	assert.NotNil(t, enc1)
	assert.NoError(t, err)
}
