package talaria

import (
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/encoding/block"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/stretchr/testify/assert"
)

func TestTalariaWriter(t *testing.T) {
	var timeout time.Duration = 5
	var concurrency int = 10
	var errorPercentage int = 50
	l := script.NewLoader(nil)
	c, err := New("www.talaria.net:8043", "", "orc", l, &timeout, &concurrency, &errorPercentage)

	// TODO: Impove test
	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write([]byte("hello"), []block.Block{})
	})

}
