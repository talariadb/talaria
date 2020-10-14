package talaria

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTalariaWriter(t *testing.T) {
	var timeout time.Duration = 5
	var concurrency int = 10
	var errorPercentage int = 50
	c, err := New("www.talaria.net:8043", &timeout, &concurrency, &errorPercentage)

	// TODO: Impove test
	assert.Nil(t, c)
	assert.Error(t, err)

	assert.Panics(t, func() {
		c.Write([]byte("abc"), []byte("hello"))
	})

}
