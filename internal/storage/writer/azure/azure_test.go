package azure

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	os.Setenv("AZURE_STORAGE_ACCOUNT", "golangrocksonazure")
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "YmFy")
	c, err := New("test", "test")

	assert.NotNil(t, c)
	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		err := c.Write([]byte("abc"), []byte("hello"))
		assert.NotNil(t, err)
	})
}
