package azure

import (
	"os"
	"strings"
	"testing"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	os.Setenv("AZURE_STORAGE_ACCOUNT", "golangrocksonazure")
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "YmFy")
	c, err := New("test", "test", "", "orc", nil)

	assert.NotNil(t, c)
	assert.NoError(t, err)

	b, err := block.Base()
	assert.Nil(t, err)
	assert.NotPanics(t, func() {
		err := c.Write([]byte("abc"), b)
		assert.NotNil(t, err)
	})
}

func TestMultiAccountWriter(t *testing.T) {
	os.Setenv("AZURE_TENANT_ID", "xyz")
	os.Setenv("AZURE_CLIENT_ID", "xyz")
	os.Setenv("AZURE_CLIENT_SECRET", "xyz")

	c, err := NewMultiAccountWriter(monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"), "", "orc", defaultBlobServiceURL, "container", "x", []string{"x-0"}, []uint{0}, 0, 0)

	assert.Nil(t, c)
	assert.True(t, strings.Contains(err.Error(), "azure: unable to get azure storage credential"))
}
