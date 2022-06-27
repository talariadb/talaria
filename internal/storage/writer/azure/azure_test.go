package azure

import (
	"bytes"
	"os"
	"strings"
	"testing"

	eorc "github.com/crphang/orc"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/orc"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
)

// Create a base block for testing purpose
func blockBase() ([]block.Block, error) {
	schema := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Int64,
		"col2": typeof.Float64,
	}
	orcSchema, _ := orc.SchemaFor(schema)

	orcBuffer1 := &bytes.Buffer{}
	writer, _ := eorc.NewWriter(orcBuffer1,
		eorc.SetSchema(orcSchema))
	_ = writer.Write("eventName", 1, 1.0)
	_ = writer.Close()

	apply := block.Transform(nil)

	block, err := block.FromOrcBy(orcBuffer1.Bytes(), "col0", nil, apply)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func TestWriter(t *testing.T) {
	os.Setenv("AZURE_STORAGE_ACCOUNT", "golangrocksonazure")
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "YmFy")
	c, err := New("test", "test", "", "orc", nil)

	assert.NotNil(t, c)
	assert.NoError(t, err)

	b, err := blockBase()
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
