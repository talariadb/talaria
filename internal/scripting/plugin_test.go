package script

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadGoPlugin(t *testing.T) {
	l := NewPluginLoader("Computed")
	s, err := l.LoadGoPlugin("data", "file:///talaria_go_function.so")
	assert.NotNil(t, s)
	assert.NoError(t, err)
	require.NoError(t, err)
	out, err := s.Value(map[string]interface{}{
		"customKeyA": 12, "time": 12999, "uuid": "uuidValue", "id": "idValue", "customKeyB": "testCustomKeyB",
	})
	require.NotNil(t, out)
	require.NoError(t, err)
	require.Equal(t, `{"customKeyA":12,"customKeyB":"testCustomKeyB"}`, out)

}

// func TestLoadS3GoPlugin(t *testing.T) {
// 	l := NewPluginLoader()
// 	s, err := l.LoadGoPlugin("data", "s3://mydata/talaria_go_function.so")
// 	require.NotNil(t, s)
// 	require.NoError(t, err)
// 	out, err := s.Value(map[string]interface{}{
// 		"customKeyA": 12, "time": 12999, "uuid": "uuidValue", "id": "idValue", "customKeyB": "testCustomKeyB",
// 	})
// 	require.NotNil(t, out)
// 	require.NoError(t, err)
// 	require.Equal(t, `{"customKeyA":12,"customKeyB":"testCustomKeyB"}`, out)
// }
