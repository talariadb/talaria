package script

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func execCommand(ctx context.Context, cli string, params []string, dir string) error {
	cmd := exec.CommandContext(ctx, cli, params...)
	cmd.Dir = dir
	var stdout, stderr io.ReadCloser
	var err error

	if stdout, err = cmd.StdoutPipe(); err != nil {
		return err
	}
	defer stdout.Close()

	if stderr, err = cmd.StderrPipe(); err != nil {
		return err
	}
	defer stderr.Close()

	fmt.Printf("[execCommand] Start command: %s, params: %s\n", cli, params)
	if err = cmd.Start(); err != nil {
		return err
	}
	multireader := io.MultiReader(stdout, stderr)
	_, err = io.Copy(os.Stdout, multireader)
	if err != nil {
		fmt.Printf("[execCommand] copy stdout to multireader met err %v\n", err)
	}

	return cmd.Wait()
}

func compliePlugin() {
	os.Remove("./talaria_plugin.so")

	err := execCommand(context.Background(), "pwd", []string{"-P"}, "./plugin_test")
	if err != nil {
		panic(err)
	}

	err = execCommand(context.Background(), "go", []string{"version"}, "./plugin_test")
	if err != nil {
		panic(err)
	}

	params := []string{
		"build", "-buildmode=plugin",
		"-o", "../talaria_plugin.so",
	}
	err = execCommand(context.Background(), "go", params, "./plugin_test")
	if err != nil {
		panic(err)
	}
}

func TestLoadGoPlugin(t *testing.T) {
	compliePlugin()
	l := NewPluginLoader("ComputeRow")
	s, err := l.Load("file:///talaria_plugin.so")
	assert.NotNil(t, s)
	assert.NoError(t, err)
	require.NoError(t, err)
	out, err := s.Value(map[string]interface{}{
		"customKeyA": 12,
		"time":       12999,
		"uuid":       "uuidValue",
		"id":         "idValue",
		"customKeyB": "testCustomKeyB",
		"customKeyC": "testCustomKeyC",
	})
	require.NotNil(t, out)
	require.NoError(t, err)
	require.Equal(t, `{"customKeyA":12,"customKeyB":"testCustomKeyB","customKeyC":"testCustomKeyC"}`, out)

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
