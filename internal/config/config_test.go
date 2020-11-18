package config_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/config/env"
	"github.com/kelindar/talaria/internal/config/static"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	const refreshTime = 50 * time.Millisecond
	const waitTime = 100 * time.Millisecond

	os.Setenv("TALARIA_STORAGE_DIR", "dir-1")
	cfg := config.Load(context.Background(), refreshTime, static.New(), env.New("TALARIA"))
	assert.Equal(t, cfg().Storage.Directory, "dir-1")

	os.Setenv("TALARIA_STORAGE_DIR", "dir-2")
	time.Sleep(waitTime)

	assert.Equal(t, cfg().Storage.Directory, "dir-2")
}
