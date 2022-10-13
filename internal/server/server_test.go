package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/config/env"
	"github.com/kelindar/talaria/internal/config/static"
	"github.com/kelindar/talaria/internal/table"
	"github.com/stretchr/testify/assert"
)

// Test for creating new Server object.
func TestNew(t *testing.T) {
	const refreshTime = 50 * time.Millisecond
	const waitTime = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	os.Setenv("TALARIA_ENV", "file:///internal/config/sample_config.yaml")
	cfg := config.Load(ctx, refreshTime, static.New(), env.New("TALARIA"))
	defer cancel()
	s := New(cfg, nil, nil, []table.Table{}...)
	time.Sleep(waitTime)
	// check if the server struct is not nil
	if assert.NotNil(t, s) {
		// check if the server struct has a valid grpc server
		assert.NotNil(t, s.server)
	}
}
