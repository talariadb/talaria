package s3compact

import (
	"testing"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/storage/disk"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	cfg := &config.S3Compact{}
	s3Compact := New(cfg, monitor.NewNoop(), disk.New(monitor.NewNoop()))
	assert.NotNil(t, s3Compact)
}
