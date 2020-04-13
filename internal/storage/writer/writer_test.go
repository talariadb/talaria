package writer

import (
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	cfg := &config.Compaction{}
	compact := New(cfg,
		monitor.NewNoop(),
		disk.New(monitor.NewNoop()),
		script.NewLoader(nil),
	)

	assert.NotNil(t, compact)
}
