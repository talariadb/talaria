package writer

import (
	"testing"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/scripting"
	"github.com/grab/talaria/internal/storage/disk"
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
