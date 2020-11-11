package noop

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/stretchr/testify/assert"
)

func TestNoop(t *testing.T) {
	noopWriter := New()

	assert.NotPanics(t, func() {
		_ = noopWriter.Write(nil, nil)
	})

	assert.NotPanics(t, func() {
		_ = noopWriter.Stream(block.Row{})
	})
}
