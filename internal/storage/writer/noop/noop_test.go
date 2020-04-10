package noop

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoop(t *testing.T) {
	noopWriter := New()

	assert.NotPanics(t, func() {
		_ = noopWriter.Write(nil, nil)
	})
}
