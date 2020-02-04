package writers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoop(t *testing.T) {
	noopWriter := NewNoop()

	assert.NotPanics(t, func() {
		_ = noopWriter.Write(nil, nil)
	})
}