package multi

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/stretchr/testify/assert"
)

type MockWriter func(key key.Key, val []byte) error

func (w MockWriter) Write(key key.Key, val []byte) error {
	return w(key, val)
}

func TestMulti(t *testing.T) {
	var count int
	sub := MockWriter(func(key key.Key, val []byte) error {
		count++
		return nil
	})

	multiWriter := New(sub, sub, sub)
	assert.NoError(t, multiWriter.Write(nil, nil))
	assert.Equal(t, 3, count)
}
