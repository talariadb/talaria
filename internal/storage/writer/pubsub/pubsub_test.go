package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func TestNew(t *testing.T) {
	conn, err := grpc.Dial("", grpc.WithInsecure())
	assert.NoError(t, err)

	c, err := New("gcp-project", "talaria", "", "", nil, nil, option.WithGRPCConn(conn))

	assert.IsType(t, &Writer{}, c)
	assert.NoError(t, err)

	// TODO: Test streaming function
}
