package pubsub

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func setup() *grpc.ClientConn {
	// Create mock server
	srv := pstest.NewServer()
	conn, _ := grpc.Dial(srv.Addr, grpc.WithInsecure())
	return conn
}

func setup2(grpcConn *grpc.ClientConn) {
	// Create topic
	ctx := context.Background()
	client, _ := pubsub.NewClient(ctx, "gcp-project", option.WithGRPCConn(grpcConn))
	client.CreateTopic(ctx, "talaria")
}

func TestNew(t *testing.T) {
	conn := setup()

	// Fail because topic doesn't exist
	c, err := New("gcp-project", "talaria", "", "", monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"), option.WithGRPCConn(conn))

	assert.Nil(t, c)
	assert.Error(t, err)

	setup2(conn)

	// Doesn't fail after creating topic
	c, err = New("gcp-project", "talaria", "", "", monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"), option.WithGRPCConn(conn))

	assert.NoError(t, err)
	assert.IsType(t, &Writer{}, c)
	assert.NotNil(t, c)
	assert.NotNil(t, c.Writer)

	row := block.Row{
		Values: map[string]interface{}{
			"test": "Hello Talaria",
			"age":  30,
		},
	}

	err = c.Stream(row)
	assert.NoError(t, err)

	//process() should display error message but continues on
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	task, _ := c.Run(ctx)

	task.Outcome()

	// Give some time for process to run in the background goroutine
	assert.Empty(t, c.buffer)

	// Close connection for failure
	c.Close()
	err = c.Stream(row)
	assert.NoError(t, err)

}

func TestFullBufferBlocking(t *testing.T) {
	conn := setup()
	setup2(conn)

	// Doesn't fail after creating topic
	c, err := New("gcp-project", "talaria", "", "", monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"), option.WithGRPCConn(conn))

	assert.IsType(t, &Writer{}, c)
	assert.NoError(t, err)

	c.buffer = make(chan []byte, 1)

	row := block.Row{
		Values: map[string]interface{}{
			"test": "Hello Talaria",
			"age":  30,
		},
	}

	go func(c *Writer) {
		err = c.Stream(row)
		assert.NoError(t, err)
		// Sleep for 5 seconds before invoking the function
		time.Sleep(time.Second * 5)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		task, _ := c.Run(ctx)

		task.Outcome()
	}(c)

	// Buffer 1 second to make sure the row went into buffer
	time.Sleep(time.Second * 1)

	// Buffer should be full at this point since the buffer is occupied by previous stream
	// for 10 seconds, the next stream should be blocked by previous stream
	select {
	// Try to insert new data into the buffer
	case c.buffer <- []byte("123"):
	default:
		// Buffer full
		assert.NotEmpty(t, c.buffer)
	}
	// Buffer should be full here, subsequent row should get blocked until previous row being processed after 5 seconds
	err = c.Stream(row)
	b := len(c.buffer)
	assert.NoError(t, err)
	assert.Equal(t, 1, b)
}

func TestWrite(t *testing.T) {
	conn := setup()
	setup2(conn)

	c, _ := New("gcp-project", "talaria", "", "", nil, option.WithGRPCConn(conn))
	res := c.Write(nil, nil)
	assert.Nil(t, res)
}
