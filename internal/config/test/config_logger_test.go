package test

import (
	"context"
	"testing"
	"time"

	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/config/s3"
	"github.com/stretchr/testify/assert"
)

type mockLogger struct {
	count int
}

func newMockLogger() *mockLogger {
	return new(mockLogger)
}

func (l *mockLogger) Errorf(f string, v ...interface{}) {
	l.count++
}

func (l *mockLogger) Warningf(f string, v ...interface{}) {
	l.count++
}

func (l *mockLogger) Infof(f string, v ...interface{}) {
	l.count++
}

func (l *mockLogger) Debugf(f string, v ...interface{}) {
	l.count++
}

type staticConf struct{}

func (st *staticConf) Configure(c *config.Config) error {
	c.URI = "s3://c.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json"
	c.Tables = config.Tables{
		Timeseries: &config.Timeseries{},
	}
	return nil
}

func TestLoggerUpdates(t *testing.T) {
	var down downloadMock = func(ctx context.Context, uri string) ([]byte, error) {
		return []byte("a: int64"), nil
	}

	stdoutLogger := newMockLogger()
	s3C := s3.NewWith(down, stdoutLogger)

	config.Load(context.Background(), 100*time.Millisecond, &staticConf{}, s3C)
	tableLogger := newMockLogger()
	s3C.SetLogger(tableLogger)

	time.Sleep(1 * time.Second)

	// First time should be called with the stdout logger. then subsequent 2(3 - 1) times should be called with the table logger.
	// total calls to the s3 configurer will be 3 because the load frequency is 1 second and sleep is 3 second
	assert.Greater(t, tableLogger.count, 5)
}

type downloadMock func(ctx context.Context, uri string) ([]byte, error)

func (d downloadMock) Load(ctx context.Context, uri string) ([]byte, error) {
	return d(ctx, uri)
}
