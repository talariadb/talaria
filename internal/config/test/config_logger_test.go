package test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/stretchr/testify/assert"
)

type mockLogger struct {
	count int64
}

func newMockLogger() *mockLogger {
	return new(mockLogger)
}

func (l *mockLogger) Errorf(f string, v ...interface{}) {
	atomic.AddInt64(&l.count, 1)
}

func (l *mockLogger) Warningf(f string, v ...interface{}) {
	atomic.AddInt64(&l.count, 1)
}

func (l *mockLogger) Infof(f string, v ...interface{}) {
	atomic.AddInt64(&l.count, 1)
}

func (l *mockLogger) Debugf(f string, v ...interface{}) {
	atomic.AddInt64(&l.count, 1)
}

type staticConf struct{}

func (st *staticConf) Configure(c *config.Config) error {
	c.URI = "s3://c.s3-ap-southeast-1.amazonaws.com/a/b/c/conf-server-conf-stg.json"
	c.Tables = config.Tables{
		Timeseries: &config.Timeseries{},
	}
	return nil
}

func logHelper(logger logging.Logger) {
	logger.Warningf("First Log")
	logger.Warningf("Second Log")
	logger.Warningf("Third Log")
}


func TestLoggerUpdates(t *testing.T) {
	tableLogger := newMockLogger()
	logHelper(tableLogger)

	assert.Equal(t, atomic.LoadInt64(&tableLogger.count), int64(3))
}

type downloadMock func(ctx context.Context, uri string) ([]byte, error)

func (d downloadMock) Load(ctx context.Context, uri string) ([]byte, error) {
	return d(ctx, uri)
}
