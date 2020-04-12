package writer

import (
	"fmt"
	"time"

	"github.com/grab/talaria/internal/column"
	"github.com/grab/talaria/internal/config"
	"github.com/grab/talaria/internal/encoding/typeof"
	"github.com/grab/talaria/internal/monitor"
	"github.com/grab/talaria/internal/monitor/errors"
	"github.com/grab/talaria/internal/scripting"
	"github.com/grab/talaria/internal/storage"
	"github.com/grab/talaria/internal/storage/compact"
	"github.com/grab/talaria/internal/storage/flush"
	"github.com/grab/talaria/internal/storage/writer/azure"
	"github.com/grab/talaria/internal/storage/writer/bigquery"
	"github.com/grab/talaria/internal/storage/writer/file"
	"github.com/grab/talaria/internal/storage/writer/gcs"
	"github.com/grab/talaria/internal/storage/writer/noop"
	"github.com/grab/talaria/internal/storage/writer/s3"
)

// New creates a compact store using the configuration provided
func New(config *config.Compaction, monitor monitor.Monitor, store storage.Storage, loader *script.Loader) *compact.Storage {
	writer, err := newWriter(config)
	if err != nil {
		monitor.Error(err)
	}

	nameFunc := func(row map[string]interface{}) (s string, e error) {
		return fmt.Sprintf("%s.orc", time.Now().UTC().Format("2006-01-02-15-04-05")), nil
	}

	// Configure the flush interval, default to 30s
	interval := 30 * time.Second
	if config.Interval > 0 {
		interval = time.Duration(config.Interval) * time.Second
	}

	// If name function was specified, use it
	if config.NameFunc != "" {
		if fn, err := column.NewComputed("nameFunc", typeof.String, config.NameFunc, loader); err == nil {
			nameFunc = func(row map[string]interface{}) (s string, e error) {
				val, err := fn.Value(row)
				return val.(string), err
			}
		}
	}

	flusher := flush.New(monitor, writer, nameFunc)
	return compact.New(store, flusher, flusher, monitor, interval)
}

// NewWriter creates a new writer from the configuration.
func newWriter(config *config.Compaction) (flush.Writer, error) {
	switch {
	case config.S3 != nil:
		return s3.New(config.S3.Bucket, config.S3.Prefix, config.S3.Region, config.S3.Endpoint, config.S3.SSE, config.S3.AccessKey, config.S3.SecretKey, config.S3.Concurrency)
	case config.Azure != nil:
		return azure.New(config.Azure.Container, config.Azure.Prefix)
	case config.GCS != nil:
		return gcs.New(config.GCS.Bucket, config.GCS.Prefix)
	case config.BigQuery != nil:
		return bigquery.New(config.BigQuery.Project, config.BigQuery.Dataset, config.BigQuery.Table)
	case config.File != nil:
		return file.New(config.File.Directory)
	default:
		return noop.New(), errors.New("compact: writer was not configured")
	}
}
