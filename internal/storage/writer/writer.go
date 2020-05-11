package writer

import (
	"fmt"
	"hash/maphash"
	"sort"
	"time"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage"
	"github.com/kelindar/talaria/internal/storage/compact"
	"github.com/kelindar/talaria/internal/storage/flush"
	"github.com/kelindar/talaria/internal/storage/writer/azure"
	"github.com/kelindar/talaria/internal/storage/writer/bigquery"
	"github.com/kelindar/talaria/internal/storage/writer/file"
	"github.com/kelindar/talaria/internal/storage/writer/gcs"
	"github.com/kelindar/talaria/internal/storage/writer/noop"
	"github.com/kelindar/talaria/internal/storage/writer/s3"
)

var seed = maphash.MakeSeed()

// New creates a compact store using the configuration provided
func New(config *config.Compaction, monitor monitor.Monitor, store storage.Storage, loader *script.Loader) *compact.Storage {
	writer, err := newWriter(config)
	if err != nil {
		monitor.Error(err)
	}

	nameFunc := func(row map[string]interface{}) (s string, e error) {
		return fmt.Sprintf("%s-%x.orc",
			time.Now().UTC().Format("year=2006/month=1/day=2/15-04-05"),
			hashOfRow(row),
		), nil
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

	monitor.Info("setting up compaction writer %T to run every %.0fs...", writer, interval.Seconds())
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

// hashOfRow computes a hash of the row, for the default filename
func hashOfRow(row map[string]interface{}) uint64 {

	// Sort the map keys
	str := make([]string, 0, len(row))
	for k, v := range row {
		str = append(str, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(str)

	// Compute the hash
	var hash maphash.Hash
	hash.SetSeed(seed)
	for _, v := range str {
		hash.WriteString(v)
	}
	return hash.Sum64()
}
