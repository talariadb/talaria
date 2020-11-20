package writer

import (
	"context"
	"fmt"
	"hash/maphash"
	"sort"
	"time"

	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage"
	"github.com/kelindar/talaria/internal/storage/compact"
	"github.com/kelindar/talaria/internal/storage/flush"
	"github.com/kelindar/talaria/internal/storage/writer/azure"
	"github.com/kelindar/talaria/internal/storage/writer/bigquery"
	"github.com/kelindar/talaria/internal/storage/writer/file"
	"github.com/kelindar/talaria/internal/storage/writer/gcs"
	"github.com/kelindar/talaria/internal/storage/writer/multi"
	"github.com/kelindar/talaria/internal/storage/writer/noop"
	"github.com/kelindar/talaria/internal/storage/writer/pubsub"
	"github.com/kelindar/talaria/internal/storage/writer/s3"
	"github.com/kelindar/talaria/internal/storage/writer/talaria"
)

var seed = maphash.MakeSeed()

// ForStreaming creates a streaming writer
func ForStreaming(config config.Streams, monitor monitor.Monitor, loader *script.Loader) (storage.Streamer, error) {
	writer, err := newStreamer(config, monitor, loader)
	if err != nil {
		monitor.Error(err)
	}

	return writer.(storage.Streamer), nil
}

// ForCompaction creates a compaction writer
func ForCompaction(config *config.Compaction, monitor monitor.Monitor, store storage.Storage, loader *script.Loader) *compact.Storage {
	writer, err := newWriter(config.Sinks, loader)
	if err != nil {
		monitor.Error(err)
	}

	// Configure the flush interval, default to 30s
	interval := 30 * time.Second
	if config.Interval > 0 {
		interval = time.Duration(config.Interval) * time.Second
	}

	// If name function was specified, use it
	nameFunc := defaultNameFunc
	if config.NameFunc != "" {
		if fn, err := column.NewComputed("nameFunc", typeof.String, config.NameFunc, loader); err == nil {
			nameFunc = func(row map[string]interface{}) (s string, e error) {
				val, err := fn.Value(row)
				if err != nil {
					monitor.Error(err)
					return "", err
				}

				return val.(string), err
			}
		}
	}

	monitor.Info("server: setting up compaction %T to run every %.0fs...", writer, interval.Seconds())
	flusher := flush.New(monitor, writer, nameFunc)
	return compact.New(store, flusher, flusher, monitor, interval)
}

// NewWriter creates a new writer from the configuration.
func newWriter(config config.Sinks, loader *script.Loader) (flush.Writer, error) {
	var writers []multi.SubWriter

	// Configure S3 writer if present
	if config.S3 != nil {
		w, err := s3.New(config.S3.Bucket, config.S3.Prefix, config.S3.Region, config.S3.Endpoint, config.S3.SSE, config.S3.AccessKey, config.S3.SecretKey, config.S3.Concurrency)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	// Configure Azure writer if present
	if config.Azure != nil {
		w, err := azure.New(config.Azure.Container, config.Azure.Prefix)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	// Configure GCS writer if present
	if config.GCS != nil {
		w, err := gcs.New(config.GCS.Bucket, config.GCS.Prefix)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	// Configure BigQuery writer if present
	if config.BigQuery != nil {
		w, err := bigquery.New(config.BigQuery.Project, config.BigQuery.Dataset, config.BigQuery.Table)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	// Configure File writer if present
	if config.File != nil {
		w, err := file.New(config.File.Directory)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	// Configure Talaria writer if present
	if config.Talaria != nil {
		w, err := talaria.New(config.Talaria.Endpoint, config.Talaria.CircuitTimeout, config.Talaria.MaxConcurrent, config.Talaria.ErrorPercentThreshold)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	// Configure Google Pub/Sub writer if present
	if config.PubSub != nil {
		w, err := pubsub.New(config.PubSub.Project, config.PubSub.Topic, config.PubSub.Encoder, config.PubSub.Filter, loader, nil)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}

	// If no writers were configured, error out
	if len(writers) == 0 {
		return noop.New(), errors.New("compact: writer was not configured")
	}

	// Setup a multi-writer for all configured writers
	return multi.New(writers...), nil
}

// newStreamer creates a new streamer from the configuration.
func newStreamer(config config.Streams, monitor monitor.Monitor, loader *script.Loader) (flush.Writer, error) {
	var writers []multi.SubWriter

	// If no streams were configured, error out
	if len(config) == 0 {
		return noop.New(), errors.New("stream: writer was not configured")
	}

	for _, v := range config {
		w, err := newWriter(v, loader)
		if err != nil {
			return noop.New(), err
		}
		writers = append(writers, w)
	}

	// Setup a multi-writer for all configured writers
	multiWriters := multi.New(writers...)
	_, err := multiWriters.Run(context.Background())
	return multiWriters, err
}

// defaultNameFunc represents a default name function
func defaultNameFunc(row map[string]interface{}) (s string, e error) {
	return fmt.Sprintf("%s-%x.orc",
		time.Now().UTC().Format("year=2006/month=1/day=2/15-04-05"),
		hashOfRow(row),
	), nil
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
