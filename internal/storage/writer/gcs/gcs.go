package gcs

import (
	"context"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Writer represents a writer for Google Cloud Storage.
type Writer struct {
	prefix  string
	client  *storage.BucketHandle
	context context.Context
}

// New creates a new writer.
func New(bucket, prefix string) (*Writer, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Internal("gcs: unable to create a client", err)
	}

	handle := client.Bucket(bucket)
	prefix = cleanPrefix(prefix)
	return &Writer{
		prefix:  prefix,
		client:  handle,
		context: ctx,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, val []byte) error {
	obj := w.client.Object(path.Join(w.prefix, string(key)))

	/// Write the payload
	writer := obj.NewWriter(w.context)
	_, err := writer.Write(val)
	if err != nil {
		return errors.Internal("gcs: unable to write", err)
	}

	return writer.Close()
}

func cleanPrefix(prefix string) string {
	return strings.Trim(prefix, "/")
}
