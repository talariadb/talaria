package gcs

import (
	"context"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/writer/base"
)

// Writer represents a writer for Google Cloud Storage.
type Writer struct {
	*base.Writer
	prefix  string
	client  *storage.BucketHandle
	context context.Context
}

// New creates a new writer.
func New(bucket, prefix, filter, encoding string, loader *script.Loader) (*Writer, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Internal("gcs: unable to create a client", err)
	}

	baseWriter, err := base.New(filter, encoding, loader)
	if err != nil {
		return nil, errors.Internal("gcs: unable to create a base Writer", err)
	}
	handle := client.Bucket(bucket)
	prefix = cleanPrefix(prefix)
	return &Writer{
		Writer:  baseWriter,
		prefix:  prefix,
		client:  handle,
		context: ctx,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, blocks []block.Block) error {
	obj := w.client.Object(path.Join(w.prefix, string(key)))

	buffer, err := w.Writer.Encode(blocks)
	if err != nil {
		return errors.Internal("gcs: unable to encode blocks", err)
	}
	/// Write the payload
	writer := obj.NewWriter(w.context)
	_, err = writer.Write(buffer)
	if err != nil {
		return errors.Internal("gcs: unable to write", err)
	}

	return writer.Close()
}

func cleanPrefix(prefix string) string {
	return strings.Trim(prefix, "/")
}
