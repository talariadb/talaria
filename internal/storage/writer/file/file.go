package file

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
	script "github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/writer/base"
)

// Writer represents a local file writer.
type Writer struct {
	*base.Writer
	directory string
}

// New creates a new writer.
func New(directory, filter, encoding string, loader *script.Loader) (*Writer, error) {
	dir, err := filepath.Abs(directory)
	if err != nil {
		return nil, errors.Internal("file: unable to create file writer", err)
	}
	baseWriter, err := base.New(filter, encoding, loader)
	if err != nil {
		return nil, errors.Newf("file: %v", err)
	}

	return &Writer{
		Writer:    baseWriter,
		directory: dir,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, blocks []block.Block) error {
	filename := path.Join(w.directory, string(key))
	dir := path.Dir(filename)
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(dir, 0777)
			if err != nil {
				return errors.Internal("file: unable to create directory", err)
			}
		} else {
			return errors.Internal("file: unable to write", err)
		}
	}

	buffer, err := w.Writer.Encode(blocks)
	if err != nil {
		return errors.Newf("file: %v", err)
	}
	if err := ioutil.WriteFile(filename, buffer, 0644); err != nil {
		return errors.Internal("file: unable to write", err)
	}
	return nil
}
