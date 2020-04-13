package file

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Writer represents a local file writer.
type Writer struct {
	directory string
}

// New creates a new writer.
func New(directory string) (*Writer, error) {
	dir, err := filepath.Abs(directory)
	if err != nil {
		return nil, errors.Internal("file: unable to create file writer", err)
	}

	return &Writer{
		directory: dir,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, val []byte) error {
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

	if err := ioutil.WriteFile(filename, val, 0644); err != nil {
		return errors.Internal("file: unable to write", err)
	}
	return nil
}
