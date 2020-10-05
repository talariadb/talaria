package talaria

import (
	"context"
	"sync"

	talaria "github.com/kelindar/talaria/client/golang"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Writer to write to TalariaDB
type Writer struct {
	lock     sync.Mutex
	endpoint string
	client   *talaria.Client
}

// New initializes a new Talaria writer.
func New(endpoint string) (*Writer, error) {
	talaria, err := talaria.Dial(endpoint)
	if err != nil {
		return nil, errors.Internal("talaria: unable to connect", err)
	}

	return &Writer{
		client:   talaria,
		endpoint: endpoint,
	}, nil
}

// Write will write the ORC data to Talaria
func (w *Writer) Write(key key.Key, val []byte) error {
	err := w.tryConnect()
	if err != nil {
		return errors.Internal("talaria: unable to connect", err)
	}

	if err := w.client.IngestORC(context.Background(), val); err != nil {
		return errors.Internal("talaria: unable to write", err)
	}
	return nil
}

// tryConnect will reconnect to Talaria if needed
func (w *Writer) tryConnect() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.client == nil {
		client, err := talaria.Dial(w.endpoint)
		if err != nil {
			return err
		}
		w.client = client
	}
	return nil
}

// Close closes the writer.
func (w *Writer) Close() error {
	return w.client.Close()
}
