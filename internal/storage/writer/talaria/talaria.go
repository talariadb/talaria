package talaria

import (
	"context"
	"sync"
	"time"

	talaria "github.com/kelindar/talaria/client/golang"

	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// GetClient will create a Talaria client
func GetClient(endpoint string, dialTimeout time.Duration, circuitTimeout time.Duration, maxConcurrent int, errorThresholdPercent int, nonBlocking bool) (*talaria.Client, error) {

	var client *talaria.Client
	var err error

	if nonBlocking {
		client, err = talaria.Dial(endpoint, talaria.WithNetwork(dialTimeout), talaria.WithCircuit(circuitTimeout, maxConcurrent, errorThresholdPercent), talaria.WithNonBlock())
	} else {
		client, err = talaria.Dial(endpoint, talaria.WithNetwork(dialTimeout), talaria.WithCircuit(circuitTimeout, maxConcurrent, errorThresholdPercent))
	}

	if err != nil {
		return nil, err
	}
	return client, nil
}

// Writer to write to TalariaDB
type Writer struct {
	lock                  sync.Mutex
	endpoint              string
	dialTimeout           time.Duration
	circuitTimeout        time.Duration
	maxConcurrent         int
	errorPercentThreshold int
	nonBlocking           bool
	client                *talaria.Client
}

// New initializes a new Talaria writer.
func New(endpoint string, dialTimeout time.Duration, circuitTimeout time.Duration, maxConcurrent int, errorPercentThreshold int, nonBlocking bool) (*Writer, error) {

	dialTimeout = dialTimeout * time.Second
	circuitTimeout = circuitTimeout * time.Second

	client, err := GetClient(endpoint, dialTimeout, circuitTimeout, maxConcurrent, errorPercentThreshold, nonBlocking)
	if err != nil {
		return nil, errors.Internal("talaria: unable to create a client", err)
	}

	return &Writer{
		client:                client,
		endpoint:              endpoint,
		dialTimeout:           dialTimeout,
		circuitTimeout:        circuitTimeout,
		maxConcurrent:         maxConcurrent,
		errorPercentThreshold: errorPercentThreshold,
		nonBlocking:           nonBlocking,
	}, nil
}

// Write will write the ORC data to Talaria
func (w *Writer) Write(key key.Key, val []byte) error {
	if err := w.tryConnect(); err != nil {
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
		client, err := GetClient(w.endpoint, w.dialTimeout, w.circuitTimeout, w.maxConcurrent, w.errorPercentThreshold, w.nonBlocking)
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
