package talaria

import (
	"context"
	"sync"
	"time"

	talaria "github.com/kelindar/talaria/client/golang"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetClient will create a Talaria client
func GetClient(endpoint string, dialTimeout time.Duration, circuitTimeout time.Duration, maxConcurrent int, errorThresholdPercent int) (*talaria.Client, error) {

	var client *talaria.Client
	var err error

	client, err = talaria.Dial(endpoint, talaria.WithNetwork(dialTimeout), talaria.WithCircuit(circuitTimeout, maxConcurrent, errorThresholdPercent))

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
	client                *talaria.Client
}

// New initializes a new Talaria writer.
func New(endpoint string, dialTimeout time.Duration, circuitTimeout time.Duration, maxConcurrent int, errorPercentThreshold int) (*Writer, error) {

	dialTimeout = dialTimeout * time.Second
	circuitTimeout = circuitTimeout * time.Second
	client, err := GetClient(endpoint, dialTimeout, circuitTimeout, maxConcurrent, errorPercentThreshold)
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
	}, nil
}

// Write will write the ORC data to Talaria
func (w *Writer) Write(key key.Key, val []byte) error {

	// Just in case client is nil, should not happen
	if w.client == nil {
		if err := w.tryConnect(); err != nil {
			return errors.Internal("talaria: client is nil, unable to connect", err)
		}
	}

	// Check error status if it needs to redial
	if err := w.client.IngestORC(context.Background(), val); err != nil {
		errStatus, _ := status.FromError(err)
		if codes.Unavailable == errStatus.Code() {

			// Close current connection if possible, else just create new client
			_ = w.client.Close()

			// Server unavailable, redial
			if err := w.tryConnect(); err != nil {
				return errors.Internal("talaria: unable to redial", err)
			}

			// Send again after redial
			if err := w.client.IngestORC(context.Background(), val); err != nil {
				return errors.Internal("talaria: unable to write after redial", err)
			}
		}
		return errors.Internal("talaria: unable to write", err)
	}
	return nil
}

// tryConnect will reconnect to Talaria if needed
func (w *Writer) tryConnect() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	client, err := GetClient(w.endpoint, w.dialTimeout, w.circuitTimeout, w.maxConcurrent, w.errorPercentThreshold)
	if err != nil {
		return err
	}
	w.client = client
	return nil
}

// Close closes the writer.
func (w *Writer) Close() error {
	return w.client.Close()
}
