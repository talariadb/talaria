package talaria

import (
	"context"
	"sync"
	"time"

	talaria "github.com/kelindar/talaria/client/golang"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/myteksi/hystrix-go/hystrix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getClient will create a Talaria client
func getClient(endpoint string, options ...talaria.Option) (*talaria.Client, error) {
	client, err := talaria.Dial(endpoint, options...)

	if err != nil {
		return nil, err
	}
	return client, nil
}

// Writer to write to TalariaDB
type Writer struct {
	lock     sync.Mutex
	endpoint string
	client   *talaria.Client
	options  []talaria.Option
}

// New initializes a new Talaria writer.
func New(endpoint string, circuitTimeout *time.Duration, maxConcurrent *int, errorPercentThreshold *int) (*Writer, error) {

	var newTimeout = 5 * time.Second
	var newMaxConcurrent = hystrix.DefaultMaxConcurrent
	var newErrorPercentThreshold = hystrix.DefaultErrorPercentThreshold

	// Set defaults for variables if there aren't any
	if circuitTimeout != nil {
		newTimeout = *circuitTimeout * time.Second
	}

	if maxConcurrent != nil {
		newMaxConcurrent = *maxConcurrent
	}

	if errorPercentThreshold != nil {
		newErrorPercentThreshold = *errorPercentThreshold
	}

	dialOptions := []talaria.Option{}
	dialOptions = append(dialOptions, talaria.WithCircuit(newTimeout, newMaxConcurrent, newErrorPercentThreshold))

	client, err := getClient(endpoint, dialOptions...)

	// Return writer with nil client
	if err != nil {
		return nil, errors.Internal("talaria: unable to create a client", err)
	}

	return &Writer{
		client:   client,
		endpoint: endpoint,
		options:  dialOptions,
	}, nil
}

// Write will write the ORC data to Talaria
func (w *Writer) Write(key key.Key, val []byte) error {

	// Check if client is nil
	if w.client == nil {
		if err := w.tryConnect(); err != nil {
			return errors.Internal("talaria: client is nil, unable to connect", err)
		}
	}

	// Check error status if it needs to redial
	if err := w.client.IngestORC(context.Background(), val); err != nil {
		errStatus, _ := status.FromError(err)
		if codes.Unavailable == errStatus.Code() {
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

	client, err := getClient(w.endpoint, w.options...)
	if err != nil {
		return err
	}
	w.client = client
	return nil
}
