package talaria

import (
	"context"
	"sync"
	"time"

	talaria "github.com/kelindar/talaria/client/golang"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/base"
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
	*base.Writer
	lock     sync.Mutex
	endpoint string
	client   *talaria.Client
	options  []talaria.Option
}

// New initializes a new Talaria writer.
func New(endpoint, filter, encoding string, monitor monitor.Monitor, circuitTimeout *time.Duration, maxConcurrent, errorPercentThreshold, maxMsgSendSize, maxMsgRecvSize *int) (*Writer, error) {

	var newTimeout = 5 * time.Second
	var newMaxConcurrent = hystrix.DefaultMaxConcurrent
	var newErrorPercentThreshold = hystrix.DefaultErrorPercentThreshold
	var newMaxMsgRecvSize = 32 * 1024 * 1024 // 32MB
	var newMaxMsgSendSize = 32 * 1024 * 1024 // 32MB
	var loadBalancingStrategy = "round_robin"

	baseWriter, err := base.New(filter, encoding, monitor)
	if err != nil {
		return nil, errors.Internal("talaria: ", err)
	}
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

	if maxMsgRecvSize != nil {
		newMaxMsgRecvSize = *maxMsgRecvSize
	}

	if maxMsgSendSize != nil {
		newMaxMsgSendSize = *maxMsgSendSize
	}

	dialOptions := []talaria.Option{}
	dialOptions = append(dialOptions, talaria.WithCircuit(newTimeout, newMaxConcurrent, newErrorPercentThreshold))
	dialOptions = append(dialOptions, talaria.WithMaxMsgSize(newMaxMsgSendSize, newMaxMsgRecvSize))
	dialOptions = append(dialOptions, talaria.WithLoadBalancer(loadBalancingStrategy))

	client, err := getClient(endpoint, dialOptions...)

	// Return writer with nil client
	if err != nil {
		return nil, errors.Internal("talaria: unable to create a client", err)
	}

	return &Writer{
		Writer:   baseWriter,
		client:   client,
		endpoint: endpoint,
		options:  dialOptions,
	}, nil
}

// Write will write the ORC data to Talaria
func (w *Writer) Write(key key.Key, blocks []block.Block) error {

	// Check if client is nil
	if w.client == nil {
		if err := w.tryConnect(); err != nil {
			return errors.Internal("talaria: client is nil, unable to connect", err)
		}
	}

	buffer, err := w.Writer.Encode(blocks)
	if err != nil {
		return errors.Internal("talaria: encoding err", err)
	}

	// Check error status if it needs to redial
	if err = w.client.IngestORC(context.Background(), buffer); err != nil {
		errStatus, _ := status.FromError(err)
		if codes.Unavailable == errStatus.Code() {
			// Server unavailable, redial
			if err := w.tryConnect(); err != nil {
				return errors.Internal("talaria: unable to redial", err)
			}
			// Send again after redial
			if err := w.client.IngestORC(context.Background(), buffer); err != nil {
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
