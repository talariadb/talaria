package streaming

import (
	"fmt"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/stream"
	"github.com/kelindar/talaria/internal/storage/writer/noop"
	"github.com/kelindar/talaria/internal/storage/writer/pubsub"
)

// Streamer will stream data
type Streamer interface {
	Stream(*map[string]interface{}) error
}

// New creates an array of streams using the configuration provided
func New(configs []config.Streaming, monitor monitor.Monitor) stream.Streamer {
	fmt.Println(configs)
	writer, err := newStreamer(configs)
	if err != nil {
		monitor.Error(err)
	}

	// If name function was specified, use it
	// nameFunc := defaultNameFunc
	// if config.NameFunc != "" {
	// 	if fn, err := column.NewComputed("nameFunc", typeof.String, config.NameFunc, loader); err == nil {
	// 		nameFunc = func(row map[string]interface{}) (s string, e error) {
	// 			val, err := fn.Value(row)
	// 			if err != nil {
	// 				monitor.Error(err)
	// 				return "", err
	// 			}

	// 			return val.(string), err
	// 		}
	// 	}
	// }

	// monitor.Info("setting up compaction writer %T to run every %.0fs...", writer, interval.Seconds())
	// flusher := flush.New(monitor, writer, nameFunc)
	// return compact.New(store, flusher, flusher, monitor, interval)
	return writer
}

// newStreamer creates a new streamer from the configuration.
// TODO: Return list of streams
func newStreamer(configs []config.Streaming) (stream.Streamer, error) {

	var streamer stream.Streamer

	fmt.Println(configs)

	if len(configs) == 0 {
		streamer = noop.New()
		return streamer, errors.New("streaming: no stream was configured")
	}

	// Only allow one stream per array element
	// Stream priority is enforced in the appearance order in if-else statements
	for _, config := range configs {
		if config.PubSubStream != nil {
			streamer, err := pubsub.New(config.PubSubStream.Project, config.PubSubStream.Topic, config.PubSubStream.Filter, config.PubSubStream.Encoder)
			if err != nil {
				return nil, err
			}
			return streamer, nil
		} else if config.RedisStream != nil {
			return nil, nil
		} else {
			return nil, errors.New("streaming: unable to set up stream")
		}
	}

	return nil, nil

	// If no writers were configured, error out
	// if len(writers) == 0 {
	// 	return noop.New(), errors.New("compact: writer was not configured")
	// }

	// Setup a multi-writer for all configured writers
	// return multi.New(writers...), nil
}
