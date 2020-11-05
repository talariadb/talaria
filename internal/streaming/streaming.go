package streaming

import (
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/streaming/streams/pubsub"
)

// Streamer will stream data
type Streamer interface {
	Stream() error
}

// New creates an array of streams using the configuration provided
func New(configs []config.Streaming, monitor monitor.Monitor) Streamer {
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
func newStreamer(configs []config.Streaming) (Streamer, error) {

	for _, config := range configs {
		// Configure PubSub stream if present
		if config.PubSubStream != nil {
			w, err := pubsub.New(config.PubSubStream.Project, config.PubSubStream.Topic, config.PubSubStream.Filter, config.PubSubStream.Encoder)
			if err != nil {
				return nil, err
			}
			return w, nil
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
