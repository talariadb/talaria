package stream

// Streamer represents a stream
type Streamer interface {
	Stream(row *map[string]interface{}) error
}
