package encoder

import (
	"encoding/json"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Writer is to filter and encode row of events
type Writer struct {
	filter  string
	encoder string
}

// New will generate a new encoder writer
func New(filter string, encoder string) (*Writer, error) {
	return &Writer{
		filter:  filter,
		encoder: encoder,
	}, nil
}

// Encode will encode a row to the format the user specifies
func (w *Writer) Encode(row *block.Row) ([]byte, error) {

	// Default JSON
	// DOUBLE CHECK LATER to NOT do any copies when putting in a byte slice
	jsonString, err := json.Marshal(row.Values)
	if err != nil {
		return nil, errors.Internal("encoder: could not marshal to JSON", err)
	}
	return []byte(string(jsonString)), nil
}
