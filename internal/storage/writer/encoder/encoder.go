package encoder

import (
	"encoding/json"

	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Writer is to filter and encode row of events
type Writer struct {
	filter  string
	encoder string
	marshal func(interface{}) ([]byte, error)
}

// New will generate a new encoder writer
func New(filter string, encoder string) (*Writer, error) {

	var marshal func(interface{}) ([]byte, error)

	// Extensible to change marshalling function
	switch encoder {
	case "json":
		marshal = json.Marshal
	default:
		marshal = json.Marshal
	}

	return &Writer{
		filter:  filter,
		encoder: encoder,
		marshal: marshal,
	}, nil
}

// Encode will encode a row to the format the user specifies
func (w *Writer) Encode(row *map[string]interface{}) ([]byte, error) {

	// Double check later to NOT do any copies when putting in a byte slice
	dataString, err := w.marshal(*row)
	if err != nil {
		errorMsg := "encoder: could not marshal to " + w.encoder + " "
		return nil, errors.Internal(errorMsg, err)
	}
	return dataString, nil
}
