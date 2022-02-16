package computed

import (
	"github.com/kelindar/talaria/internal/encoding/typeof"
	script "github.com/kelindar/talaria/internal/scripting"
)

type loadComputed struct {
	name   string // Name of the column
	loader script.Handler
	typ    typeof.Type
}

func (l *loadComputed) Name() string {
	return l.name
}

func (l *loadComputed) Type() typeof.Type {
	return l.typ
}

func (l *loadComputed) Value(row map[string]interface{}) (interface{}, error) {
	return l.loader.Value(row)
}
