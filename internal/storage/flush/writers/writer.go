package writers

import "github.com/grab/talaria/internal/encoding/key"

type Writer interface {
	Write(key key.Key, value []byte) error
}