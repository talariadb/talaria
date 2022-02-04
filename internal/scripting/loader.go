package script

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/grab/async"
	"github.com/kelindar/loader"
)

const (
	luaType    = "lua"
	pluginType = "plugin"
)

type Loader struct {
	loader *loader.Loader // The loader to use to load and watch for code updates
}

type Handler interface {
	Load(uriOrCode string) (Handler, error)
	String() string
	Value(map[string]interface{}) (interface{}, error)
	// Type() typeof.Type
}

type HandlerLoader struct {
	hs []Handler
}

func NewHandlerLoader(handlers ...Handler) *HandlerLoader {
	s := HandlerLoader{}
	for _, h := range handlers {
		s.hs = append(s.hs, h)
	}
	return &s
}

func parseHandler(uriOrCode string) string {
	if strings.HasSuffix(uriOrCode, ".so") {
		return pluginType
	}

	return luaType
}

func (l *HandlerLoader) LoadHandler(uriOrCode string) (Handler, error) {
	h := parseHandler(uriOrCode)
	for _, d := range l.hs {
		if d.String() == h {
			return d.Load(uriOrCode)
		}
	}

	return nil, nil
}

// Wtch starts watching for script updates
func (l *Loader) watch(uriOrCode string, onUpdate func(io.Reader) error) error {
	if _, err := url.Parse(uriOrCode); err != nil {
		return onUpdate(strings.NewReader(uriOrCode)) // Assume it's the actual lua code
	}

	// Start watching on the URL
	updates := l.loader.Watch(context.Background(), uriOrCode, 5*time.Minute)
	u := <-updates
	if u.Err != nil {
		return u.Err
	}

	// Read the updates asynchronously
	async.Invoke(context.Background(), func(ctx context.Context) (interface{}, error) {
		for u := range updates {
			if u.Err == nil {
				_ = onUpdate(bytes.NewReader(u.Data))
			}
		}
		return nil, nil
	})

	// Perform a first update
	return onUpdate(bytes.NewReader(u.Data))
}
