package column

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"plugin"
	"strings"
	"time"

	"github.com/grab/async"
	loaderpkg "github.com/kelindar/loader"
	"github.com/kelindar/lua"
)

// NewLuaLoader creates a new loader that can be used to load scripts
func NewLuaHandler(luaModules []lua.Module) *LuaHandler {
	return &LuaHandler{
		modules: luaModules,
		Loader:  Loader{loaderpkg.New()},
	}
}

func (l *LuaHandler) String() string { return "lua" }

// Load creates a new script from code or URL and starts a watching if needed
func (l *LuaHandler) Load(name string, uriOrCode string) (Computed, error) {
	log.Println("LoadLua: ", name, uriOrCode)

	// Default empty script
	const emptyScript = `function main(row)
		return null
	end`

	// Create an empty script, we'll update it right away
	s, err := lua.FromString(name, emptyScript, l.modules...)
	if err != nil {
		return nil, err
	}

	// If the string is actually a URL, try to download it
	if err := l.watch(uriOrCode, s.Update); err != nil {
		return nil, err
	}

	return s, nil
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

func (h *PluginHandler) updateGoPlugin(r io.Reader) error {
	tmpFileName := fmt.Sprintf("%s.so", time.Now().Format("20060102150405"))
	tmpFile, err := ioutil.TempFile("", tmpFileName)
	if err != nil {
		return err
	}
	_, err = io.Copy(tmpFile, r)
	if err != nil {
		return err
	}
	log.Printf("updateGoPlugin: write to file %s, try to open %s: ", tmpFileName, tmpFile.Name())
	p, err := plugin.Open(tmpFile.Name())
	if err != nil {
		return err
	}

	f, err := p.Lookup(h.functionName)
	if err != nil {
		return err
	}

	ok := false
	h.main, ok = f.(mainFunc)
	if !ok {
		return errors.New("type assertions on plugin funtion failed")
	}
	return nil
}
