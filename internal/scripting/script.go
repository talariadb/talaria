// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

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
	"github.com/kelindar/lua"
)

// Default empty script
const emptyScript = `function main(row) 
	return null
end`

// Loader represents a script loader
type Loader struct {
	modules []lua.Module   // The modules for the scripting environment
	loader  *loader.Loader // The loader to use to load and watch for code updates
}

// NewLoader creates a new loader that can be used to load scripts
func NewLoader(modules []lua.Module) *Loader {
	return &Loader{
		modules: modules,
		loader:  loader.New(),
	}
}

// Load creates a new script from code or URL and starts a watching if needed
func (l *Loader) Load(name string, uriOrCode string) (*lua.Script, error) {

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
