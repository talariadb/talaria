// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package orc

import (
	"bytes"
	"errors"
	"io"

	"github.com/crphang/orc"
	"github.com/grab/talaria/internal/encoding/typeof"
)

var errNoWriter = errors.New("unable to create an orc writer")

// Iterator represents orc data frame.
type Iterator interface {
	io.Closer
	Range(f func(int, []interface{}) bool, columns ...string) (int, bool)
	Schema() typeof.Schema
}

// FromFile creates an iterator from a file.
func FromFile(filename string) (Iterator, error) {
	r, err := orc.Open(filename)
	if err != nil {
		return nil, err
	}

	return &iterator{reader: r}, nil
}

// FromBuffer creates an iterator from a buffer.
func FromBuffer(b []byte) (Iterator, error) {
	r, err := orc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	return &iterator{reader: r}, nil
}

// Range is a helper function that ranges over a set of columns in an orc buffer
func Range(payload []byte, f func(int, []interface{}) bool, columns ...string) error {
	i, err := FromBuffer(payload)
	if err != nil {
		return err
	}

	_, _ = i.Range(f, columns...)
	return nil
}

// First selects a first row only, then stops.
func First(payload []byte, columns ...string) (result []interface{}, err error) {
	err = Range(payload, func(_ int, v []interface{}) bool {
		result = v
		return true // No need to iterate further, we just take 1st element
	}, columns...)
	return
}

// Iterator represents orc data frame.
type iterator struct {
	reader *orc.Reader
}

// Range iterates through the reader.
func (i *iterator) Range(f func(int, []interface{}) bool, columns ...string) (index int, stop bool) {
	c := i.reader.Select(columns...)
	for c.Stripes() {
		for c.Next() {
			index++
			if stop = f(index-1, c.Row()); stop {
				return index, false
			}
		}
	}
	return index, true
}

// Schema gets the SQL schema for the iterator.
func (i *iterator) Schema() typeof.Schema {
	schema := i.reader.Schema()
	result := make(typeof.Schema, len(schema.Columns()))
	for _, c := range schema.Columns() {
		if t, err := schema.GetField(c); err == nil {
			if t, supported := typeof.FromOrc(t); supported {
				result[c] = t
			}
		}
	}
	return result
}

// Close closes the iterator.
func (i *iterator) Close() error {
	return i.reader.Close()
}
