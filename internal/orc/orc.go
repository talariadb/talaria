// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package orc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/scritchley/orc"
)

var errNoWriter = errors.New("unable to create an orc writer")

// Iterator represents orc data frame.
type Iterator interface {
	io.Closer
	Range(f func(int, []interface{}) bool, columns ...string) (int, bool)
	Schema() map[string]reflect.Type
	SplitBySize(max int, f func([]byte) bool) (err error)
	SplitByColumn(column string, f func(string, []byte) bool) (err error)
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

// SplitBySize is a helper function that splits the incoming buffer in a set of smaller
// and uncompressed orc files.
func SplitBySize(payload []byte, max int, f func([]byte) bool) (map[string]reflect.Type, error) {
	i, err := FromBuffer(payload)
	if err != nil {
		return nil, err
	}

	return i.Schema(), i.SplitBySize(max, f)
}

// SplitByColumn is a helper function that splits the incoming buffer in a set of smaller
// and uncompressed orc files by column value.
func SplitByColumn(payload []byte, column string, f func(string, []byte) bool) (map[string]reflect.Type, error) {
	i, err := FromBuffer(payload)
	if err != nil {
		return nil, err
	}

	return i.Schema(), i.SplitByColumn(column, f)
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

// SplitBySize creates and iterates over a set of smaller orc files by size
func (i *iterator) SplitBySize(max int, f func([]byte) bool) (err error) {
	var count int           // The current row
	var chunk *bytes.Buffer // The pointer to the current chunk
	var writer *orc.Writer  // The current writer

	types := []orc.TypeDescriptionTransformFunc{
		orc.SetCategory(orc.CategoryStruct),
	}

	var cols []string
	for _, field := range i.fields() {
		types = append(types, orc.AddField(field.name, orc.SetCategory(field.category)))
		cols = append(cols, field.name)
	}

	newSchema, errSchema := orc.NewTypeDescription(types...)
	if errSchema != nil {
		return errSchema
	}

	// We range over each row in the file and re-write it to smaller chunks
	i.Range(func(_ int, v []interface{}) bool {
		if count%max == 0 {
			// If we've been asked to stop, stop here
			if writer != nil && flushTo(writer, chunk, f) {
				return true
			}

			if writer, chunk = newWriter(newSchema); writer == nil {
				err = errNoWriter
				return true
			}
		}

		count++
		err = writer.Write(v...)
		return err != nil
	}, cols...)
	// Last write
	_ = flushTo(writer, chunk, f)
	return
}

// SplitByColumn creates and iterates over a set of smaller orc files based on column
func (i *iterator) SplitByColumn(column string, f func(string, []byte) bool) (err error) {
	var count int           // The current row
	var chunk *bytes.Buffer // The pointer to the current chunk
	var writer *orc.Writer  // The current writer

	types := []orc.TypeDescriptionTransformFunc{
		orc.SetCategory(orc.CategoryStruct),
	}

	var cols []string
	var colIdx int
	for idx, field := range i.fields() {
		types = append(types, orc.AddField(field.name, orc.SetCategory(field.category)))
		cols = append(cols, field.name)
		if field.name == column {
			colIdx = idx
		}
	}

	newSchema, errSchema := orc.NewTypeDescription(types...)
	if errSchema != nil {
		return errSchema
	}

	// We range over each row in the file and re-write it to smaller chunks
	prevValue := ""
	i.Range(func(_ int, v []interface{}) bool {
		thisValue, ok := convertToString(v[colIdx])
		if !ok {
			return true
		}

		if thisValue != prevValue {
			// If we've been asked to stop, stop here
			if writer != nil && flushToByColumn(writer, chunk, f, prevValue) {
				return true
			}

			if writer, chunk = newWriter(newSchema); writer == nil {
				err = errNoWriter
				return true
			}
			prevValue = thisValue
		}

		count++
		err = writer.Write(v...)
		return err != nil
	}, cols...)

	// Last write
	_ = flushToByColumn(writer, chunk, f, prevValue)
	return
}

// convertToString converst value to string because currently all the keys in Badger are stored in the form of string before hashing to the byte array
func convertToString(value interface{}) (string, bool) {
	v, ok := value.(string)
	if ok {
		return v, true
	}
	valueInt, ok := value.(int64)
	if ok {
		return strconv.FormatInt(valueInt, 10), true
	}
	return "", false
}

// Helper function for flushing
func flushToByColumn(writer *orc.Writer, chunk *bytes.Buffer, f func(string, []byte) bool, columnValue string) bool {
	if err := writer.Close(); err != nil {
		return true
	}

	return f(columnValue, chunk.Bytes())
}

// Helper function for flushing
func flushTo(writer *orc.Writer, chunk *bytes.Buffer, f func([]byte) bool) bool {
	if err := writer.Close(); err != nil {
		return true
	}

	return f(chunk.Bytes())
}

// newWriter creates a new buffer and a writer for a schema
func newWriter(schema *orc.TypeDescription) (*orc.Writer, *bytes.Buffer) {
	chunk := new(bytes.Buffer)
	w, err := orc.NewWriter(chunk, orc.SetSchema(schema))
	if err != nil {
		// TODO: change to injectetd monitor
		fmt.Println("cannot create new writer", err)
		return nil, nil
	}

	return w, chunk
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
func (i *iterator) Schema() (result map[string]reflect.Type) {
	schema := i.reader.Schema()
	result = map[string]reflect.Type{}
	for _, c := range schema.Columns() {
		if t, err := schema.GetField(c); err == nil {
			if t, supported := convert(t.Type().GetKind().String()); supported {
				result[c] = t
			}
		}
	}
	return
}

// fields gets the set of supported columns
func (i *iterator) fields() (out []field) {
	schema := i.reader.Schema()
	for _, columnName := range schema.Columns() {
		if f, err := schema.GetField(columnName); err == nil {
			if category, ok := supported[f.Type().GetKind().String()]; ok {
				out = append(out, field{
					name:     columnName,
					category: category,
				})
			}
		}
	}
	return
}

type field struct {
	name     string
	category orc.Category
}

// Close closes the iterator.
func (i *iterator) Close() error {
	return i.reader.Close()
}

// List of supported types
var supported = map[string]orc.Category{
	"LONG":    orc.CategoryLong,
	"STRING":  orc.CategoryVarchar,
	"VARCHAR": orc.CategoryVarchar,
	"DOUBLE":  orc.CategoryDouble,
}

func convert(kind string) (reflect.Type, bool) {
	switch kind {
	case "LONG":
		return reflect.TypeOf(int64(0)), true
	case "DOUBLE":
		return reflect.TypeOf(float64(0)), true
	case "VARCHAR":
		return reflect.TypeOf(""), true
	case "STRING":
		return reflect.TypeOf(""), true
	}
	return nil, false
}
