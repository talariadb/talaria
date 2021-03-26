package parquet

import (
	"bytes"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"io"
	"os"
	"sort"
)

var errNoWriter = errors.New("unable to create Parquet writer")

// Iterator represents parquet data frame.
type Iterator interface {
	io.Closer
	Range(f func(int, []interface{}) bool, columns ...string) (int, bool)
	Schema() typeof.Schema
}

// FromFile creates an iterator from a file.
func FromFile(filename string) (Iterator, error) {
	rf, err := os.Open(filename)

	if err != nil {
		return nil, err
	}

	r, err := goparquet.NewFileReader(rf)
	return &iterator{reader: r}, nil
}

// FromBuffer creates an iterator from a buffer.
func FromBuffer(b []byte) (Iterator, error) {
	r, err := goparquet.NewFileReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	return &iterator{reader: r}, nil
}

// Range is a helper function that ranges over a set of columns in a Parquet buffer
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

// Iterator represents parquet data frame.
type iterator struct {
	reader *goparquet.FileReader
}

// Range iterates through the reader.
func (i *iterator) Range(f func(int, []interface{}) bool, columns ...string) (index int, stop bool) {
	//TODO: Do this once the release is done
	//c := i.reader.SchemaReader.setSelectedColumns
	r := i.reader
	for {
		row, err := r.NextRow()
		if err == io.EOF {
			break
		}

		var arr []interface{}

		// We need to ensure that the row has columns ordered by name since that is how columns are generated
		// in the upstream schema
		keys := make([]string, len(row))
		i := 0
		for k := range row {
			keys[i] = k
			i++
		}
		sort.Strings(keys)

		for k := range keys {
			k := keys[k]
			v := row[k]

			arr = append(arr, v)
		}

		if stop = f(index-1, arr); stop {
			return index, false
		}
	}

	return index, true
}

// Schema gets the SQL schema for the iterator.
func (i *iterator) Schema() typeof.Schema {
	schema := i.reader.SchemaReader
	result := make(typeof.Schema, len(schema.Columns()))
	for _, c := range schema.Columns() {
		t := c.Type()

		if t, supported := typeof.FromParquet(t); supported {
			result[c.Name()] = t
		}
	}
	return result
}

// Close closes the iterator.
func (i *iterator) Close() error {
	// No Op
	return nil
}
