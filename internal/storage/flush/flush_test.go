// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package flush

import (
	"bytes"
	"testing"

	eorc "github.com/crphang/orc"
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/orc"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/storage/writer/noop"
	"github.com/stretchr/testify/assert"
)

func TestNameFunc(t *testing.T) {
	fileNameFunc := func(row map[string]interface{}) (string, error) {
		lua, _ := column.NewComputed("fileName", "main", typeof.String, `
	function main(row)

		-- Convert the time to a lua date
		local ts = row["col1"]
		local tz = timezone()
		local dt = os.date('*t', ts - tz)
	
		-- Format the filename
		return string.format("year=%d/month=%d/day=%d/ns=%s/%d-%d-%d-%s.orc", 
			dt.year,
			dt.month,
			dt.day,
			row["col0"],
			dt.hour,
			dt.min,
			dt.sec,
			"127.0.0.1")
	end

	function timezone()
		local utcdate   = os.date("!*t")
		local localdate = os.date("*t")
		--localdate.isdst = false
		return os.difftime(os.time(localdate), os.time(utcdate))
	end
	`, nil)

		output, err := lua.Value(row)
		return output.(string), err
	}

	flusher, _ := ForCompaction(monitor.NewNoop(), noop.New(), "orc", fileNameFunc)
	schema := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Timestamp,
		"col2": typeof.Float64,
	}

	orcSchema, err := orc.SchemaFor(schema)
	if err != nil {
		t.Fatal(err)
	}

	orcBuffer := &bytes.Buffer{}
	writer, _ := eorc.NewWriter(orcBuffer,
		eorc.SetSchema(orcSchema))
	_ = writer.Write("eventName", 1, 1.0)
	_ = writer.Close()

	apply := block.Transform(nil)

	blocks, err := block.FromOrcBy(orcBuffer.Bytes(), "col0", nil, apply)
	assert.NoError(t, err)
	fileName := flusher.generateFileName(blocks[0])

	assert.Equal(t, "year=46970/month=3/day=29/ns=eventName/0-0-0-127.0.0.1.orc", string(fileName))

}
