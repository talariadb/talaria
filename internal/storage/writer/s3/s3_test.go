// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"bytes"
	"testing"

	eorc "github.com/crphang/orc"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/encoding/orc"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Create a base block for testing purpose
func blockBase() ([]block.Block, error) {
	schema := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Int64,
		"col2": typeof.Float64,
	}
	orcSchema, _ := orc.SchemaFor(schema)

	orcBuffer1 := &bytes.Buffer{}
	writer, _ := eorc.NewWriter(orcBuffer1,
		eorc.SetSchema(orcSchema))
	_ = writer.Write("eventName", 1, 1.0)
	_ = writer.Close()

	apply := block.Transform(nil)

	block, err := block.FromOrcBy(orcBuffer1.Bytes(), "col0", nil, apply)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func TestS3Writer(t *testing.T) {
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	_, err := New(m, "testBucket", "", "us-east-1", "", "", "", "", "", "", 128)

	assert.Nil(t, err)
}

func TestS3Writer_Write(t *testing.T) {
	m := monitor.New(logging.NewNoop(), statsd.NewNoop(), "x", "y")
	mockUploader := &MockS3Uploader{}
	mockUploader.On("Upload", mock.Anything, mock.Anything).Return(nil, nil).Once()

	s3Writer, err := New(m, "testBucket", "", "us-east-1", "", "", "", "", "", "", 128)
	s3Writer.monitor = monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x")
	s3Writer.uploader = mockUploader
	s3Writer.bucket = "testBucket"

	block, err := blockBase()
	assert.Nil(t, err)
	err = s3Writer.Write(key.Key("testKey"), block)

	assert.Equal(t, err, nil)
}
