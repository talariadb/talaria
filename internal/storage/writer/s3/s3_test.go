// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package s3

import (
	"testing"

	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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

	block, err := block.Base()
	assert.Nil(t, err)
	err = s3Writer.Write(key.Key("testKey"), block)

	assert.Equal(t, err, nil)
}
