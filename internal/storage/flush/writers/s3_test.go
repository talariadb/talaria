// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package writers

import (
	"github.com/grab/talaria/internal/encoding/key"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestS3Writer(t *testing.T) {
	_, err := NewS3Writer("us-east-1", "testBucket", 128)

	assert.Nil(t, err)
}

func TestS3Writer_Write(t *testing.T) {
	mockUploader := &MockS3Uploader{}
	mockUploader.On("Upload", mock.Anything, mock.Anything).Return(nil, nil).Once()

	s3Writer := &S3Writer{
		uploader: mockUploader,
		bucket: "testBucket",
	}

	val := []byte("Test Upload Data")

	err := s3Writer.Write(key.Key("testKey"), val)

	assert.Equal(t, err, nil)
}