// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package ingest

import (
	"io/ioutil"
	"sync"
	"testing"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.myteksi.net/grab-x/talaria/internal/monitor"
	"gitlab.myteksi.net/grab-x/talaria/internal/storage/s3"
)

func TestQueueReader(t *testing.T) {
	queue := make(chan *awssqs.Message, 1)
	queue <- newMessage()
	close(queue)

	// Create SQS reader mock
	sqs := new(MockReader)
	sqs.On("StartPolling", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((<-chan *awssqs.Message)(queue))
	sqs.On("Close").Return(nil)

	// Create S3 client mock
	orc, _ := ioutil.ReadFile("../../../test/test.snappy.orc")
	s3 := new(s3.MockClient)
	s3.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(orc, nil)

	// Create new storage
	storage := New(sqs, s3, monitor.NewNoop())
	assert.NotNil(t, storage)
	defer storage.Close()

	// Range until we're done
	var wg sync.WaitGroup
	wg.Add(1)
	storage.Range(func(v []byte) bool {
		assert.Equal(t, orc, v)
		wg.Done()
		return true
	})

	wg.Wait()
}

func newMessage() *awssqs.Message {
	evt := `{  
		"Records":[  
		   {  
			  "eventVersion":"2.0",
			  "eventSource":"aws:s3",
			  "awsRegion":"us-east-1",
			  "eventName":"event-type",
			  "userIdentity":{  
				 "principalId":"Amazon-customer-ID-of-the-user-who-caused-the-event"
			  },
			  "requestParameters":{  
				 "sourceIPAddress":"ip-address-where-request-came-from"
			  },
			  "responseElements":{  
				 "x-amz-request-id":"Amazon S3 generated request ID",
				 "x-amz-id-2":"Amazon S3 host that processed the request"
			  },
			  "s3":{  
				 "s3SchemaVersion":"1.0",
				 "configurationId":"ID found in the bucket notification configuration",
				 "bucket":{  
					"name":"bucket-name",
					"ownerIdentity":{  
					   "principalId":"Amazon-customer-ID-of-the-bucket-owner"
					},
					"arn":"bucket-ARN"
				 },
				 "object":{  
					"key":"object-key",
					"eTag":"object eTag",
					"versionId":"object version if bucket is versioning-enabled, otherwise null"     
				 }
			  }
		   }
		]
	}`

	return &awssqs.Message{
		Body: &evt,
	}
}
