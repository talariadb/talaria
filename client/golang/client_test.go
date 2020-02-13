package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDial(t *testing.T) {
	testData := []struct {
		desc           string
		blocking       bool
		expectedClient bool
		expectedErr    error
	}{
		{
			desc:        "client creation blocked by connection",
			blocking:    true,
			expectedErr: ErrUnableToConnect,
		},
		{
			desc:           "client creation not blocked by connection",
			expectedClient: true,
		},
	}

	for _, td := range testData {
		t.Run(td.desc, func(t *testing.T) {
			var client *Client
			var err error

			if !td.blocking {
				client, err = Dial("invalid", WithNonBlock())
			} else {
				client, err = Dial("invalid")
			}

			assert.Equal(t, td.expectedClient, client != nil)
			assert.Equal(t, td.expectedErr, err)
		})
	}
}
