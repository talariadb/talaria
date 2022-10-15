package server

import (
	"context"
	"fmt"
	"testing"

	talaria "github.com/kelindar/talaria/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.nhat.io/grpcmock"
)

func mockQueryServiceServer(m ...grpcmock.ServerOption) grpcmock.ServerMockerWithContextDialer {
	opts := []grpcmock.ServerOption{grpcmock.RegisterService(talaria.RegisterQueryServer)}
	opts = append(opts, m...)

	return grpcmock.MockServerWithBufConn(opts...)
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()

	const descTableWireName = "talaria.Query/DescribeTable"
	fmt.Println(descTableWireName)
	testCases := []struct {
		scenario   string
		mockServer grpcmock.ServerMockerWithContextDialer
		request    *talaria.DescribeTableRequest
		expected   *talaria.DescribeTableResponse
	}{
		{
			scenario: "success",
			mockServer: mockQueryServiceServer(func(s *grpcmock.Server) {
				s.ExpectUnary(descTableWireName).
					WithPayload(&talaria.DescribeTableRequest{Name: "events"}).
					Return(&talaria.DescribeTableResponse{Table: &talaria.TableMeta{
						Schema:  "data",
						Table:   "events",
						Columns: nil,
						Hashby:  "event",
						Sortby:  "ingested_at",
					}})
			}),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.scenario, func(t *testing.T) {
			t.Parallel()

			_, dialer := tc.mockServer(t)

			out := &talaria.DescribeTableResponse{Table: &talaria.TableMeta{
				Schema:  "data",
				Table:   "events",
				Columns: nil,
				Hashby:  "event",
				Sortby:  "ingested_at",
			}}
			err := grpcmock.InvokeUnary(context.Background(),
				descTableWireName, &talaria.DescribeTableRequest{Name: "events"}, out,
				grpcmock.WithInsecure(),
				grpcmock.WithContextDialer(dialer),
			)

			require.NoError(t, err)

			assert.Equal(t, "events", out.Table.Table)
			assert.Equal(t, "data", out.Table.Schema)
			assert.Equal(t, "event", out.Table.Hashby)
			assert.Equal(t, "ingested_at", out.Table.Sortby)

		})
	}
}

func TestGetNodes(t *testing.T) {
	t.Parallel()

	const getNodesWireName = "talaria.Query/GetNodes"
	fmt.Println(getNodesWireName)
	testCases := []struct {
		scenario   string
		mockServer grpcmock.ServerMockerWithContextDialer
		request    *talaria.GetNodesRequest
		expected   *talaria.GetNodesResponse
	}{
		{
			scenario: "success",
			mockServer: mockQueryServiceServer(func(s *grpcmock.Server) {
				s.ExpectUnary(getNodesWireName).
					WithPayload(&talaria.GetNodesRequest{}).
					Return(&talaria.GetNodesResponse{Nodes: []*talaria.Endpoint{
						&talaria.Endpoint{
							Host: "127.0.0.1",
							Port: 8080,
						},
					}})
			}),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.scenario, func(t *testing.T) {
			t.Parallel()

			_, dialer := tc.mockServer(t)

			out := &talaria.GetNodesResponse{}
			err := grpcmock.InvokeUnary(context.Background(),
				getNodesWireName, &talaria.GetNodesRequest{}, out,
				grpcmock.WithInsecure(),
				grpcmock.WithContextDialer(dialer),
			)

			require.NoError(t, err)
			if assert.Equal(t, 1, len(out.Nodes)) {
				assert.Equal(t, "127.0.0.1", out.Nodes[0].Host)
				assert.Equal(t, int32(8080), out.Nodes[0].Port)
			}
		})
	}
}
