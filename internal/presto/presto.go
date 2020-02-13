// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"context"
	"fmt"
	"net"
	"net/rpc"

	"github.com/grab/talaria/internal/encoding/typeof"
	talaria "github.com/grab/talaria/proto"
	"github.com/samuel/go-thrift/thrift"
)

const frameSize = 1 << 24 // 16MB

// Column contract represent a column that can be appended.
type Column interface {
	Append(v interface{}) int
	AppendBlock([]Column)
	Count() int
	Size() int
	Kind() typeof.Type
	Last() interface{}
	Min() (int64, bool)
	AsThrift() *PrestoThriftBlock
	AsProto() *talaria.Column
	Range(from int, until int, f func(int, interface{}))
}

// Serve creates and serves thrift RPC for presto. Context is used for cancellation purposes.
func Serve(ctx context.Context, port int32, service PrestoThriftService) error {
	if err := rpc.RegisterName("Thrift", &PrestoThriftServiceServer{
		Implementation: service,
	}); err != nil {
		return err
	}

	// Create a TCP listener for our thrift
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return err
	}

	// Close the listener if context is cancelled
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	// Connection loop, old school
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if conn, err := ln.Accept(); err == nil {
				t := thrift.NewTransport(thrift.NewFramedReadWriteCloser(conn, frameSize), thrift.BinaryProtocol)
				go rpc.ServeCodec(thrift.NewServerCodec(t))
			}
		}
	}
}

// ------------------------------------------------------------------------------------------------------------

// Size returns the size of the block.
func (b *PrestoThriftBlock) Size() int {
	switch {
	case b.IntegerData != nil:
		return b.IntegerData.Size()
	case b.BigintData != nil:
		return b.BigintData.Size()
	case b.VarcharData != nil:
		return b.VarcharData.Size()
	case b.DoubleData != nil:
		return b.DoubleData.Size()
	case b.BooleanData != nil:
		return b.BooleanData.Size()
	case b.TimestampData != nil:
		return b.TimestampData.Size()
	case b.JsonData != nil:
		return b.JsonData.Size()
	}
	return 0
}

// Count returns the count of rows in the block.
func (b *PrestoThriftBlock) Count() int {
	switch {
	case b.IntegerData != nil:
		return b.IntegerData.Count()
	case b.BigintData != nil:
		return b.BigintData.Count()
	case b.VarcharData != nil:
		return b.VarcharData.Count()
	case b.DoubleData != nil:
		return b.DoubleData.Count()
	case b.BooleanData != nil:
		return b.BooleanData.Count()
	case b.TimestampData != nil:
		return b.TimestampData.Count()
	case b.JsonData != nil:
		return b.JsonData.Count()
	}
	return 0
}
