// Copyright 2012-2015 Samuel Stauffer. All rights reserved.
// Use of this source code is governed by a 3-clause BSD
// license that can be found in the LICENSE file at https://github.com/samuel/go-thrift/blob/master/LICENSE.

package presto

import (
	"bytes"
	"github.com/samuel/go-thrift/thrift"
	"github.com/stretchr/testify/assert"
	"net/rpc"
	"testing"
)

type ClosingBuffer struct {
	*bytes.Buffer
}

func (c *ClosingBuffer) Close() error {
	return nil
}

// TestGetWireNameFor replaces the service method,
//  trino -> presto for 1st occurence
func TestGetWireNameFor(t *testing.T) {
	wireName := "trino.GetTableMetadata"
	assert.Equal(t, "presto.GetTableMetadata", getWireNameFor(wireName))
	wireName = "presto.GetTableMetadata"
	assert.Equal(t, "presto.GetTableMetadata", getWireNameFor(wireName))
}

// TestTrinoCompatibility makes sure the ServerCodec returns the same method name
// in the response as was in the request.
// and intercepts the trino service methods to presto service methods
func TestTrinoCompatibility(t *testing.T) {
	buf := &ClosingBuffer{&bytes.Buffer{}}
	clientCodec := thrift.NewClientCodec(thrift.NewTransport(buf, thrift.BinaryProtocol), false)
	defer clientCodec.Close()
	serverCodec := NewServerCodec(thrift.NewTransport(buf, thrift.BinaryProtocol))
	defer serverCodec.Close()

	trinoReq := &rpc.Request{
		ServiceMethod: "trino.GetSplits",
		Seq:           143,
	}
	empty := &struct{}{}
	if err := clientCodec.WriteRequest(trinoReq, empty); err != nil {
		t.Fatal(err)
	}

	var prestoReq rpc.Request
	if err := serverCodec.ReadRequestHeader(&prestoReq); err != nil {
		t.Fatal(err)
	}
	if trinoReq.Seq != prestoReq.Seq {
		t.Fatalf("Expected seq %d, got %d", trinoReq.Seq, prestoReq.Seq)
	}
	if prestoReq.ServiceMethod != "Presto.GetSplits" {
		t.Fatalf("Expected serviceMethod %s, got %s", "Presto.GetSplits", prestoReq.ServiceMethod)
	}
	if err := serverCodec.ReadRequestBody(empty); err != nil {
		t.Fatal(err)
	}
	prestoRes := &rpc.Response{
		ServiceMethod: prestoReq.ServiceMethod,
		Seq:           prestoReq.Seq,
	}
	if err := serverCodec.WriteResponse(prestoRes, empty); err != nil {
		t.Fatal(err)
	}
	var trinoRes rpc.Response
	if err := clientCodec.ReadResponseHeader(&trinoRes); err != nil {
		t.Fatal(err)
	}
	if trinoRes.Seq != trinoReq.Seq {
		t.Fatalf("Expected seq %d, got %d", trinoReq.Seq, trinoRes.Seq)
	}
	if trinoRes.Error != "" {
		t.Fatalf("Expected error of '' instead of '%s'", trinoRes.Error)
	}
	if trinoRes.ServiceMethod != trinoReq.ServiceMethod {
		t.Fatalf("Expected ServiceMethod of '%s' instead of '%s'", trinoReq.ServiceMethod, trinoRes.ServiceMethod)
	}
}

// TestServerMethodName makes sure the ServerCodec returns the same method name
// in the response as was in the request.
//  and presto service method names are not intercepted.
func TestServerMethodName(t *testing.T) {
	buf := &ClosingBuffer{&bytes.Buffer{}}
	clientCodec := thrift.NewClientCodec(thrift.NewTransport(buf, thrift.BinaryProtocol), false)
	defer clientCodec.Close()
	serverCodec := NewServerCodec(thrift.NewTransport(buf, thrift.BinaryProtocol))
	defer serverCodec.Close()

	req := &rpc.Request{
		ServiceMethod: "presto.GetSplits",
		Seq:           3,
	}
	empty := &struct{}{}
	if err := clientCodec.WriteRequest(req, empty); err != nil {
		t.Fatal(err)
	}
	var req2 rpc.Request
	if err := serverCodec.ReadRequestHeader(&req2); err != nil {
		t.Fatal(err)
	}
	if req.Seq != req2.Seq {
		t.Fatalf("Expected seq %d, got %d", req.Seq, req2.Seq)
	}
	t.Logf("Mangled method name: %s", req2.ServiceMethod)
	if req2.ServiceMethod != "Presto.GetSplits" {
		t.Fatalf("Expected serviceMethod %s, got %s", "Presto.GetSplits", req2.ServiceMethod)
	}
	if err := serverCodec.ReadRequestBody(empty); err != nil {
		t.Fatal(err)
	}
	res := &rpc.Response{
		ServiceMethod: req2.ServiceMethod,
		Seq:           req2.Seq,
	}
	if err := serverCodec.WriteResponse(res, empty); err != nil {
		t.Fatal(err)
	}
	var res2 rpc.Response
	if err := clientCodec.ReadResponseHeader(&res2); err != nil {
		t.Fatal(err)
	}
	if res2.Seq != req.Seq {
		t.Fatalf("Expected seq %d, got %d", req.Seq, res2.Seq)
	}
	if res2.Error != "" {
		t.Fatalf("Expected error of '' instead of '%s'", res2.Error)
	}
	if res2.ServiceMethod != req.ServiceMethod {
		t.Fatalf("Expected ServiceMethod of '%s' instead of '%s'", req.ServiceMethod, res2.ServiceMethod)
	}
}
