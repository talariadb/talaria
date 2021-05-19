// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package presto

import (
	"errors"
	"io"
	"net/rpc"
	"strings"
	"sync"

	"github.com/samuel/go-thrift/thrift"
)

type serverCodec struct {
	conn       thrift.Transport
	nameCache  map[string]string // incoming name -> registered name
	methodName map[uint64]string // sequence ID -> method name
	mu         sync.Mutex
}

// ServeConn runs the Thrift RPC server on a single connection. ServeConn blocks,
// serving the connection until the client hangs up. The caller typically invokes
// ServeConn in a go statement.
func ServeConn(conn thrift.Transport) {
	rpc.ServeCodec(NewServerCodec(conn))
}

// NewServerCodec returns a new rpc.ServerCodec using Thrift RPC on conn using the specified protocol.
func NewServerCodec(conn thrift.Transport) rpc.ServerCodec {
	return &serverCodec{
		conn:       conn,
		nameCache:  make(map[string]string, 8),
		methodName: make(map[uint64]string, 8),
	}
}

func (c *serverCodec) ReadRequestHeader(request *rpc.Request) error {
	name, messageType, seq, err := c.conn.ReadMessageBegin()
	if err != nil {
		return err
	}
	if messageType != thrift.MessageTypeCall { // Currently don't support one way
		return errors.New("thrift: expected Call message type")
	}

	// TODO: should use a limited size cache for the nameCache to avoid a possible
	//       memory overflow from nefarious or broken clients
	wireName := name
	name = strings.Replace(name, "trino", "presto", 1)

	newName := c.nameCache[name]
	if newName == "" {
		newName = thrift.CamelCase(name)
		if !strings.ContainsRune(newName, '.') {
			newName = "Thrift." + newName
		}
		c.nameCache[name] = newName
	}

	c.mu.Lock()
	c.methodName[uint64(seq)] = wireName
	c.mu.Unlock()

	request.ServiceMethod = newName
	request.Seq = uint64(seq)

	return nil
}

func (c *serverCodec) ReadRequestBody(thriftStruct interface{}) error {
	if thriftStruct == nil {
		if err := thrift.SkipValue(c.conn, thrift.TypeStruct); err != nil {
			return err
		}
	} else {
		if err := thrift.DecodeStruct(c.conn, thriftStruct); err != nil {
			return err
		}
	}
	return c.conn.ReadMessageEnd()
}

func (c *serverCodec) WriteResponse(response *rpc.Response, thriftStruct interface{}) error {
	c.mu.Lock()
	methodName := c.methodName[response.Seq]
	delete(c.methodName, response.Seq)
	c.mu.Unlock()
	response.ServiceMethod = methodName

	mtype := byte(thrift.MessageTypeReply)
	if response.Error != "" {
		mtype = thrift.MessageTypeException
		etype := int32(thrift.ExceptionInternalError)
		if strings.HasPrefix(response.Error, "rpc: can't find") {
			etype = thrift.ExceptionUnknownMethod
		}
		thriftStruct = &thrift.ApplicationException{response.Error, etype}
	}
	if err := c.conn.WriteMessageBegin(response.ServiceMethod, mtype, int32(response.Seq)); err != nil {
		return err
	}
	if err := thrift.EncodeStruct(c.conn, thriftStruct); err != nil {
		return err
	}
	if err := c.conn.WriteMessageEnd(); err != nil {
		return err
	}
	return c.conn.Flush()
}

func (c *serverCodec) Close() error {
	if cl, ok := c.conn.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}
