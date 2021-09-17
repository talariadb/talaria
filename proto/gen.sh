#!/bin/bash
set -e

# Makesure protoc is added to PATH
PROTOC=protoc # provide path to protoc binary
PROTO=talaria.proto
GOBIN=$(go env GOPATH)/bin
GOSRC=$(go env GOPATH)/src
TALARIA_ROOT=$GOSRC/github.com/kelindar/talaria
PROTO_PATH=$TALARIA_ROOT/proto
PYTHON_OUT=$TALARIA_ROOT/client/python/talaria_client
SED_BINARY=sed
# if using OSX make sure to install gsed;
#   brew install gnu-sed
if [ `uname` = "Darwin" ];then
    SED_BINARY=gsed
fi

# Using vtprotobuf compiler to generate gRPC code.
# https://github.com/golang/protobuf/issues/139 
# https://github.com/vdaas/vald/pull/1378
gen_golang() {
    $PROTOC -I $PROTO_PATH --go_out=$GOSRC \
     --go-grpc_out=require_unimplemented_servers=false:$GOSRC \
     --plugin protoc-gen-go-grpc=$GOBIN/protoc-gen-go-grpc \
     --go-vtproto_out=$GOSRC \
     --plugin protoc-gen-go-vtproto=$GOBIN/protoc-gen-go-vtproto \
     --go-vtproto_opt=features=marshal+unmarshal+size \
     $PROTO
}

gen_python() {
    python3 -m grpc_tools.protoc -I $PROTO_PATH \
     --python_out=$PYTHON_OUT \
     --grpc_python_out=$PYTHON_OUT \
     $PROTO
    # gnu-sed
    $SED_BINARY -i 's/\(import talaria_pb2 as talaria__pb2\)/from . \1/' $PYTHON_OUT/talaria_pb2_grpc.py
}

gen_golang
gen_python

# Not compatible with grpc-go API/v2 - use it only for API/v1(grpc-go < 1.36)
# protoc talaria.proto --gogoslick_out=plugins=grpc:.
