#!/bin/bash
protoc talaria.proto --gogoslick_out=plugins=grpc:.
python3 -m grpc_tools.protoc -I. --python_out=../client/python --grpc_python_out=../client/python ./talaria.proto