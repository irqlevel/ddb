#!/bin/bash -xv
rm -rf bin
mkdir bin
go build -o bin/mds mds/main/main.go
go build -o bin/client client/main/main.go