#!/bin/bash -xv
rm -rf bin
mkdir bin
go build -o bin/mds mds/main/main.go
