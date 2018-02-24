#!/bin/bash -xv
rm -rf logs
mkdir logs
nohup bin/mds -apiAddress 127.0.0.1:8080 -debugAddress 127.0.0.1:9111 -pidFile logs/mds.pid -logFile logs/mds.log 2>logs/mds_std_err 1>logs/mds_std_out &
