package main

import (
	mds "ddb/mds/core"
	"flag"
	"fmt"
)

func main() {
	var params mds.MdsParameters

	flag.StringVar(&params.ApiAddress, "apiAddress", "127.0.0.1:8000", "api address")
	flag.StringVar(&params.DebugAddress, "debugAddress", "127.0.0.1:8001", "debug address")
	flag.StringVar(&params.LogFile, "logFile", "mds.log", "log file path")
	flag.StringVar(&params.PidFile, "pidFile", "mds.pid", "pid file")
	flag.StringVar(&params.StoragePath, "storagePath", ".", "storage path")

	flag.Parse()

	err := mds.GetMds().Run(&params)
	if err != nil {
		fmt.Printf("mds run error %v\n", err)
	}
}
