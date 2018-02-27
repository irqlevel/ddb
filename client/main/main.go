package main

import (
	client "ddb/client/core"
	"flag"
	"fmt"
	"os"
)

func main() {
	var endpoint string
	var operation string
	var key string
	var value string
	var err error

	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:8080", "endpoint address")
	flag.StringVar(&operation, "operation", "", "operation")
	flag.StringVar(&key, "key", "", "key")
	flag.StringVar(&value, "value", "", "value")

	flag.Parse()

	c := client.NewClient(endpoint)
	switch operation {
	case "set":
		err = c.SetKey(key, value)
	case "get":
		value, err = c.GetKey(key)
		if err == nil {
			fmt.Printf("%s\n", value)
		}
	case "delete":
		err = c.DeleteKey(key)
	default:
		err = fmt.Errorf("Unknown operation %s", operation)
	}

	if err != nil {
		fmt.Printf("error %v\n", err)
		os.Exit(1)
	}
}
