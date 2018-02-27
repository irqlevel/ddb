package client

import (
	"ddb/lib/common/random"
	"fmt"
	"sync"
	"testing"
)

func testClientThread(t *testing.T, c *Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < 1000; i++ {

		key := random.GenerateRandomHexString(8)
		value := random.GenerateRandomHexString(16)

		err := c.SetKey(key, value)
		if err != nil {
			t.Fatal(err)
		}

		rvalue, err := c.GetKey(key)
		if err != nil {
			t.Fatal(err)
		}

		if value != rvalue {
			t.Fatal(fmt.Errorf("key %s val %s:%d rval %s:%d",
				key, value, len(value), rvalue, len(rvalue)))

		}

		err = c.DeleteKey(key)
		if err != nil {
			t.Fatal(err)
		}

		_, err = c.GetKey(key)
		if err != ErrNotFound {
			t.Fatal(fmt.Errorf("Unexpected get deleted key error %v", err))
		}
	}
}

func TestClient(t *testing.T) {
	c := NewClient("http://127.0.0.1:8080")
	wg := new(sync.WaitGroup)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go testClientThread(t, c, wg)
	}
	wg.Wait()
}
