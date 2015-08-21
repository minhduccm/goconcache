# goconcache
goconcache or concurrent cache in Golang is small library that is used to store key/value in memory. Some features: <br />
	- Thread safe that can be safely used by multiple goroutines <br />
	- Storing items with expiration time or forever <br />
	- Better performance with sharded buckets when multiple goroutines read/write concurrently to same cache instance <br />
	- Be able to persist values of cache to files on disk, and reload them again <br />

## Installing

go get github.com/minhduccm/goconcache

## Example

	package main

	import (
		"fmt"
		"time"

		"github.com/minhduccm/goconcache"
	)

	func main() {
		// Create a new cache instance with 10 buckets (sharded maps)
		cacheInstance := concache.NewCache(10, "cacheinstance1")

		// Set value of key "key1" to 1 with no expiration time
		cacheInstance.Set("key1", 1, concache.NO_EXPIRATION)
		// Set value of key "key2" to "value 2" with expiration time = 5s
		cacheInstance.Set("key2", []byte("concurrent cache"), time.Second*5)

		value1, found1 := cacheInstance.Get("key1")
		value2, found2 := cacheInstance.Get("key2")

		if found1 {
			fmt.Println("value1: ", value1.(int)) // value1: 1
		}
		if found2 {
			fmt.Println("value2: ", string(value2.([]byte))) // value2: concurrent cache
		}
	}


## More detail, see docs:
http://godoc.org/github.com/minhduccm/goconcache

