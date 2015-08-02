package concache

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pmylund/go-cache"
)

const (
	BUCKETS_COUNT = 32
)

// var keys []string{
// 	"gopher1",
// 	"gopher12",
// 	"gopher123",
// 	"gopher1234",
// 	"gopher12345",
// 	"gopher123456",
// 	"gopher1234567",
// 	"gopher12345678",
// 	"gopher123456789",
// 	"1gopher",
// 	"12gopher",
// 	"123gopher",
// 	"1234gopher",
// 	"12345gopher",
// 	"123456gopher",
// 	"1234567gopher",
// 	"12345678gopher",
// 	"123456789gopher"
// }

var dataCache = []struct {
	key   string
	value string
}{
	{"gopher1", "value0"},
	{"gopher12", "value1"},
	{"gopher123", "value2"},
	{"gopher1234", "value3"},
	{"gopher12345", "value4"},
	{"gopher123456", "value5"},
	{"gopher1234567", "value6"},
	{"gopher12345678", "value7"},
	{"gopher123456789", "value8"},
	{"gopher1234567890", "value9"},
}

/*** functionality testing ***/

func TestGetBucketWithDjbHasher(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	_, bucketIndex1 := cache.getBucketWithDjbHasher("conCache")
	_, bucketIndex2 := cache.getBucketWithDjbHasher("concurrentCache")

	_, bucketIndex3 := cache.getBucketWithDjbHasher("conCache")
	_, bucketIndex4 := cache.getBucketWithDjbHasher("concurrentCache")

	fmt.Printf("bucketIndex1: %d", bucketIndex1)
	fmt.Printf("bucketIndex2: %d", bucketIndex2)
	fmt.Printf("bucketIndex3: %d", bucketIndex3)
	fmt.Printf("bucketIndex4: %d", bucketIndex4)

	if bucketIndex1 != bucketIndex3 {
		testing.Errorf("Expect bucketIndex1 == bucketIndex3, but bucketIndex1 = %d and bucketIndex2 = %d", bucketIndex1, bucketIndex3)
	}

	if bucketIndex2 != bucketIndex4 {
		testing.Errorf("Expect bucketIndex2 == bucketIndex4, but bucketIndex2 = %d and bucketIndex4 = %d", bucketIndex2, bucketIndex4)
	}
}

func TestGetBucketWithBuiltInHasher(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	_, bucketIndex1 := cache.getBucketWithBuiltInHasher("conCache")
	_, bucketIndex2 := cache.getBucketWithBuiltInHasher("concurrentCache")

	_, bucketIndex3 := cache.getBucketWithBuiltInHasher("conCache")
	_, bucketIndex4 := cache.getBucketWithBuiltInHasher("concurrentCache")

	fmt.Printf("bucketIndex1: %d", bucketIndex1)
	fmt.Printf("bucketIndex2: %d", bucketIndex2)
	fmt.Printf("bucketIndex3: %d", bucketIndex3)
	fmt.Printf("bucketIndex4: %d", bucketIndex4)

	if bucketIndex1 != bucketIndex3 {
		testing.Errorf("Expect bucketIndex1 == bucketIndex3, but bucketIndex1 = %d and bucketIndex2 = %d", bucketIndex1, bucketIndex3)
	}

	if bucketIndex2 != bucketIndex4 {
		testing.Errorf("Expect bucketIndex2 == bucketIndex4, but bucketIndex2 = %d and bucketIndex4 = %d", bucketIndex2, bucketIndex4)
	}
}

func TestGetSetCache(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	cache.Set("GOPHER", 123, NO_EXPIRATION)
	cache.Set("GOCONCURRENCACHE", "56789", NO_EXPIRATION)
	value1, found1 := cache.Get("GOPHER")
	value2, found2 := cache.Get("GOCONCURRENCACHE")
	actualValue1 := value1.(int)
	actualValue2 := value2.(string)
	if !found1 || !found2 || actualValue1 != 123 || actualValue2 != "56789" {
		testing.Errorf("Item 1: Expect 123, but return %d", actualValue1)
	}

	if !found2 || actualValue2 != "56789" {
		testing.Errorf("Item 1: Expect '56789', but return %s", actualValue2)
	}
}

func TestDeleteCache(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	cache.Set("GOCACHE", 123, NO_EXPIRATION)
	value1, found := cache.Get("GOCACHE")
	if !found || value1.(int) != 123 {
		testing.Errorf("Expect value of cache should be equal to 123, but %d", value1.(int))
	}

	cache.Delete("GOCACHE")
	value2, found := cache.Get("GOCACHE")
	if found {
		testing.Errorf("Expect: value of cache should be deleted, but %d", value2.(int))
	}
}

func TestExpiredItems(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	tempStruct := struct {
		first  int
		second string
	}{
		999,
		"gophers",
	}
	cache.Set("concache", &tempStruct, time.Second*3)
	value1, found := cache.Get("concache")
	fmt.Printf("Value of cache before expired: %v", value1)

	time.Sleep(time.Second * 4)

	value2, found := cache.Get("concache")
	if found {
		testing.Errorf("Expect: value of cache should be deleted when expired, but %v", value2)
	}
}

/*** Performance testing ***/

func BenchmarkGetBucketWithBuiltInHasher(bench *testing.B) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	for i := 0; i < bench.N; i++ {
		cache.getBucketWithBuiltInHasher("GOCONCURRENTCACHE")
	}
}

func BenchmarkGetBucketWithDjbHasher(bench *testing.B) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	for i := 0; i < bench.N; i++ {
		cache.getBucketWithDjbHasher("GOCONCURRENTCACHE")
	}
}

// func BenchmarkGetWithShardedCache(bench *testing.B) {
// 	bench.StopTimer()
// 	cache := NewCache(BUCKETS_COUNT)
// 	cache.Set("GOisFUN", "oops", NO_EXPIRATION)
// 	bench.StartTimer()
// 	for i := 0; i < bench.N; i++ {
// 		cache.Get("GOisFUN")
// 	}
// }

// func BenchmarkGetWithoutShardedCache(bench *testing.B) {
// 	bench.StopTimer()
// 	noShardedCache := cache.New(time.Minute*5, time.Second*30)
// 	noShardedCache.Set("GOisFUN", "oops", NO_EXPIRATION)
// 	bench.StartTimer()
// 	for i := 0; i < bench.N; i++ {
// 		noShardedCache.Get("GOisFUN")
// 	}
// }

func BenchmarkGetWithShardedCache(bench *testing.B) {
	bench.StopTimer()
	shardedCache := NewCache(BUCKETS_COUNT, "shardedCache")

	n := 1000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		shardedCache.Set(keys[i], keys[i], NO_EXPIRATION)
	}

	// for _, data := range dataCache {
	// 	shardedCache.Set(data.key, data.value, shardedNO_EXPIRATION)
	// }

	bench.StartTimer()
	for i := 0; i < bench.N; i++ {
		for _, key := range keys {
			shardedCache.Get(key)
		}
	}
}

func BenchmarkGetWithoutShardedCache(bench *testing.B) {
	bench.StopTimer()
	noShardedCache := cache.New(time.Minute*5, time.Second*30)

	n := 1000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		noShardedCache.Set(keys[i], keys[i], NO_EXPIRATION)
	}

	// for _, data := range dataCache {
	// 	noShardedCache.Set(data.key, data.value, NO_EXPIRATION)
	// }

	bench.StartTimer()
	for i := 0; i < bench.N; i++ {
		for _, key := range keys {
			noShardedCache.Get(key)
		}
	}
}

func BenchmarkSetWithShardedCache(bench *testing.B) {
	bench.StopTimer()
	shardedCache := NewCache(BUCKETS_COUNT, "shardedCache")

	n := 50000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	bench.StartTimer()
	for i := 0; i < bench.N; i++ {
		var waitGroup sync.WaitGroup
		waitGroup.Add(n)

		for _, key := range keys {
			go func(key string) {
				shardedCache.Set(key, key, NO_EXPIRATION)
				waitGroup.Done()
			}(key)
		}
		waitGroup.Wait()
	}
}

func BenchmarkSetWithoutShardedCache(bench *testing.B) {
	bench.StopTimer()
	noShardedCache := cache.New(time.Minute*5, time.Millisecond*30)

	n := 50000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	bench.StartTimer()
	for i := 0; i < bench.N; i++ {
		var waitGroup sync.WaitGroup
		waitGroup.Add(n)

		for _, key := range keys {
			go func(key string) {
				noShardedCache.Set(key, key, NO_EXPIRATION)
				waitGroup.Done()
			}(key)
		}
		waitGroup.Wait()
	}
}
