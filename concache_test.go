package concache

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	BUCKETS_COUNT = 32
)

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
	if !found1 || actualValue1 != 123 {
		testing.Errorf("Item 1: Expect 123, but return %d", actualValue1)
	}

	if !found2 || actualValue2 != "56789" {
		testing.Errorf("Item 1: Expect '56789', but return %s", actualValue2)
	}
}

func TestDeleteCache(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	cache.Set("GOCACHE", 123, NO_EXPIRATION)
	value1, found1 := cache.Get("GOCACHE")
	if !found1 || value1.(int) != 123 {
		testing.Errorf("Expect value of cache should be equal to 123, but %d", value1.(int))
	}

	cache.Delete("GOCACHE")
	value2, found2 := cache.Get("GOCACHE")
	if found2 {
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
	value1, _ := cache.Get("concache")
	fmt.Printf("Value of cache before expired: %v", value1)

	time.Sleep(time.Second * 4)

	value2, found := cache.Get("concache")
	if found {
		testing.Errorf("Expect: value of cache should be deleted when expired, but %v", value2)
	}
}

func TestDeleteExpiredItems(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	tempStruct := struct {
		first  int
		second string
	}{
		999,
		"gophers",
	}
	cache.Set("concache", &tempStruct, time.Second*3)
	value1, _ := cache.Get("concache")
	fmt.Printf("Value of cache before expired: %v", value1)

	time.Sleep(time.Second * 4)

	cache.DeleteExpiredItems()

	bucket, _ := cache.getBucketWithDjbHasher("concache")
	item, found := bucket.Items["concache"]

	if found {
		testing.Errorf("Expect: value of cache should be deleted when expired, but %v", item.Value)
	}
}

func TestGetAllItemsCache(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	cache.Set("GOPHER", 123, NO_EXPIRATION)
	cache.Set("GOCONCURRENCACHE", "56789", NO_EXPIRATION)
	allItemsCache := cache.GetAllItemsCache()
	firstItem := allItemsCache["GOPHER"].Value.(int)
	secondItem := allItemsCache["GOCONCURRENCACHE"].Value.(string)
	if firstItem != 123 {
		testing.Errorf("Expect: value of first item should be 123, but %d", firstItem)
	}
	if secondItem != "56789" {
		testing.Errorf("Expect: value of first item should be 123, but %s", secondItem)
	}
}

func TestCountItemsCache(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	cache.Set("GOPHER", 123, NO_EXPIRATION)
	cache.Set("GOCONCURRENCACHE", "56789", NO_EXPIRATION)
	itemsCount := cache.CountItemsCache()
	if itemsCount != 2 {
		testing.Errorf("Expect: total items in cache should be 2, but %d", itemsCount)
	}
}

func TestFlushAll(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache")
	cache.Set("GOPHER", 123, NO_EXPIRATION)
	cache.Set("GOCONCURRENCACHE", "56789", NO_EXPIRATION)
	cache.FlushAll()
	if cache.CountItemsCache() > 0 {
		testing.Errorf("Expect: all items in cache should be cleared empty")
	}
}

func TestBackUpCacheToDiskInterval(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache_backup_interval")
	cache.BackUpCacheToDiskInterval(time.Second * 2)
	cache.Set("GOCONCURRENCACHE", "56789", NO_EXPIRATION)

	time.Sleep(time.Second * 2)
	cache.Delete("GOCONCURRENCACHE")
	cache.LoadCacheFromDisk()
	_, found := cache.Get("GOCONCURRENCACHE")
	if !found {
		testing.Errorf("Expect: cache should be backed up to disk and loaded again successfully")
	}
}

func TestPersistAndLoadCache(testing *testing.T) {
	cache := NewCache(BUCKETS_COUNT, "cache_persistence")

	cache.Set("concache", 999, time.Second*3)
	cache.PersistCacheToDisk()
	cache.Delete("concache")
	value, found := cache.Get("concache")
	if !found {
		cache.LoadCacheFromDisk()
		value, found = cache.Get("concache")
		if !found {
			testing.Error("Cannot get cache that was loaded from disk")
		} else {
			if value.(int) != 999 {
				testing.Errorf("Expect: value of first property should be 999, but %d", value.(int))
			}
		}
	} else {
		testing.Error("Deleting cache with key does not work")
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

func BenchmarkGetWithShardedCache(bench *testing.B) {
	bench.StopTimer()
	shardedCache := NewCache(BUCKETS_COUNT, "shardedCache")

	n := 1000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		shardedCache.Set(keys[i], keys[i], NO_EXPIRATION)
	}

	bench.StartTimer()
	for i := 0; i < bench.N; i++ {
		for _, key := range keys {
			shardedCache.Get(key)
		}
	}
}

func BenchmarkSetWithShardedCache(bench *testing.B) {
	bench.StopTimer()
	shardedCache := NewCache(BUCKETS_COUNT, "shardedCache")

	n := 5000
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
