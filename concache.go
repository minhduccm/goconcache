package concache

import (
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"math"
	"math/big"
	insecurerand "math/rand"
	"os"
	"sync"
	"time"
)

const (
	NO_EXPIRATION    time.Duration = -1
	CLEANUP_INTERVAL               = 60 // interval time for cleaning expired items
)

type Item struct {
	Value       interface{}
	ExpiredTime *time.Time
}

// ConcurrentMap, Item and their properties are exported because of using GOB to persist contents to file and to reload them again
type ConcurrentMap struct {
	Items map[string]*Item
}

type ShardedConcurrentMaps []*ConcurrentMap

type Cache struct {
	sync.RWMutex
	seed      uint32
	buckets   ShardedConcurrentMaps
	cacheName string
}

// Create new cache instance with:
// shardedMapsCount: how many sharded maps in this cache instance
// cacheName: name of this cache, this wll be useful for cache persistence to disk
func NewCache(shardedMapsCount int, cacheName string) *Cache {
	max := big.NewInt(0).SetUint64(uint64(math.MaxUint32))
	rnd, err := rand.Int(rand.Reader, max)
	var seed uint32
	if err != nil {
		seed = insecurerand.Uint32()
	} else {
		seed = uint32(rnd.Uint64())
	}

	buckets := make([]*ConcurrentMap, shardedMapsCount)
	for i := 0; i < shardedMapsCount; i++ {
		buckets[i] = &ConcurrentMap{
			Items: make(map[string]*Item),
		}
	}

	cacheInstance := &Cache{
		seed:      seed,
		buckets:   buckets,
		cacheName: cacheName,
	}

	cacheInstance.runExpiredItemCleaner()

	return cacheInstance
}

// djb hash algorithms
func djbHasher(seed uint32, k string) uint32 {
	var (
		l = uint32(len(k))
		d = 5381 + seed + l
		i = uint32(0)
	)

	if l >= 4 {
		for i < l-4 {
			d = (d * 33) ^ uint32(k[i])
			d = (d * 33) ^ uint32(k[i+1])
			d = (d * 33) ^ uint32(k[i+2])
			d = (d * 33) ^ uint32(k[i+3])
			i += 4
		}
	}
	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint32(k[i])
	case 3:
		d = (d * 33) ^ uint32(k[i])
		d = (d * 33) ^ uint32(k[i+1])
	case 4:
		d = (d * 33) ^ uint32(k[i])
		d = (d * 33) ^ uint32(k[i+1])
		d = (d * 33) ^ uint32(k[i+2])
	}
	return d ^ (d >> 16)
}

// Get bucket (or sharded map) by key in 1 cache instance with djb hash algorithms
// By using djb hash algorithms, this function should be 5 times faster than using built in hash function of Golang
func (cache *Cache) getBucketWithDjbHasher(key string) (*ConcurrentMap, uint32) {
	bucketsCount := (uint32)(len(cache.buckets))
	bucketIndex := djbHasher(cache.seed, key) % bucketsCount
	return cache.buckets[bucketIndex], bucketIndex
}

// Get bucket (or sharded map) by key in 1 cache instance with built in hash function of Golang
func (cache *Cache) getBucketWithBuiltInHasher(key string) (*ConcurrentMap, uint) {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	bucketsCount := len(cache.buckets)
	bucketIndex := uint(hasher.Sum32()) % uint(bucketsCount)
	return cache.buckets[bucketIndex], bucketIndex
}

// Get an item from cache by key. Return the item value (if found & not expired yet). Otherwise, return nil. And bool indicates whether key was found or not.
func (cache *Cache) Get(key string) (interface{}, bool) {
	bucket, _ := cache.getBucketWithDjbHasher(key)

	cache.RLock()
	item, ok := bucket.Items[key]
	cache.RUnlock()

	if !ok || IsExpired(item) {
		return nil, false
	}
	return item.Value, true
}

// Set an item to the cache with given key and replace any existing item. If expiredDuration is -1 (NO_EXPIRATION) then the item will never be expired.
func (cache *Cache) Set(key string, value interface{}, expiredDuration time.Duration) {
	var expiredTime *time.Time
	if expiredDuration > -1 {
		t := time.Now().Add(expiredDuration)
		expiredTime = &t
	}

	bucket, _ := cache.getBucketWithDjbHasher(key)

	cache.Lock()
	bucket.Items[key] = &Item{
		Value:       value,
		ExpiredTime: expiredTime,
	}
	cache.Unlock()
}

// Delete an item from the cache by key and do nothing if the key is not found
func (cache *Cache) Delete(key string) {
	bucket, _ := cache.getBucketWithDjbHasher(key)
	cache.Lock()
	delete(bucket.Items, key)
	cache.Unlock()
}

// Check an item is expired or not
func IsExpired(item *Item) bool {
	if item.ExpiredTime == nil {
		return false
	}
	return item.ExpiredTime.Before(time.Now())
}

// Manually delete expired items in all buckets (sharded maps) of the cache
func (cache *Cache) DeleteExpiredItems() {
	buckets := cache.buckets
	for _, bucket := range buckets {
		cache.Lock()
		for key, item := range bucket.Items {
			if IsExpired(item) {
				delete(bucket.Items, key)
			}
		}
		cache.Unlock()
	}
}

// cleaner will run and delete all expired items at time-interval (CLEANUP_INTERVAL = 60s)
func (cache *Cache) runExpiredItemCleaner() {
	ticker := time.NewTicker(time.Second * CLEANUP_INTERVAL).C
	go func() {
		for {
			select {
			case <-ticker:
				cache.DeleteExpiredItems()
			}
		}
	}()
}

// Return all items (may include expired items that has not been deleted by cleaner yet) in the cache
func (cache *Cache) GetAllItemsCache() map[string]*Item {
	allItems := make(map[string]*Item)
	buckets := cache.buckets
	for _, bucket := range buckets {
		cache.RLock()
		for key, itemInBucket := range bucket.Items {
			allItems[key] = itemInBucket
		}
		cache.RUnlock()
	}
	return allItems
}

// Return count of all items (may include expired items that has not been deleted by cleaner yet) in the cache
func (cache *Cache) CountItemsCache() int {
	buckets := cache.buckets
	numItems := 0
	for _, bucket := range buckets {
		cache.RLock()
		numItems += len(bucket.Items)
		cache.RUnlock()
	}
	return numItems
}

// Delete all items in the cache
func (cache *Cache) FlushAll() {
	buckets := cache.buckets
	for _, bucket := range buckets {
		cache.Lock()
		bucket.Items = map[string]*Item{}
		cache.Unlock()
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
}

// Save data of the cache to file on disk. File's name is cache's name
func (cache *Cache) saveCacheToFile() {
	buckets := cache.buckets
	cacheFile, err := os.Create(cache.cacheName + ".txt")
	defer cacheFile.Close()
	checkError(err)
	dataEncoder := gob.NewEncoder(cacheFile)

	dataEncoder.Encode(buckets)
}

// Persist data of the cache to file on disk. File's name is cache's name. It's useful for reloading those data again (ex: when restart server ...)
func (cache *Cache) PersistCacheToDisk() {
	cache.saveCacheToFile()
}

// Execute backup (aka: save content of the cache to file) at given time-interval
func (cache *Cache) BackUpCacheToDiskInterval(backUpInterval time.Duration) {
	ticker := time.NewTicker(backUpInterval).C
	go func() {
		for {
			select {
			case <-ticker:
				cache.saveCacheToFile()
			}
		}
	}()
}

// Load all items that were persisted to file on disk. File's name is cache's name.
// An important note, to load successfully content from file to buckets of the cache, count of cache's buckets should be equal count of buckets that was loaded from file
func (cache *Cache) LoadCacheFromDisk() {
	cacheFile, err := os.Open(cache.cacheName + ".txt")
	defer cacheFile.Close()

	checkError(err)

	dataDecoder := gob.NewDecoder(cacheFile)
	var bucketsOnFile []*ConcurrentMap
	err = dataDecoder.Decode(&bucketsOnFile)

	checkError(err)

	if len(cache.buckets) == len(bucketsOnFile) {
		cache.buckets = append(cache.buckets, bucketsOnFile...)
	} else {
		fmt.Println("Mismatched len of 2 buckets (on file vs on initiated cache) => cannot load cache")
	}
}
