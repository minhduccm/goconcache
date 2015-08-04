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
	CLEANUP_INTERVAL               = 60
)

type Item struct {
	Value       interface{}
	ExpiredTime *time.Time
}

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

func (cache *Cache) getBucketWithDjbHasher(key string) (*ConcurrentMap, uint32) {
	bucketsCount := (uint32)(len(cache.buckets))
	bucketIndex := djbHasher(cache.seed, key) % bucketsCount
	return cache.buckets[bucketIndex], bucketIndex
}

func (cache *Cache) getBucketWithBuiltInHasher(key string) (*ConcurrentMap, uint) {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	bucketsCount := len(cache.buckets)
	bucketIndex := uint(hasher.Sum32()) % uint(bucketsCount)
	return cache.buckets[bucketIndex], bucketIndex
}

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

func (cache *Cache) Delete(key string) {
	bucket, _ := cache.getBucketWithDjbHasher(key)
	cache.Lock()
	delete(bucket.Items, key)
	cache.Unlock()
}

func IsExpired(item *Item) bool {
	if item.ExpiredTime == nil {
		return false
	}
	return item.ExpiredTime.Before(time.Now())
}

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

func (cache *Cache) saveCacheToFile() {
	buckets := cache.buckets
	cacheFile, err := os.Create(cache.cacheName + ".txt")
	defer cacheFile.Close()
	checkError(err)
	dataEncoder := gob.NewEncoder(cacheFile)

	dataEncoder.Encode(buckets)
}

func (cache *Cache) PersistCacheToDisk() {
	cache.saveCacheToFile()
}

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
