package memory_allocator

import (
	"hash/fnv"
	"sync"
)

const storeShardCount = 64

type storeShard struct {
	sync.RWMutex
	items map[string]Key
}

type ShardedStore struct {
	shards [storeShardCount]storeShard
}

func NewShardedStore() *ShardedStore {
	store := &ShardedStore{}

	for i := range store.shards {
		store.shards[i].items = make(map[string]Key)
	}

	return store
}

func (s *ShardedStore) shard(key string) *storeShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return &s.shards[int(h.Sum32())%storeShardCount]
}

func (s *ShardedStore) Store(key string, value Key) {
	shard := s.shard(key)

	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

func (s *ShardedStore) Load(key string) (Key, bool) {
	shard := s.shard(key)

	shard.RLock()
	value, ok := shard.items[key]
	shard.RUnlock()

	return value, ok
}

func (s *ShardedStore) Delete(key string) {
	shard := s.shard(key)

	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}
