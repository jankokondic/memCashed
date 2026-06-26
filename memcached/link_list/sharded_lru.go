package link_list

import (
	"sync"
	"unsafe"
)

const DefaultShardCount = 256

type ShardedLRU struct {
	shards []LRUShard
}

type LRUShard struct {
	root *Node
	last *Node
	mu   sync.Mutex
}

type Node struct {
	left  *Node
	right *Node
	value Value
}

type Value struct {
	pointer unsafe.Pointer
	key     string
}

func NewValue(pointer unsafe.Pointer, key string) Value {
	return Value{
		pointer: pointer,
		key:     key,
	}
}

func (n *Node) GetKey() string {
	if n == nil {
		return ""
	}

	return n.value.key
}

func (n *Node) GetPointer() unsafe.Pointer {
	if n == nil {
		return nil
	}

	return n.value.pointer
}

func NewShardedLRU() *ShardedLRU {
	return NewShardedLRUWithShardCount(DefaultShardCount)
}

func NewShardedLRUWithShardCount(shardCount int) *ShardedLRU {
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}

	return &ShardedLRU{
		shards: make([]LRUShard, shardCount),
	}
}

func (lru *ShardedLRU) Insert(value Value) *Node {
	shard := lru.getShard(value.key)

	node := &Node{
		value: value,
	}

	shard.mu.Lock()

	oldRoot := shard.root

	if oldRoot != nil {
		node.right = oldRoot
		oldRoot.left = node
	} else {
		shard.last = node
	}

	shard.root = node

	shard.mu.Unlock()

	return node
}

func (lru *ShardedLRU) Delete(node *Node) {
	if node == nil {
		return
	}

	shard := lru.getShard(node.value.key)

	shard.mu.Lock()
	shard.unlinkLocked(node)
	shard.mu.Unlock()
}

func (lru *ShardedLRU) Read(node *Node) {
	if node == nil {
		return
	}

	shard := lru.getShard(node.value.key)

	shard.mu.Lock()

	if node != shard.root {
		shard.unlinkLocked(node)

		oldRoot := shard.root

		node.left = nil
		node.right = oldRoot

		if oldRoot != nil {
			oldRoot.left = node
		} else {
			shard.last = node
		}

		shard.root = node
	}

	shard.mu.Unlock()
}

func (lru *ShardedLRU) PopLastFreeSpace(blockSize int) ([]byte, string, bool) {
	for i := 0; i < len(lru.shards); i++ {
		shard := &lru.shards[i]

		shard.mu.Lock()

		node := shard.last
		if node == nil {
			shard.mu.Unlock()
			continue
		}

		shard.unlinkLocked(node)

		ptr := node.value.pointer
		key := node.value.key

		shard.mu.Unlock()

		if ptr == nil {
			return nil, key, false
		}

		return unsafe.Slice((*byte)(ptr), blockSize), key, true
	}

	return nil, "", false
}

func (lru *ShardedLRU) GetLRUFreeSpace(node *Node, blockSize int) []byte {
	if node == nil {
		return nil
	}

	ptr := node.value.pointer
	if ptr == nil {
		return nil
	}

	return unsafe.Slice((*byte)(ptr), blockSize)
}

func (lru *ShardedLRU) LastNode(key string) *Node {
	shard := lru.getShard(key)

	shard.mu.Lock()
	node := shard.last
	shard.mu.Unlock()

	return node
}

func (lru *ShardedLRU) getShard(key string) *LRUShard {
	index := int(fnv32aString(key)) & (len(lru.shards) - 1)
	return &lru.shards[index]
}

func (shard *LRUShard) unlinkLocked(node *Node) {
	left := node.left
	right := node.right

	if left != nil {
		left.right = right
	} else if shard.root == node {
		shard.root = right
	}

	if right != nil {
		right.left = left
	} else if shard.last == node {
		shard.last = left
	}

	node.left = nil
	node.right = nil
}

func fnv32aString(key string) uint32 {
	var h uint32 = 2166136261

	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}

	return h
}
