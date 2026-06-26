package link_list

import (
	"testing"
	"unsafe"
)

func TestNewValueAndNodeGetters(t *testing.T) {
	data := []byte("hello")
	ptr := unsafe.Pointer(&data[0])

	value := NewValue(ptr, "key1")
	lru := NewShardedLRU()
	node := lru.Insert(value)

	if node.GetKey() != "key1" {
		t.Fatalf("expected key %q, got %q", "key1", node.GetKey())
	}

	if node.GetPointer() != ptr {
		t.Fatalf("expected pointer %v, got %v", ptr, node.GetPointer())
	}
}

func TestInsertAddsNodesToFrontInSameShard(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))
	n2 := lru.Insert(NewValue(nil, "second"))
	n3 := lru.Insert(NewValue(nil, "third"))

	shard := &lru.shards[0]

	if shard.root != n3 {
		t.Fatal("expected root to be third inserted node")
	}

	if shard.last != n1 {
		t.Fatal("expected last to be first inserted node")
	}

	if n3.right != n2 {
		t.Fatal("expected third.right to be second")
	}

	if n2.right != n1 {
		t.Fatal("expected second.right to be first")
	}

	if n1.left != n2 {
		t.Fatal("expected first.left to be second")
	}
}

func TestDeleteRootNode(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))
	n2 := lru.Insert(NewValue(nil, "second"))
	n3 := lru.Insert(NewValue(nil, "third"))

	lru.Delete(n3)

	shard := &lru.shards[0]

	if shard.root != n2 {
		t.Fatal("expected root to become second node")
	}

	if shard.last != n1 {
		t.Fatal("expected last to stay first node")
	}

	if n2.left != nil {
		t.Fatal("expected new root.left to be nil")
	}
}

func TestDeleteMiddleNode(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))
	n2 := lru.Insert(NewValue(nil, "second"))
	n3 := lru.Insert(NewValue(nil, "third"))

	lru.Delete(n2)

	shard := &lru.shards[0]

	if shard.root != n3 {
		t.Fatal("expected root to stay third node")
	}

	if shard.last != n1 {
		t.Fatal("expected last to stay first node")
	}

	if n3.right != n1 {
		t.Fatal("expected third.right to point to first")
	}

	if n1.left != n3 {
		t.Fatal("expected first.left to point to third")
	}

	if n2.left != nil || n2.right != nil {
		t.Fatal("expected deleted node links to be nil")
	}
}

func TestDeleteLastNode(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))
	n2 := lru.Insert(NewValue(nil, "second"))
	n3 := lru.Insert(NewValue(nil, "third"))

	lru.Delete(n1)

	shard := &lru.shards[0]

	if shard.root != n3 {
		t.Fatal("expected root to stay third node")
	}

	if shard.last != n2 {
		t.Fatal("expected last to become second node")
	}

	if n2.right != nil {
		t.Fatal("expected new last.right to be nil")
	}
}

func TestDeleteNilDoesNothing(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))

	lru.Delete(nil)

	shard := &lru.shards[0]

	if shard.root != n1 {
		t.Fatal("expected root to stay unchanged")
	}

	if shard.last != n1 {
		t.Fatal("expected last to stay unchanged")
	}
}

func TestPopLastFreeSpaceEmptyListDoesNothing(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	block, key, ok := lru.PopLastFreeSpace(4)

	if ok {
		t.Fatal("expected ok to be false")
	}

	if block != nil {
		t.Fatal("expected block to be nil")
	}

	if key != "" {
		t.Fatal("expected key to be empty")
	}
}

func TestPopLastFreeSpaceSingleNode(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	data := make([]byte, 16)
	copy(data, []byte("abcdefghijklmnop"))

	lru.Insert(NewValue(unsafe.Pointer(&data[0]), "only"))

	block, key, ok := lru.PopLastFreeSpace(4)

	if !ok {
		t.Fatal("expected ok to be true")
	}

	if key != "only" {
		t.Fatalf("expected key %q, got %q", "only", key)
	}

	if string(block) != "abcd" {
		t.Fatalf("expected block %q, got %q", "abcd", string(block))
	}

	shard := &lru.shards[0]

	if shard.root != nil {
		t.Fatal("expected root to be nil")
	}

	if shard.last != nil {
		t.Fatal("expected last to be nil")
	}
}

func TestPopLastFreeSpaceRemovesLastNode(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	data1 := make([]byte, 16)
	data2 := make([]byte, 16)
	data3 := make([]byte, 16)

	copy(data1, []byte("1111222233334444"))
	copy(data2, []byte("aaaabbbbccccdddd"))
	copy(data3, []byte("xxxxxxxxyyyyyyyy"))

	n1 := lru.Insert(NewValue(unsafe.Pointer(&data1[0]), "first"))
	n2 := lru.Insert(NewValue(unsafe.Pointer(&data2[0]), "second"))
	n3 := lru.Insert(NewValue(unsafe.Pointer(&data3[0]), "third"))

	block, key, ok := lru.PopLastFreeSpace(4)

	if !ok {
		t.Fatal("expected ok to be true")
	}

	if key != "first" {
		t.Fatalf("expected key %q, got %q", "first", key)
	}

	if string(block) != "1111" {
		t.Fatalf("expected block %q, got %q", "1111", string(block))
	}

	shard := &lru.shards[0]

	if shard.root != n3 {
		t.Fatal("expected root to stay third node")
	}

	if shard.last != n2 {
		t.Fatal("expected last to become second node")
	}

	if n2.right != nil {
		t.Fatal("expected new last.right to be nil")
	}

	if n1.left != nil || n1.right != nil {
		t.Fatal("expected removed node to be detached")
	}
}

func TestLastNode(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	if lru.LastNode("any") != nil {
		t.Fatal("expected last node of empty list to be nil")
	}

	n1 := lru.Insert(NewValue(nil, "first"))

	if lru.LastNode("first") != n1 {
		t.Fatal("expected last node to be first node")
	}
}

func TestGetLRUFreeSpace(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	data := make([]byte, 16)
	copy(data, []byte("abcdefghijklmnop"))

	node := lru.Insert(NewValue(unsafe.Pointer(&data[0]), "key1"))

	freeSpace := lru.GetLRUFreeSpace(node, 4)

	if string(freeSpace) != "abcd" {
		t.Fatalf("expected %q, got %q", "abcd", string(freeSpace))
	}

	freeSpace[0] = 'z'

	if data[0] != 'z' {
		t.Fatal("expected returned slice to point to original memory")
	}
}

func TestReadMovesMiddleNodeToFront(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))
	n2 := lru.Insert(NewValue(nil, "second"))
	n3 := lru.Insert(NewValue(nil, "third"))

	lru.Read(n2)

	shard := &lru.shards[0]

	if shard.root != n2 {
		t.Fatal("expected second node to become root")
	}

	if shard.last != n1 {
		t.Fatal("expected last to stay first node")
	}

	if n2.left != nil {
		t.Fatal("expected new root.left to be nil")
	}

	if n2.right != n3 {
		t.Fatal("expected second.right to be third")
	}

	if n3.left != n2 {
		t.Fatal("expected third.left to be second")
	}
}

func TestReadRootDoesNothing(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))
	n2 := lru.Insert(NewValue(nil, "second"))

	lru.Read(n2)

	shard := &lru.shards[0]

	if shard.root != n2 {
		t.Fatal("expected root to stay second node")
	}

	if shard.last != n1 {
		t.Fatal("expected last to stay first node")
	}
}

func TestReadLastNodeDoesNotPanic(t *testing.T) {
	lru := NewShardedLRUWithShardCount(1)

	n1 := lru.Insert(NewValue(nil, "first"))
	n2 := lru.Insert(NewValue(nil, "second"))
	n3 := lru.Insert(NewValue(nil, "third"))

	lru.Read(n1)

	shard := &lru.shards[0]

	if shard.root != n1 {
		t.Fatal("expected first node to become root")
	}

	if shard.last != n2 {
		t.Fatal("expected last to become second node")
	}

	if n1.left != nil {
		t.Fatal("expected new root.left to be nil")
	}

	if n1.right != n3 {
		t.Fatal("expected first.right to be third")
	}
}
