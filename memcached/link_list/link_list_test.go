package link_list

import (
	"testing"
	"unsafe"
)

func TestNewValueAndNodeGetters(t *testing.T) {
	data := []byte("hello")
	ptr := unsafe.Pointer(&data[0])

	value := NewValue(ptr, "key1")
	node := (&DLL{}).Inset(value)

	if node.GetKey() != "key1" {
		t.Fatalf("expected key %q, got %q", "key1", node.GetKey())
	}

	if node.GetPointer() != ptr {
		t.Fatalf("expected pointer %v, got %v", ptr, node.GetPointer())
	}
}

func TestInsetAddsNodesToFront(t *testing.T) {
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))
	n3 := dll.Inset(NewValue(nil, "third"))

	if dll.root != n3 {
		t.Fatal("expected root to be third inserted node")
	}

	if dll.last != n1 {
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
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))
	n3 := dll.Inset(NewValue(nil, "third"))

	dll.Delete(n3)

	if dll.root != n2 {
		t.Fatal("expected root to become second node")
	}

	if dll.last != n1 {
		t.Fatal("expected last to stay first node")
	}

	if n2.left != nil {
		t.Fatal("expected new root.left to be nil")
	}
}

func TestDeleteMiddleNode(t *testing.T) {
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))
	n3 := dll.Inset(NewValue(nil, "third"))

	dll.Delete(n2)

	if dll.root != n3 {
		t.Fatal("expected root to stay third node")
	}

	if dll.last != n1 {
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
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))
	n3 := dll.Inset(NewValue(nil, "third"))

	dll.Delete(n1)

	if dll.root != n3 {
		t.Fatal("expected root to stay third node")
	}

	if dll.last != n2 {
		t.Fatal("expected last to become second node")
	}

	if n2.right != nil {
		t.Fatal("expected new last.right to be nil")
	}
}

func TestDeleteNilDoesNothing(t *testing.T) {
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))

	dll.Delete(nil)

	if dll.root != n1 {
		t.Fatal("expected root to stay unchanged")
	}

	if dll.last != n1 {
		t.Fatal("expected last to stay unchanged")
	}
}

func TestRemoveEmptyListDoesNothing(t *testing.T) {
	var dll DLL

	dll.Remove()

	if dll.root != nil {
		t.Fatal("expected root to be nil")
	}

	if dll.last != nil {
		t.Fatal("expected last to be nil")
	}
}

func TestRemoveSingleNode(t *testing.T) {
	var dll DLL

	dll.Inset(NewValue(nil, "only"))

	dll.Remove()

	if dll.root != nil {
		t.Fatal("expected root to be nil")
	}

	if dll.last != nil {
		t.Fatal("expected last to be nil")
	}
}

func TestRemoveLastNode(t *testing.T) {
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))
	n3 := dll.Inset(NewValue(nil, "third"))

	dll.Remove()

	if dll.root != n3 {
		t.Fatal("expected root to stay third node")
	}

	if dll.last != n2 {
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
	var dll DLL

	if dll.LastNode() != nil {
		t.Fatal("expected last node of empty list to be nil")
	}

	n1 := dll.Inset(NewValue(nil, "first"))

	if dll.LastNode() != n1 {
		t.Fatal("expected last node to be first node")
	}
}

func TestGetLRUFreeSpace(t *testing.T) {
	var dll DLL

	data := make([]byte, 16)
	copy(data, []byte("abcdefghijklmnop"))

	node := dll.Inset(NewValue(unsafe.Pointer(&data[0]), "key1"))

	freeSpace := dll.GetLRUFreeSpace(node, 4)

	if string(freeSpace) != "abcd" {
		t.Fatalf("expected %q, got %q", "abcd", string(freeSpace))
	}

	freeSpace[0] = 'z'

	if data[0] != 'z' {
		t.Fatal("expected returned slice to point to original memory")
	}
}

func TestReadMovesMiddleNodeToFront(t *testing.T) {
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))
	n3 := dll.Inset(NewValue(nil, "third"))

	dll.Read(n2)

	if dll.root != n2 {
		t.Fatal("expected second node to become root")
	}

	if dll.last != n1 {
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
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))

	dll.Read(n2)

	if dll.root != n2 {
		t.Fatal("expected root to stay second node")
	}

	if dll.last != n1 {
		t.Fatal("expected last to stay first node")
	}
}

func TestReadLastNodeDoesNotPanic(t *testing.T) {
	var dll DLL

	n1 := dll.Inset(NewValue(nil, "first"))
	n2 := dll.Inset(NewValue(nil, "second"))
	n3 := dll.Inset(NewValue(nil, "third"))

	dll.Read(n1)

	if dll.root != n1 {
		t.Fatal("expected first node to become root")
	}

	if dll.last != n2 {
		t.Fatal("expected last to become second node")
	}

	if n1.left != nil {
		t.Fatal("expected new root.left to be nil")
	}

	if n1.right != n3 {
		t.Fatal("expected first.right to be third")
	}
}
