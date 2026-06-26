package link_list

import (
	"fmt"
	"sync"
	"unsafe"
)

// DLL represents a doubly linked list with root, last node, and a read-write lock.
type DLL struct {
	root         *Node // Pointer to the first (root) node in the list.
	last         *Node // Pointer to the last node in the list.
	sync.RWMutex       // Read-Write lock to ensure safe concurrent access.
}

// Node represents a node in the doubly linked list.
type Node struct {
	left  *Node // Pointer to the previous node in the list.
	right *Node // Pointer to the next node in the list.
	value Value // Value stored in the node.
}

// GetKey returns the key of the value stored in the node.
func (n *Node) GetKey() string {
	return n.value.key // Return the key of the node's value.
}

func (n *Node) GetPointer() unsafe.Pointer {
	return n.value.pointer
}

// Value represents the data in a node, containing a pointer and a key.
type Value struct {
	pointer unsafe.Pointer // Pointer to the actual data, storing memory location.
	key     string         // Key used to link to the hash table.
}

// NewValue creates a new Value with the provided pointer and key.
func NewValue(pointer unsafe.Pointer, key string) Value {
	return Value{
		pointer: pointer, // Assign pointer to memory location.
		key:     key,     // Assign key to link the data.
	}
}

// GetLRUFreeSpace returns a slice of bytes representing the free space in the LRU node.
// It locks the list to avoid concurrent modification during the operation.
func (dll *DLL) GetLRUFreeSpace(lru *Node, blockSize int) []byte {
	dll.Lock()         // Lock the DLL to ensure thread-safe access.
	defer dll.Unlock() // Unlock the DLL after the function completes.

	ptr := lru.value.pointer // Get the pointer to the LRU node's data.
	// Return a slice of bytes with the size of blockSize starting from the pointer.
	return unsafe.Slice((*byte)(ptr), blockSize)
}

// Inset adds a new node with the given value to the doubly linked list.
// It locks the DLL to prevent race conditions while modifying the list.
func (dll *DLL) Inset(value Value) *Node {
	dll.Lock()         // Lock the DLL to ensure thread-safe modifications.
	defer dll.Unlock() // Unlock the DLL after the operation.

	// Create a new node and get the current root of the list.
	newNode, root := &Node{}, dll.root

	// If the list is not empty, insert the new node before the root.
	if dll.root != nil {
		newNode.right = root // New node's right points to the current root.
		root.left = newNode  // Current root's left points to the new node.
	} else { // If the list is empty, the new node becomes both the root and the last node.
		dll.last = newNode
	}

	// Set the value of the new node.
	newNode.value = value
	// Update the root to the new node.
	dll.root = newNode

	return newNode // Return the newly inserted node.
}

// Delete removes a given node from the doubly linked list.
// It locks the DLL to prevent race conditions during deletion.
func (dll *DLL) Delete(node *Node) {
	if node == nil {
		return // Return if the node is nil (nothing to delete).
	}

	dll.Lock()         // Lock the DLL to ensure safe modification.
	defer dll.Unlock() // Unlock the DLL after the operation.

	// If the node has a left neighbor, update its right pointer to skip the node.
	if node.left != nil {
		node.left.right = node.right
	} else { // If the node is the root, update the root pointer.
		dll.root = node.right
	}

	// If the node has a right neighbor, update its left pointer to skip the node.
	if node.right != nil {
		node.right.left = node.left
	} else { // If the node is the last node, update the last pointer.
		dll.last = node.left
	}

	node.left = nil
	node.right = nil
}

// Remove deletes the last node from the doubly linked list.
// It locks the DLL to ensure safe modification of the list.
func (dll *DLL) Remove() {
	dll.Lock()
	defer dll.Unlock()

	if dll.last == nil {
		return
	}

	removed := dll.last

	if dll.last.left == nil {
		dll.last = nil
		dll.root = nil

		removed.left = nil
		removed.right = nil
		return
	}

	dll.last = dll.last.left
	dll.last.right = nil

	removed.left = nil
	removed.right = nil
}

// LastNode returns the last node in the doubly linked list.
func (dll *DLL) LastNode() *Node {
	return dll.last // Return the last node in the list.
}

// Read moves a node to the front of the doubly linked list (making it the new root).
// It locks the DLL to prevent concurrent modification.
func (dll *DLL) Read(node *Node) {
	if node == nil {
		return
	}

	dll.Lock()
	defer dll.Unlock()

	if node == dll.root {
		return
	}

	if node.left != nil {
		node.left.right = node.right
	}

	if node.right != nil {
		node.right.left = node.left
	} else {
		dll.last = node.left
	}

	node.left = nil
	node.right = dll.root

	if dll.root != nil {
		dll.root.left = node
	}

	dll.root = node

	if dll.last == nil {
		dll.last = node
	}
}

// ReadAll traverses the entire doubly linked list from root to last, printing each node's value.
func (dll *DLL) ReadAll() {
	// Traverse the list starting from the root.
	for root := dll.root; root != nil; root = root.right {
		fmt.Println(root.value) // Print the value of the current node.
	}
}

// ReadBack traverses the entire doubly linked list from last to root, printing each node's value.
func (dll *DLL) ReadBack() {
	// Traverse the list starting from the last node.
	for current := dll.last; current != nil; current = current.left {
		fmt.Println(current.value) // Print the value of the current node.
	}
}
