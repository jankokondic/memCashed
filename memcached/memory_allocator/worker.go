package memory_allocator

import (
	"log"
	"time"
	"unsafe"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/link_list"
	decoder "github.com/WatchJani/memCashed/memcached/parser"
)

func ParseOperation(payload []byte) byte {
	return payload[0]
}

// Worker listens for transfer jobs and processes them based on the payload command.
func (s *SlabManager) Worker() {
	for payload := range s.JobCh {
		switch ParseOperation(payload.payload) {
		case constants.SetOperation: // Command to store data
			s.SetOperationFn(payload)
		case constants.GetOperation: // Command to get data
			s.GetOperationFn(payload)
		case constants.DeleteOperation: // Command to delete data
			s.DeleteOperationFn(payload)
		default:
			log.Println(constants.ErrOperationIsNotSupported)
		}
	}
}

// test usage
func (s *SlabManager) chooseOperation(payload Transfer) {
	switch ParseOperation(payload.payload) {
	case constants.SetOperation: // Command to store data
		s.SetOperationFn(payload)
	case constants.GetOperation: // Command to get data
		s.GetOperationFn(payload)
	case constants.DeleteOperation: // Command to delete data
		s.DeleteOperationFn(payload)
	default:
		log.Println(constants.ErrOperationIsNotSupported)
	}
}

func (s *SlabManager) SetOperationFn(payload Transfer) {
	_, keySize, ttl, bodySize := decoder.Decode(payload.payload) // Decode the payload

	bodyOffset := constants.HeaderSize + keySize
	key := string(payload.payload[constants.HeaderSize:bodyOffset]) // Extract key from the payload

	// Insert the key into the LRU cache
	node := s.lru[payload.index].Inset(link_list.NewValue(unsafe.Pointer(&payload.payload[0]), key))

	// Store the key-value pair in the store with TTL

	s.store.Store(key, Key{
		field:     payload.payload[bodyOffset : bodyOffset+bodySize],
		ttl:       TLLParser(ttl),
		pointer:   node,
		slabIndex: payload.index,
	})

	if _, err := payload.conn.Write(constants.ObjectInserted); err != nil {
		log.Println(err) // Log any errors that occur while writing to the connection
	}
}

func (s *SlabManager) GetOperationFn(payload Transfer) {
	_, keySize, _, _ := decoder.Decode(payload.payload)                                 // Decode the payload
	key := string(payload.payload[constants.HeaderSize : constants.HeaderSize+keySize]) // Extract key from the payload

	s.slabs[payload.index].freeList.Push(unsafe.Pointer(&payload.payload[0])) //delete our header space

	// Fetch the value from the store
	valueObject, isFound := s.store.Load(key)
	if !isFound {
		if _, err := payload.conn.Write(constants.ErrObjectNotFound); err != nil {
			log.Println(err)
		}
		return
	}

	value := valueObject.(Key)

	// Check if the TTL has expired and delete the object if expired
	if !value.ttl.IsZero() && time.Now().After(value.ttl) {
		s.store.Delete(key)
		s.lru[payload.index].Delete(value.pointer) // Remove the node from LRU
		memoryPointer := value.pointer.GetPointer()
		s.slabs[value.slabIndex].freeList.Push(memoryPointer)

		if _, err := payload.conn.Write(constants.ErrTimeExpire); err != nil {
			log.Println(err)
		}
		return
	}
	s.lru[payload.index].Read(value.pointer)

	// Return the field data if found
	if _, err := payload.conn.Write(value.field); err != nil {
		log.Println(err)
	}
}

func (s *SlabManager) DeleteOperationFn(payload Transfer) {
	_, keySize, _, _ := decoder.Decode(payload.payload)                                 // Decode the payload
	key := string(payload.payload[constants.HeaderSize : constants.HeaderSize+keySize]) // Extract key from the payload

	s.slabs[payload.index].freeList.Push(unsafe.Pointer(&payload.payload[0])) //delete our header space

	// Fetch and delete the object from the store
	valueObject, isFound := s.store.Load(key)
	if !isFound {
		if _, err := payload.conn.Write(constants.ErrObjectNotFound); err != nil {
			log.Println(err)
		}
		return
	}

	value := valueObject.(Key)
	s.store.Delete(key)

	memoryPointer := value.pointer.GetPointer()

	s.lru[payload.index].Delete(value.pointer) // Remove from LRU
	s.slabs[value.slabIndex].freeList.Push(memoryPointer)
	if _, err := payload.conn.Write(constants.ObjectDeleted); err != nil {
		log.Println(err)
	}
}
