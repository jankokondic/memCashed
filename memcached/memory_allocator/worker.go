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

func (s *SlabManager) Process(payload Transfer) {
	switch ParseOperation(payload.payload) {
	case constants.SetOperation:
		s.SetOperationFn(payload)
	case constants.GetOperation:
		s.GetOperationFn(payload)
	case constants.DeleteOperation:
		s.DeleteOperationFn(payload)
	default:
		log.Println(constants.ErrOperationIsNotSupported)
	}
}

func (s *SlabManager) SetOperationFn(payload Transfer) {
	_, keySize, ttl, bodySize := decoder.Decode(payload.payload)

	bodyOffset := constants.HeaderSize + keySize
	key := unsafe.String(&payload.payload[constants.HeaderSize], keySize)

	node := s.lru[payload.index].Insert(
		link_list.NewValue(unsafe.Pointer(&payload.payload[0]), key),
	)

	newValue := Key{
		field:     payload.payload[bodyOffset : bodyOffset+bodySize],
		ttl:       TLLParser(ttl),
		pointer:   node,
		slabIndex: payload.index,
	}

	// Atomic swap: whichever goroutine's Store lands second sees the
	// other one's fresh value as "old" — never the same stale entry twice.
	oldValue, hadOld := s.store.Swap(key, newValue)
	if hadOld {
		memoryPointer := oldValue.pointer.GetPointer()
		s.lru[oldValue.slabIndex].Delete(oldValue.pointer)
		s.slabs[oldValue.slabIndex].FreeMemory(memoryPointer)
	}

	if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ObjectInserted)); err != nil {
		log.Println(err)
	}
}

func (s *SlabManager) GetOperationFn(payload Transfer) {
	_, keySize, _, _ := decoder.Decode(payload.payload)
	// Must copy here (not unsafe.String): this payload block is freed
	// on the very next line and could be reused before we're done with key.
	key := string(payload.payload[constants.HeaderSize : constants.HeaderSize+keySize])

	s.slabs[payload.index].FreeMemory(unsafe.Pointer(&payload.payload[0]))

	value, isFound := s.store.Load(key)
	if !isFound {
		if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ErrObjectNotFound)); err != nil {
			log.Println(err)
		}
		return
	}

	if !value.ttl.IsZero() && time.Now().After(value.ttl) {
		// Only the goroutine that wins the CompareAndDelete actually frees
		// the node/memory — a racing SET/DELETE on the same key won't
		// cause a double free.
		if s.store.CompareAndDelete(key, value) {
			memoryPointer := value.pointer.GetPointer()
			s.lru[value.slabIndex].Delete(value.pointer)
			s.slabs[value.slabIndex].FreeMemory(memoryPointer)
		}

		if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ErrTimeExpire)); err != nil {
			log.Println(err)
		}
		return
	}

	s.lru[value.slabIndex].Read(value.pointer)

	if _, err := payload.conn.Write(decoder.EncodeResponse(value.field)); err != nil {
		log.Println(err)
	}
}

func (s *SlabManager) DeleteOperationFn(payload Transfer) {
	_, keySize, _, _ := decoder.Decode(payload.payload)
	key := string(payload.payload[constants.HeaderSize : constants.HeaderSize+keySize])

	s.slabs[payload.index].FreeMemory(unsafe.Pointer(&payload.payload[0]))

	value, isFound := s.store.Load(key)
	if !isFound {
		if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ErrObjectNotFound)); err != nil {
			log.Println(err)
		}
		return
	}

	if s.store.CompareAndDelete(key, value) {
		memoryPointer := value.pointer.GetPointer()
		s.lru[value.slabIndex].Delete(value.pointer)
		s.slabs[value.slabIndex].FreeMemory(memoryPointer)
	}

	if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ObjectDeleted)); err != nil {
		log.Println(err)
	}
}
