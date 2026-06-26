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

func (s *SlabManager) Worker() {
	for payload := range s.JobCh {
		s.Process(payload)
	}
}

func (s *SlabManager) SetOperationFn(payload Transfer) {
	_, keySize, ttl, bodySize := decoder.Decode(payload.payload)

	bodyOffset := constants.HeaderSize + keySize
	key := string(payload.payload[constants.HeaderSize:bodyOffset])

	if oldValueObject, isFound := s.store.Load(key); isFound {
		oldValue := oldValueObject.(Key)

		s.lru[oldValue.slabIndex].Delete(oldValue.pointer)

		memoryPointer := oldValue.pointer.GetPointer()
		s.slabs[oldValue.slabIndex].FreeMemory(memoryPointer)
	}

	node := s.lru[payload.index].Insert(
		link_list.NewValue(unsafe.Pointer(&payload.payload[0]), key),
	)

	s.store.Store(key, Key{
		field:     payload.payload[bodyOffset : bodyOffset+bodySize],
		ttl:       TLLParser(ttl),
		pointer:   node,
		slabIndex: payload.index,
	})

	if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ObjectInserted)); err != nil {
		log.Println(err)
	}
}

func (s *SlabManager) GetOperationFn(payload Transfer) {
	_, keySize, _, _ := decoder.Decode(payload.payload)
	key := string(payload.payload[constants.HeaderSize : constants.HeaderSize+keySize])

	s.slabs[payload.index].FreeMemory(unsafe.Pointer(&payload.payload[0]))

	valueObject, isFound := s.store.Load(key)
	if !isFound {
		if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ErrObjectNotFound)); err != nil {
			log.Println(err)
		}
		return
	}

	value := valueObject.(Key)

	if !value.ttl.IsZero() && time.Now().After(value.ttl) {
		s.store.Delete(key)
		s.lru[value.slabIndex].Delete(value.pointer)

		memoryPointer := value.pointer.GetPointer()
		s.slabs[value.slabIndex].FreeMemory(memoryPointer)

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

	valueObject, isFound := s.store.Load(key)
	if !isFound {
		if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ErrObjectNotFound)); err != nil {
			log.Println(err)
		}
		return
	}

	value := valueObject.(Key)

	s.store.Delete(key)
	s.lru[value.slabIndex].Delete(value.pointer)

	memoryPointer := value.pointer.GetPointer()
	s.slabs[value.slabIndex].FreeMemory(memoryPointer)

	if _, err := payload.conn.Write(decoder.EncodeResponse(constants.ObjectDeleted)); err != nil {
		log.Println(err)
	}
}
