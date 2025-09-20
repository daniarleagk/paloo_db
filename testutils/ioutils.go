package testutils

import (
	"fmt"
	"sync"

	"github.com/daniarleagk/paloo_db/io"
	"github.com/daniarleagk/paloo_db/utils"
)

// for testing purposes
type MapStorage[I io.DbObjectId, O any] struct {
	storage    map[I]O // map of pages by id
	storageId  string  // storage identifier
	rwMutex    sync.RWMutex
	nextIdFunc func() (I, error)
}

func NewMapStorage[I io.DbObjectId, O any](storageId string, nextIdFunc func() (I, error)) *MapStorage[I, O] {
	return &MapStorage[I, O]{
		storage:    make(map[I]O),
		rwMutex:    sync.RWMutex{},
		nextIdFunc: nextIdFunc, // start with id 0
		storageId:  storageId,
	}
}

func (m *MapStorage[I, O]) StorageId() string {
	return m.storageId
}

func (m *MapStorage[I, O]) Read(id I) (O, error) {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	if pPage, exists := m.storage[id]; exists {
		return pPage, nil
	}
	return utils.Zero[O](), fmt.Errorf("page with id %v not found", id)
}

func (m *MapStorage[I, O]) Write(id I, b O) error {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.storage[id] = b
	return nil
}

func (m *MapStorage[I, O]) Reserve() (I, error) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	id, err := m.nextIdFunc()
	if err != nil {
		return utils.Zero[I](), fmt.Errorf("failed to reserve id: %v", err)
	}
	return id, nil
}

func (m *MapStorage[I, O]) Delete(id I) error {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	if _, exists := m.storage[id]; !exists {
		return fmt.Errorf("page with id %v not found", id)
	}
	delete(m.storage, id)
	return nil
}

func (m *MapStorage[I, O]) Close() error {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.storage = make(map[I]O) // clear storage
	return nil
}
