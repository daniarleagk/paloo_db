// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory
package io

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/daniar-achakeev/paloo_db/utils"
)

// DbObjectId is an interface for database object identifiers
type DbObjectId interface {
	comparable
	StorageId() string // returns storage id
}

// Storage represents block oriented storage interface
type Storage[I comparable, O any] interface {
	StorageId() string     // returns storage id
	Reserve() (I, error)   // reserves a new id for the object
	Read(id I) (O, error)  // read object by id
	Write(id I, b O) error // write object by id
	Delete(id I) error     // delete object by id
	Close() error
}

// PageId is a simple tuple of string and int64
type PageId struct {
	storageId string
	blockNr   int64
}

func NewPageId(storageId string, blockNr int64) PageId {
	return PageId{storageId: storageId, blockNr: blockNr}
}

func (p PageId) StorageId() string {
	return p.storageId
}

// implementation of Page interface
type Page struct {
	data []byte
}

func (b *Page) Init(data []byte) {
	b.data = data
}

func (b *Page) InitCapacity(cap int) {
	b.data = make([]byte, 0, cap)
}

func (b *Page) GetData() []byte {
	return b.data
}

// copies the output
func (b *Page) GetBytes(offset int, len int) ([]byte, error) {
	if offset+len > cap(b.data) {
		return nil, fmt.Errorf("not enough space for to write data")
	}
	output := make([]byte, 0, len)
	copy(output, b.data[offset:offset*len])
	return output, nil
}

// copies bytes into internal buffer
func (b *Page) SetBytes(offset int, data []byte) error {
	if offset+len(data) >= cap(b.data) {
		return fmt.Errorf("not enough space for to write data")
	}
	copy(b.data[offset:], data)
	return nil
}

func (b *Page) GetCapacity() int {
	return cap(b.data)
}

// Simple local storage adapter for block oriented storage
type StorageManager[I DbObjectId, O any] struct {
	storages map[string]Storage[I, O]
}

func NewStorageManager[I DbObjectId, O any]() *StorageManager[I, O] {
	return &StorageManager[I, O]{
		storages: make(map[string]Storage[I, O]),
	}
}

func (sm *StorageManager[I, O]) Register(blockStorage Storage[I, O]) error {
	if _, exists := sm.storages[blockStorage.StorageId()]; exists {
		return fmt.Errorf("storage with id %s already exists", blockStorage.StorageId())
	}
	sm.storages[blockStorage.StorageId()] = blockStorage
	return nil
}

func (sm *StorageManager[I, O]) GetStorage(storageId string) (Storage[I, O], error) {
	if storage, exists := sm.storages[storageId]; exists {
		return storage, nil
	}
	return nil, fmt.Errorf("storage with id %s not found", storageId)
}

func (sm *StorageManager[I, O]) Read(id I) (O, error) {
	if storage, exists := sm.storages[id.StorageId()]; exists {
		return storage.Read(id)
	}
	return utils.Zero[O](), fmt.Errorf("storage with id %s not found", id.StorageId())
}

// write block
func (sm *StorageManager[I, O]) Write(id I, b O) error {
	if storage, exists := sm.storages[id.StorageId()]; exists {
		return storage.Write(id, b)
	}
	return fmt.Errorf("storage with id %s not found", id.StorageId())
}

func (sm *StorageManager[I, O]) Close() error {
	for _, storage := range sm.storages {
		if err := storage.Close(); err != nil {
			return fmt.Errorf("error closing storage: %v", err)
		}
	}
	sm.storages = make(map[string]Storage[I, O]) // clear storages
	return nil
}

type SequenceBlockSingleFileStorage struct {
	baseDir   string
	file      *os.File
	fileName  string
	blockSize int
	rwMutex   sync.RWMutex
}

func NewSequenceBlockSingleFileStorage(baseDir string, fileName string, blockSize int) (*SequenceBlockSingleFileStorage, error) {
	fp := filepath.Join(baseDir, fileName)
	// create or open file
	// TODO set O_SYNC
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open or create file")
	}
	return &SequenceBlockSingleFileStorage{
		baseDir:   baseDir,
		file:      f,
		fileName:  fileName,
		blockSize: blockSize,
		rwMutex:   sync.RWMutex{},
	}, nil
}

func (s *SequenceBlockSingleFileStorage) StorageId() string {
	return s.fileName
}

func (s *SequenceBlockSingleFileStorage) Reserve() (PageId, error) {
	// reserve a new id for the object
	// reserve by block appending zero content byte array will be added
	s.rwMutex.Lock() // exclusive lock
	defer s.rwMutex.Unlock()
	curSize, err := s.getCurrentSize()
	if err != nil {
		return utils.Zero[PageId](), fmt.Errorf("file stat not available %v", err)
	}
	blockNr := curSize / int64(s.blockSize)
	// reserve block by writing
	offset := curSize
	if _, err := s.file.WriteAt(make([]byte, s.blockSize), offset); err != nil {
		return utils.Zero[PageId](), fmt.Errorf("write block error %v", err)
	}
	return PageId{storageId: s.fileName, blockNr: blockNr}, nil
}

func (s *SequenceBlockSingleFileStorage) Read(id PageId) (*Page, error) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	offset := int64(s.blockSize) * id.blockNr
	block := Page{}
	data := make([]byte, s.blockSize)
	block.Init(data)
	_, err := s.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("read block error %v", err)
	}
	return &block, nil
}

func (s *SequenceBlockSingleFileStorage) Write(id PageId, b *Page) error {
	s.rwMutex.Lock() // exclusive lock
	defer s.rwMutex.Unlock()
	offset := int64(s.blockSize) * id.blockNr
	_, err := s.file.WriteAt(b.GetData(), offset)
	if err != nil {
		return fmt.Errorf("write block error %v", err)
	}
	return nil
}

func (s *SequenceBlockSingleFileStorage) Delete(id PageId) error {
	// deletion in single file storage is not supported
	return fmt.Errorf("deletion is not supported in single file storage")
}

func (s *SequenceBlockSingleFileStorage) getCurrentSize() (int64, error) {
	i, err := s.file.Stat()
	if err != nil {
		return -1, fmt.Errorf("file not stat available %v", err)
	}
	return i.Size(), nil
}

func (s *SequenceBlockSingleFileStorage) Close() error {
	if err := s.file.Close(); err != nil {
		return err
	}
	return nil
}
