// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory
package paloo_db

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const TempFileSuffix string = "tmp"

type DbObjectId interface {
	comparable
	StorageId() string // returns storage id
}

// Storage represents block oriented storage interface
type Storage[I comparable, O any] interface {
	StorageId() string     // returns storage id
	ReserveId() (I, error) // reserves a new id for the object
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

// Block of records
// records are fixed size
// we use this block to store temp files for sorting
type RecordsBlock struct {
	data            []byte
	maxByteCapacity uint32
	numRecords      uint32
	currentOffset   uint32
	recordByteSize  uint32
}

// Default Constructor
func NewRecordsBlock(maxByteCap uint32, recordByteSize uint32) *RecordsBlock {
	return &RecordsBlock{
		data:            make([]byte, maxByteCap),
		numRecords:      0,
		maxByteCapacity: maxByteCap,
		recordByteSize:  recordByteSize,
		currentOffset:   12, // first 12 bytes are reserved for numRecords, currentOffset and recordByteSize
	}
}

// initializes block buffer with fixed byte capacity
// first four bytes is reserved for numRecords
func (rb *RecordsBlock) Init(maxByteCap uint32, recordByteSize uint32) {
	rb.maxByteCapacity = maxByteCap
	rb.recordByteSize = recordByteSize
	rb.data = make([]byte, maxByteCap)
	rb.numRecords = 0
	rb.currentOffset = 12
}

// return ok  or error as bool if successfully appended
func (rb *RecordsBlock) Append(data []byte) bool {
	//no space
	if rb.currentOffset+rb.recordByteSize > rb.maxByteCapacity {
		return false
	}
	// copy
	copy(rb.data[rb.currentOffset:], data)
	rb.currentOffset += rb.recordByteSize
	rb.numRecords++
	return true
}

// returns data with first bytes the
func (rb RecordsBlock) ToByteArray() []byte {
	binary.BigEndian.PutUint32(rb.data[:4], rb.numRecords)
	binary.BigEndian.PutUint32(rb.data[4:8], rb.currentOffset)
	binary.BigEndian.PutUint32(rb.data[8:12], rb.recordByteSize)
	return rb.data
}

func (rb *RecordsBlock) FromByteArray(d []byte) {
	//TODO
	rb.data = make([]byte, 0, rb.maxByteCapacity)
	//
}

// ALL arrays stored in
func (rb RecordsBlock) All() func(yield func([]byte) bool) {
	return func(yield func([]byte) bool) {
		offset := 12
		for range int(rb.numRecords) {
			if !yield(rb.data[offset : offset+int(rb.recordByteSize)]) {
				break
			}
			offset += int(rb.recordByteSize)
		}
	}
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
	return Zero[O](), fmt.Errorf("storage with id %s not found", id.StorageId())
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

func (s *SequenceBlockSingleFileStorage) ReserveId() (PageId, error) {
	// reserve a new id for the object
	s.rwMutex.Lock() // exclusive lock
	defer s.rwMutex.Unlock()
	curSize, err := s.getCurrentSize()
	if err != nil {
		return Zero[PageId](), fmt.Errorf("file not stat available %v", err)
	}
	blockNr := curSize / int64(s.blockSize)
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

// for testing purposes
type MapStorage[I DbObjectId, O any] struct {
	storage    map[I]O // map of pages by id
	storageId  string  // storage identifier
	rwMutex    sync.RWMutex
	nextIdFunc func() (I, error)
}

func NewMapStorage[I DbObjectId, O any](storageId string, nextIdFunc func() (I, error)) *MapStorage[I, O] {
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
	return Zero[O](), fmt.Errorf("page with id %v not found", id)
}

func (m *MapStorage[I, O]) Write(id I, b O) error {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.storage[id] = b
	return nil
}

func (m *MapStorage[I, O]) ReserveId() (I, error) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	id, err := m.nextIdFunc()
	if err != nil {
		return Zero[I](), fmt.Errorf("failed to reserve id: %v", err)
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
