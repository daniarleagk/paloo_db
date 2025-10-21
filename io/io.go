// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory
package io

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sync"

	"github.com/daniarleagk/paloo_db/utils"
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

// Block interface for block oriented storage
type TmpBlock interface {
	Append(data []byte) bool // appends data to the block, returns false if not enough space
	ToByteArray() []byte     // returns the block as byte array
	All() iter.Seq[[]byte]   // returns a function that yields all records in the block
	Reset() error            // resets the block
	Bootstrap()              // initializes the block from the byte array
	Size() int               // returns the current size of the block
	SetBytes(data []byte)    // sets the byte array for the block
	GetBytes() []byte        // gets the byte array for the block
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

// TempFileWriter interface for writing temporary files
type TempFileWriter[T any] interface {
	WriteSeq(recordSeq iter.Seq[T]) error
	Flush() error
	Close() error
}

// TempFileReader interface for reading temporary files
type TempFileReader[T any] interface {
	All() iter.Seq2[T, error]
	Close() error
}

// BlockTempFileWriter simple wrapper temp file writer streamed and buffered
// assumption is that record fits into block/buffer
type BlockTempFileWriter[T any, B TmpBlock, S utils.Serializer[T]] struct {
	file        *os.File
	bufferSize  int
	serialize   S
	bufferBlock B
	recordSize  int
}

func NewFixedSizeTempFileWriter[T any](file *os.File, bufferSize int, recordSize int, serialize utils.Serializer[T]) *BlockTempFileWriter[T, *FixedSizeRecordBlock, utils.Serializer[T]] {
	bufferBlock := NewFixedSizeRecordBlock(bufferSize, recordSize)
	return &BlockTempFileWriter[T, *FixedSizeRecordBlock, utils.Serializer[T]]{
		file:        file,
		bufferSize:  bufferSize,
		serialize:   serialize,
		bufferBlock: &bufferBlock,
		recordSize:  recordSize,
	}
}

func (w *BlockTempFileWriter[T, B, S]) WriteSeq(recordSeq iter.Seq[T]) error {
	buf := make([]byte, w.recordSize) // allocate buffer
	for record := range recordSeq {
		err := w.serialize.Serialize(record, buf)
		if err != nil {
			return err
		}
		ok := w.bufferBlock.Append(buf)
		if !ok {
			if err := w.Flush(); err != nil {
				return err
			}
			w.bufferBlock.Reset()
			w.bufferBlock.Append(buf)
		}
	}
	// flush remaining data
	if w.bufferBlock.Size() > 0 {
		if err := w.Flush(); err != nil {
			return err
		}
		w.bufferBlock.Reset()
	}
	return nil
}

func (w *BlockTempFileWriter[T, B, S]) Flush() error {
	// currently don´t use fsync
	// FIXME for WAL writer
	_, err := w.file.Write(w.bufferBlock.ToByteArray())
	return err
}

func (w *BlockTempFileWriter[T, B, S]) Close() error {
	// flush remaining data
	if err := w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// TempFileReader simple wrapper temp file reader buffered
type BlockTempFileReader[T any, B TmpBlock, D utils.Deserializer[T]] struct {
	file        *os.File
	deserialize D
	bufferSize  int
	bufferBlock B
}

func NewFixedLenTempFileReader[T any](file *os.File, bufferSize int, recordSize int, deserialize utils.Deserializer[T]) *BlockTempFileReader[T, *FixedSizeRecordBlock, utils.Deserializer[T]] {
	block := NewFixedSizeRecordBlock(bufferSize, recordSize)
	return &BlockTempFileReader[T, *FixedSizeRecordBlock, utils.Deserializer[T]]{
		file:        file,
		bufferSize:  bufferSize,
		bufferBlock: &block,
		deserialize: deserialize,
	}
}

func NewVarLenTempFileReader[T any](file *os.File, bufferSize int, deserialize utils.Deserializer[T]) *BlockTempFileReader[T, *VarLenRecordBlock, utils.Deserializer[T]] {
	block := NewVarLenRecordBlock(bufferSize)
	return &BlockTempFileReader[T, *VarLenRecordBlock, utils.Deserializer[T]]{
		file:        file,
		bufferSize:  bufferSize,
		bufferBlock: &block,
		deserialize: deserialize,
	}
}

func (r *BlockTempFileReader[T, B, D]) All() iter.Seq2[T, error] {
	f := func(yield func(T, error) bool) {
	outerLoop:
		for {
			_, err := r.file.Read(r.bufferBlock.GetBytes()) // initial read
			if errors.Is(err, io.EOF) {
				r.file.Close()
				break
			}
			if err != nil {
				yield(utils.Zero[T](), err)
				break
			}
			r.bufferBlock.Bootstrap()
			for bSlice := range r.bufferBlock.All() {
				record, err := r.deserialize.Deserialize(bSlice)
				if err != nil {
					yield(utils.Zero[T](), err)
					break outerLoop
				}
				if !yield(record, nil) {
					break outerLoop
				}
			}
			r.bufferBlock.Reset()
		}
	}
	return f
}

func (r *BlockTempFileReader[T, B, D]) Close() error {
	return r.file.Close()
}

// FixedSizeRecordBlock is a block of fixed-size records as internal storage helper.
// used e.g. for tempfiles while sorting
type FixedSizeRecordBlock struct {
	data           []byte
	blockSize      uint32
	numRecords     uint32
	currentOffset  uint32
	recordByteSize uint32
	currentIndex   uint32 // for iterator

}

// Default Constructor
func NewFixedSizeRecordBlock(blockSize int, recordByteSize int) FixedSizeRecordBlock {
	return FixedSizeRecordBlock{
		data:           make([]byte, blockSize),
		numRecords:     0,
		blockSize:      uint32(blockSize),
		recordByteSize: uint32(recordByteSize),
		currentOffset:  12, // first 12 bytes are reserved for numRecords, currentOffset and recordByteSize
		currentIndex:   0,
	}
}

func (rb *FixedSizeRecordBlock) Size() int {
	return int(rb.numRecords)
}

// set bytes for the block
func (rb *FixedSizeRecordBlock) SetBytes(data []byte) {
	rb.data = data
}

// get bytes for the block
func (rb *FixedSizeRecordBlock) GetBytes() []byte {
	return rb.data
}

// return ok  or error as bool if successfully appended
func (rb *FixedSizeRecordBlock) Append(data []byte) bool {
	//no space
	if rb.currentOffset+rb.recordByteSize > rb.blockSize {
		return false
	}
	// copy
	copy(rb.data[rb.currentOffset:], data)
	rb.currentOffset += rb.recordByteSize
	rb.numRecords++
	return true
}

// returns data with first bytes the
func (rb *FixedSizeRecordBlock) ToByteArray() []byte {
	binary.BigEndian.PutUint32(rb.data[0:4], rb.numRecords)
	binary.BigEndian.PutUint32(rb.data[4:8], rb.currentOffset)
	binary.BigEndian.PutUint32(rb.data[8:12], rb.recordByteSize)
	return rb.data
}

func (rb *FixedSizeRecordBlock) Bootstrap() {
	rb.numRecords = binary.BigEndian.Uint32(rb.data[0:4])
	rb.currentOffset = binary.BigEndian.Uint32(rb.data[4:8])
	rb.recordByteSize = binary.BigEndian.Uint32(rb.data[8:12])
}

func (rb *FixedSizeRecordBlock) All() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		offset := 12 // first 12 bytes are metadata
		for range int(rb.numRecords) {
			if !yield(rb.data[offset : offset+int(rb.recordByteSize)]) {
				break
			}
			offset += int(rb.recordByteSize)
		}
	}
}

func (rb *FixedSizeRecordBlock) Next() ([]byte, bool, error) {
	if rb.currentIndex < rb.numRecords {
		offset := 12 + rb.currentIndex*rb.recordByteSize
		rb.currentIndex++
		return rb.data[offset : offset+rb.recordByteSize], true, nil
	}
	return nil, false, nil
}

func (rb *FixedSizeRecordBlock) Reset() error {
	rb.numRecords = 0
	rb.currentOffset = 12
	return nil
}

func (rb *FixedSizeRecordBlock) String() string {
	return fmt.Sprintf("FixedSizeRecordBlock{numRecords: %d, currentOffset: %d, recordByteSize: %d, blockSize: %d}",
		rb.numRecords, rb.currentOffset, rb.recordByteSize, rb.blockSize)
}

// VarLenRecordBlock is a block of variable-length records as internal storage helper.
// used e.g. for tempfiles while sorting
// first 8 bytes are reserved for blockSize (4 bytes) and currentOffset (4 bytes)
// max record size is 65535 bytes (2 bytes for length prefix) 65KB
type VarLenRecordBlock struct {
	data          []byte
	numRecords    uint32
	blockSize     uint32
	currentOffset uint32
}

func NewVarLenRecordBlock(blockSize int) VarLenRecordBlock {
	return VarLenRecordBlock{
		data:          make([]byte, blockSize),
		numRecords:    0,
		blockSize:     uint32(blockSize),
		currentOffset: 8, // first 8 bytes are reserved for blockSize and currentOffset
	}
}

func (rb *VarLenRecordBlock) Size() int {
	return int(rb.numRecords)
}

// set bytes for the block
func (rb *VarLenRecordBlock) SetBytes(data []byte) {
	rb.data = data
}

// get bytes for the block
func (rb *VarLenRecordBlock) GetBytes() []byte {
	return rb.data
}

// append now writes length prefixed data into the block
// for length we use 2 bytes uint16
func (rb *VarLenRecordBlock) Append(data []byte) bool {
	dataLen := uint32(len(data))
	//no space
	if rb.currentOffset+dataLen+2 > rb.blockSize {
		return false
	}
	// write length prefix
	binary.BigEndian.PutUint16(rb.data[rb.currentOffset:], uint16(dataLen))
	rb.currentOffset += 2
	// write data
	copy(rb.data[rb.currentOffset:], data)
	rb.currentOffset += dataLen
	rb.numRecords++
	return true
}

// returns data with first bytes the
func (rb *VarLenRecordBlock) ToByteArray() []byte {
	binary.BigEndian.PutUint32(rb.data[0:4], rb.numRecords)
	binary.BigEndian.PutUint32(rb.data[4:8], rb.currentOffset)
	return rb.data
}

func (rb *VarLenRecordBlock) Bootstrap() {
	rb.numRecords = binary.BigEndian.Uint32(rb.data[0:4])
	rb.currentOffset = binary.BigEndian.Uint32(rb.data[4:8])
}

func (rb *VarLenRecordBlock) Reset() error {
	rb.numRecords = 0
	rb.currentOffset = 8
	return nil
}

func (rb *VarLenRecordBlock) All() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		offset := 8 // first 8 bytes are metadata
		for range int(rb.numRecords) {
			// read first two bytes for length
			length := binary.BigEndian.Uint16(rb.data[offset:])
			offset += 2
			lengthInt := int(length)
			if !yield(rb.data[offset : offset+lengthInt]) {
				break
			}
			offset += lengthInt
		}
	}
}
