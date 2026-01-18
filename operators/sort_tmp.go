// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory of this source tree.

// simple tmp file management
package operators

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/daniar-achakeev/paloo_db/utils"
)

// TempFileWriter interface for writing temporary files
type TempFileWriter[T any] interface {
	Write(it utils.Iterator[T]) error
	WriteSeq(recordSeq iter.Seq[T]) error
	Flush() error
	Close() error
}

// TempFileReader interface for reading temporary files
type TempFileReader[T any] interface {
	All() iter.Seq2[T, error]
	Close() error
}

// Block interface for block oriented storage
type TmpBlock interface {
	utils.Iterator[[]byte]
	Append(data []byte) bool // appends data to the block, returns false if not enough space
	ToByteArray() []byte     // returns the block as byte array
	All() iter.Seq[[]byte]   // returns a function that yields all records in the block
	Reset() error            // resets the block
	Bootstrap()              // initializes the block from the byte array
	Size() int               // returns the current size of the block
	SetBytes(data []byte)    // sets the byte array for the block
	GetBytes() []byte        // gets the byte array for the block
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
	rb.currentIndex = 0
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
	rb.currentIndex = 0
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

func (rb *VarLenRecordBlock) Next() ([]byte, bool, error) {
	// TODO
	return nil, false, nil
}

// BlockTmpFileWriter simple wrapper temp file writer streamed and buffered
// assumption is that record fits into block/buffer
type BlockTmpFileWriter[T any, B TmpBlock, S utils.Serializer[T]] struct {
	file        *os.File
	bufferSize  int
	serialize   S
	bufferBlock B
	recordSize  int
}

func NewFixedSizeTmpFileWriter[T any](file *os.File, bufferSize int, recordSize int, serialize utils.Serializer[T]) *BlockTmpFileWriter[T, *FixedSizeRecordBlock, utils.Serializer[T]] {
	bufferBlock := NewFixedSizeRecordBlock(bufferSize, recordSize)
	return &BlockTmpFileWriter[T, *FixedSizeRecordBlock, utils.Serializer[T]]{
		file:        file,
		bufferSize:  bufferSize,
		serialize:   serialize,
		bufferBlock: &bufferBlock,
		recordSize:  recordSize,
	}
}

func (w *BlockTmpFileWriter[T, B, S]) WriteSeq(recordSeq iter.Seq[T]) error {
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

func (w *BlockTmpFileWriter[T, B, S]) Write(it utils.Iterator[T]) error {
	buf := make([]byte, w.recordSize) // allocate buffer
	for {
		record, ok, err := it.Next()
		if !ok {
			break
		}
		if err != nil {
			return err
		}
		err = w.serialize.Serialize(record, buf)
		if err != nil {
			return err
		}
		ok = w.bufferBlock.Append(buf)
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

func (w *BlockTmpFileWriter[T, B, S]) Flush() error {
	_, err := w.file.Write(w.bufferBlock.ToByteArray())
	return err
}

func (w *BlockTmpFileWriter[T, B, S]) Close() error {
	// flush remaining data
	if err := w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// TempFileReader simple wrapper temp file reader buffered
type BlockTmpFileIterator[T any, B TmpBlock, D utils.Deserializer[T]] struct {
	file        *os.File
	deserialize D
	bufferSize  int
	bufferBlock B
}

func NewFixedSizeTmpFileIterator[T any](file *os.File, bufferSize int, recordSize int, deserialize utils.Deserializer[T]) (*BlockTmpFileIterator[T, *FixedSizeRecordBlock, utils.Deserializer[T]], error) {
	block := NewFixedSizeRecordBlock(bufferSize, recordSize)
	it := &BlockTmpFileIterator[T, *FixedSizeRecordBlock, utils.Deserializer[T]]{
		file:        file,
		bufferSize:  bufferSize,
		bufferBlock: &block,
		deserialize: deserialize,
	}
	_, err := it.readNextBlock()
	if err != nil {
		return nil, err
	}
	return it, nil
}

func NewFixedSizeTmpFileReader[T any](file *os.File, bufferSize int, recordSize int, deserialize utils.Deserializer[T]) *BlockTmpFileIterator[T, *FixedSizeRecordBlock, utils.Deserializer[T]] {
	block := NewFixedSizeRecordBlock(bufferSize, recordSize)
	return &BlockTmpFileIterator[T, *FixedSizeRecordBlock, utils.Deserializer[T]]{
		file:        file,
		bufferSize:  bufferSize,
		bufferBlock: &block,
		deserialize: deserialize,
	}
}

func (r *BlockTmpFileIterator[T, B, D]) All() iter.Seq2[T, error] {
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

func (r *BlockTmpFileIterator[T, B, D]) Next() (T, bool, error) {
	b, ok, err := r.bufferBlock.Next()
	if !ok { // read next block
		ok, err := r.readNextBlock()
		if !ok || err != nil {
			return utils.Zero[T](), false, err
		}
		b, ok, err = r.bufferBlock.Next()
		if !ok || err != nil {
			return utils.Zero[T](), false, err
		}
	}
	if err != nil {
		return utils.Zero[T](), false, err
	}
	t, err := r.deserialize.Deserialize(b)
	if err != nil {
		return utils.Zero[T](), false, err
	}
	return t, true, nil
}

func (r *BlockTmpFileIterator[T, B, D]) readNextBlock() (bool, error) {
	// initial read
	_, err := r.file.Read(r.bufferBlock.GetBytes())
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	r.bufferBlock.Bootstrap()
	return true, nil
}

func (r *BlockTmpFileIterator[T, B, D]) Close() error {
	return r.file.Close()
}
