//go:build !race

package paloo_db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

type StorageConfig struct {
	directory   string
	blockSize   int
	storageName string
}

func setTestConfig() *StorageConfig {
	return &StorageConfig{
		directory:   "/home/da/work/tmp/godb",
		blockSize:   32,
		storageName: "tmpTable.tbl",
	}
}

func tearDown(conf *StorageConfig) {
	if _, err := os.Stat(conf.directory); err == nil {
		//
		os.RemoveAll(conf.directory)
	}
}

func createByteSliceBlock(slice []int64) *Page {
	block := &Page{}
	data := make([]byte, 32)
	for idx, d := range slice {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.BigEndian, d)
		if err != nil {
			fmt.Println("binary.Write failed:", err)
		}
		copy(data[idx*8:idx*8+8], buf.Bytes())
	}
	block.Init(data)
	return block
}

func byteSliceBlockToSlice(block *Page) []int64 {
	data := block.GetData()
	result := make([]int64, 0, 4)
	for idx := range 4 {
		slice := data[idx*8 : idx*8+8]
		var value int64
		b := bytes.NewReader(slice)
		binary.Read(b, binary.BigEndian, &value)
		result = append(result, value)
	}
	return result
}

func TestFixedSizeStorage(t *testing.T) {
	conf := setTestConfig()
	// tear down test
	defer tearDown(conf)
	sm := NewStorageManager[PageId, *Page]()
	// create a FixedBlockStorageFile
	fbs, err := NewSequenceBlockSingleFileStorage(conf.directory, conf.storageName, conf.blockSize)
	sm.Register(fbs)
	if err != nil {
		//
		t.Errorf("error during creation %v", err)
	}
	// check if file created
	fp := filepath.Join(conf.directory, conf.storageName)
	if _, err := os.Stat(fp); errors.Is(err, fs.ErrNotExist) {
		// Create the directory with appropriate permissions
		t.Fatalf("file %s does not exists", fp)
	}
	// append empty block
	for bs := range 10 {
		dummyBlock := &Page{}
		dummyBlock.Init(make([]byte, 32, 32))
		//Append
		id, err := fbs.ReserveId()
		if err != nil {
			t.Fatalf("cannot %v append", err)
		}
		//fmt.Println("id", id)
		slice := make([]int64, 0, 4)
		for v := range 4 {
			slice = append(slice, int64((bs*4)+v))
		}
		block := createByteSliceBlock(slice)
		// Write
		if err := fbs.Write(id, block); err != nil {
			t.Fatalf("cannot %v write", err)
		}
		// Read
		rb, err := fbs.Read(id)
		result := byteSliceBlockToSlice(rb)
		// check results

		if equal := slices.Equal(slice, result); !equal {
			t.Fatalf("expected %v got %v", slice, result)
		}
		//fmt.Println("Append/Write/Read", bs, "Result", result)
	}
}

func TestSingleWriterMultipleReaders(t *testing.T) {
	conf := setTestConfig()
	defer tearDown(conf)
	sm := &StorageManager[PageId, *Page]{}
	// create a FixedBlockStorageFile
	fbs, _ := NewSequenceBlockSingleFileStorage(conf.directory, conf.storageName, conf.blockSize)
	sm.Register(fbs)
	var wg sync.WaitGroup
	goFuncs := 5
	// make channel of size 5
	var ch = make(chan PageId, goFuncs)
	defer close(ch)
	for i := range goFuncs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			dummyBlock := &Page{}
			dummyBlock.Init(make([]byte, 32, 32))
			//Append/reserve space
			id, _ := fbs.ReserveId()
			fbs.Write(id, dummyBlock)
			// append id to channel
			ch <- id
			//
			slice := make([]int64, 0, 4)
			for range 4 {
				slice = append(slice, int64(i))
			}
			block := createByteSliceBlock(slice)
			// write
			fbs.Write(id, block)
			// read
			rb, _ := fbs.Read(id)
			result := byteSliceBlockToSlice(rb)
			// check results
			fmt.Printf("Writer %d, %v Read Result: %v\n", idx, id, result)
		}(i)
	}
	wg.Wait()
	// now we read all ids from channel
	for range goFuncs {
		id := <-ch
		rb, _ := fbs.Read(id)
		result := byteSliceBlockToSlice(rb)
		fmt.Printf("Reader %v, Read Result: %v\n", id, result)
	}
}
