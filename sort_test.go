//go:build !race

package paloo_db

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"slices"
	"testing"
)

type FixedSizeTempFileWriterFactory[T any] struct {
	recordSize int
}

func (f *FixedSizeTempFileWriterFactory[T]) CreateTempFileWriter(file *os.File, bufferSize int, serialize func(item T) ([]byte, error)) TempFileWriter[T] {
	return NewFixedSizeTempFileWriter(file, bufferSize, f.recordSize, serialize)
}

func permutate(slice []int32) []int32 {
	n := len(slice)
	r := rand.New(rand.NewSource(42))
	for i := n - 1; i > 0; i-- {
		// get a random index j such that 0 <= j <= i
		if i > 1 {
			j := int(r.Int31n(int32(i - 1)))
			// swap slice[i] and slice[j]
			slice[i], slice[j] = slice[j], slice[i]
		}
	}
	return slice
}

func TestGoSortRunGenerator(t *testing.T) {
	elementsCount := 128 + 30 // we would have three runs each run will be sorted in parallel producing 4 files per run
	readWriteBufferSize := 64 // 12 bytes per page header 64 -12 = 52 bytes for data => 13 int32 per page
	runSizeByteMemory := 256  // 256 /4 = 64 int32 per run
	directory := t.TempDir()
	int32Slice := make([]int32, 0, elementsCount)
	parallelism := 4
	for i := range elementsCount {
		int32Slice = append(int32Slice, int32(i))
	}
	int32Slice = permutate(int32Slice)
	serialize := func(item int32) ([]byte, error) {
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, item); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	comparator := func(a, b int32) int {
		return int(a - b)
	}
	getByteSize := func(item int32) int {
		return 4 // size of int32
	}
	runGenerator := NewGoSortRunGenerator(
		comparator,
		getByteSize,
		serialize,
		readWriteBufferSize, // buffer size
		runSizeByteMemory,   // run size
		128/4,               // initial run size
		directory,
		"sort",
		"tmp",
		&FixedSizeTempFileWriterFactory[int32]{recordSize: 4},
		parallelism, // parallelism
	)
	err := runGenerator.GenerateRuns(slices.Values(int32Slice))
	if err != nil {
		t.Fatalf("failed to generate runs: %v", err)
	}
	// read back the files and verify sorting

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("failed to read directory: %v", err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	slices.Sort(files)
	deserialize := func(data []byte) (int32, error) {
		var record int32
		buf := bytes.NewReader(data)
		if err := binary.Read(buf, binary.BigEndian, &record); err != nil {
			return 0, err
		}
		return record, nil
	}
	t.Log("Generated files:", files)
	count := 0
	if len(files) != 12 { // 3 runs * 4 parallelism
		t.Fatalf("expected 12 files, but got %d", len(files))
	}
	for _, file := range files {
		f, err := os.Open(directory + "/" + file)
		if err != nil {
			t.Fatalf("failed to open file %s: %v", file, err)
		}
		reader := NewFixedSizeTempFileReader(f, 64, 4, deserialize)
		t.Logf("Reading file: %s", file)
		previous := int32(-1)
		for r := range reader.All() { // just to ensure we can read all records
			if r.Error != nil {
				t.Errorf("read failed: %v", r.Error)
			}
			count++
			// check ordering
			if r.Record < previous {
				t.Errorf("expected %d, but got %d", previous+1, r.Record)
			}
			previous = r.Record
		}
		f.Close()
	}
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}
}

// TODO More tests  e.g. error handling and edge cases
// also different configurations e.g. parallelism, run size, buffer size etc.
