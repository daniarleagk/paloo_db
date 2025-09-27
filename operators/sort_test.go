//go:build !race

package operators

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daniarleagk/paloo_db/io"
)

type FixedSizeTempFileWriterFactory[T any] struct {
	recordSize int
}

func (f *FixedSizeTempFileWriterFactory[T]) CreateTempFileWriter(file *os.File, bufferSize int, serialize func(item T, buf []byte) error) io.TempFileWriter[T] {
	return io.NewFixedSizeTempFileWriter(file, bufferSize, f.recordSize, serialize)
}

type FixedSizeTempFileReaderFactory[T any] struct {
	recordSize int
}

func (f *FixedSizeTempFileReaderFactory[T]) CreateTempFileReader(file *os.File, bufferSize int, deserialize func(data []byte) (T, error)) io.TempFileReader[T] {
	return io.NewFixedSizeTempFileReader(file, bufferSize, f.recordSize, deserialize)
}

func permutate[T any](slice []T) []T {
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
	serialize := func(item int32, buf []byte) error {
		binary.BigEndian.PutUint32(buf, uint32(item))
		return nil
	}

	comparator := func(a, b int32) int {
		return int(a - b)
	}
	getByteSize := func(item int32) int {
		return 4 // size of int32
	}
	runGenerator := NewGoSortRunGenerator[int32](
		readWriteBufferSize, // buffer size
		runSizeByteMemory,   // run size
		128/4,               // initial run size
		parallelism,         // parallelism
	)
	createTmpFile := func(currentRunIndex int, index int) (*os.File, error) {
		fileName := fmt.Sprintf("%s/%s_run_%06d_%03d.%s", directory, "sort", currentRunIndex, index, "tmp")
		return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	}
	runGenerator.Initialize(comparator, getByteSize, serialize, createTmpFile, &FixedSizeTempFileWriterFactory[int32]{recordSize: 4})
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
		return int32(binary.BigEndian.Uint32(data)), nil
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
		reader := io.NewFixedSizeTempFileReader(f, 64, 4, deserialize)
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

func Map[T any, U any](input iter.Seq[T], mapFunc func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for item := range input {
			if !yield(mapFunc(item)) {
				break
			}
		}
	}
}

func TestKWayMerger(t *testing.T) {
	k := 5
	sequences := make([]iter.Seq[io.RecordWithError[int32]], 0)
	allCount := 0
	slice := make([]int32, 0)
	numElements := 53
	for i := range numElements {
		slice = append(slice, int32(i))
		allCount++
	}
	chunkSize := (numElements + k - 1) / k
	for i := range k {
		start := i * chunkSize
		end := min(start+chunkSize, numElements)
		if start >= end {
			continue
		}
		subSlice := slice[start:end]
		t.Log("Sequence", i, "from", start, "to", end, "size", len(subSlice), "contents:", subSlice)
		mappedIt := Map(slices.Values(subSlice), func(v int32) io.RecordWithError[int32] {
			return io.RecordWithError[int32]{Record: v, Error: nil}
		})
		sequences = append(sequences, mappedIt)
	}
	t.Logf("Merging %d sequences", len(sequences))
	mergedSeq, err := MergeHeapFunc(sequences, func(a, b int32) int {
		return int(a - b)
	})
	if err != nil {
		t.Fatalf("failed to merge sequences: %v", err)
	}
	previous := int32(-1)
	count := 0
	for r := range mergedSeq {
		if r.Record < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		//t.Log("sorted record", r)
		previous = r.Record
		count++
	}
	if count != allCount {
		t.Errorf("expected to read %d records, but got %d", allCount, count)
	}
}

func TestSimpleInt64Sort(t *testing.T) {
	elementsCount := 100
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	comparator := func(a, b int64) int {
		return int(a - b)
	}
	getByteSize := func(item int64) int {
		return 8 // size of int64
	}
	serialize := func(item int64, buf []byte) error {
		binary.BigEndian.PutUint64(buf, uint64(item))
		return nil
	}
	deserialize := func(data []byte) (int64, error) {
		return int64(binary.BigEndian.Uint64(data)), nil
	}
	tmpDirectory := t.TempDir()
	prefix := "int64sort"
	suffix := "tmp"
	factoryReader := &FixedSizeTempFileReaderFactory[int64]{recordSize: 8}
	factoryWriter := &FixedSizeTempFileWriterFactory[int64]{recordSize: 8}
	parallel := 1
	kWay := 4
	readBufferSize, writeBufferSize := 128, 128 // 12 bytes per page header 128 -12 = 116 bytes for data => 14 int64 per page
	runSize := 512                              // 512 /8 = 64 int64 per run
	runGenerator := NewGoSortRunGenerator[int64](
		readBufferSize,
		runSize,
		512/8, // initial run size
		parallel,
	)
	sorter := NewSorter(
		comparator,
		getByteSize,
		serialize,
		deserialize,
		runGenerator,
		MergeHeapFunc,
		factoryReader,
		factoryWriter,
		tmpDirectory,
		prefix,
		suffix,
		readBufferSize,
		writeBufferSize,
		kWay,
	)
	sortedSeq, err := sorter.Sort(slices.Values(int64Slice))
	if err != nil {
		t.Fatalf("failed to sort: %v", err)
	}
	previous := int64(-1)
	count := 0
	for r := range sortedSeq {
		if r.Error != nil {
			t.Errorf("read failed: %v", r.Error)
		}
		if r.Record < previous {
			t.Errorf("expected %d, but got %d", previous, r.Record)
		}
		previous = r.Record
		count++
	}
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}

}

func TestSimpleInt64SortLarge(t *testing.T) {

	if strings.Contains(t.Name(), "Large") {
		t.Skip("Local run test only")
	}

	elementsCount := 100_000_000
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	comparator := func(a, b int64) int {
		return int(a - b)
	}
	getByteSize := func(item int64) int {
		return 8 // size of int64
	}
	serialize := func(item int64, buf []byte) error {
		binary.BigEndian.PutUint64(buf, uint64(item))
		return nil
	}
	deserialize := func(data []byte) (int64, error) {
		return int64(binary.BigEndian.Uint64(data)), nil
	}
	tmpDirectory := t.TempDir()
	t.Logf("Using temp directory: %s", tmpDirectory)
	prefix := "int64sort"
	suffix := "tmp"
	factoryReader := &FixedSizeTempFileReaderFactory[int64]{recordSize: 8}
	factoryWriter := &FixedSizeTempFileWriterFactory[int64]{recordSize: 8}
	parallelism := 4
	kWay := 64                                            // memory is 128 KB * 100 at least
	readBufferSize, writeBufferSize := 1024*256, 1024*256 // 256 KB
	runSize := 1024 * 1024 * 16                           // 16 MB Buffer
	runGenerator := NewGoSortRunGenerator[int64](
		readBufferSize,
		runSize,
		(1024*512)/8, // initial run size
		parallelism,
	)
	sorter := NewSorter(
		comparator,
		getByteSize,
		serialize,
		deserialize,
		runGenerator,
		MergeHeapFunc,
		factoryReader,
		factoryWriter,
		tmpDirectory,
		prefix,
		suffix,
		readBufferSize,
		writeBufferSize,
		kWay,
	)
	start := time.Now()
	sortedSeq, err := sorter.Sort(slices.Values(int64Slice))
	duration := time.Since(start)
	t.Logf("Sort -1 merge %s", duration)

	if err != nil {
		t.Fatalf("failed to sort: %v", err)
	}
	previous := int64(-1)
	count := 0
	start = time.Now()
	for r := range sortedSeq {
		if r.Error != nil {
			t.Errorf("read failed: %v", r.Error)
		}
		if r.Record < previous {
			t.Errorf("expected %d, but got %d", previous, r.Record)
		}
		previous = r.Record
		count++
	}
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}
	duration = time.Since(start)
	t.Logf("Last Merge %s", duration)

}
