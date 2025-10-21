//go:build !race

package operators

import (
	"encoding/binary"
	"fmt"
	"iter"
	"log"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daniarleagk/paloo_db/io"
)

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

type Int32DSCmp struct{}

func (d Int32DSCmp) Deserialize(data []byte) (int32, error) {
	return int32(binary.BigEndian.Uint32(data)), nil
}

// Serialize
func (d Int32DSCmp) Serialize(item int32, buf []byte) error {
	binary.BigEndian.PutUint32(buf, uint32(item))
	return nil
}

func (d Int32DSCmp) Compare(a, b int32) int {
	return int(a - b)
}

func (g Int32DSCmp) GetByteSize(item int32) int {
	return 4
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
	merger := TournamentMerger[int32, Int32DSCmp]{}
	runGenerator := NewGoSortRunGenerator[int32, Int32DSCmp, Int32DSCmp, Int32DSCmp, TournamentMerger[int32, Int32DSCmp]](
		func(file *os.File, bufferSize int, serialize Int32DSCmp) io.TempFileWriter[int32] {
			return io.NewFixedSizeTempFileWriter(file, bufferSize, 4, serialize)
		},
		readWriteBufferSize, // buffer size
		runSizeByteMemory,   // run size
		128/4,               // initial run size
		parallelism,         // parallelism
	)
	int32DSCmp := Int32DSCmp{}
	createTmpFile := func(currentRunIndex int, index int) (*os.File, error) {
		fileName := fmt.Sprintf("%s/%s_run_%06d_%03d.%s", directory, "sort", currentRunIndex, index, "tmp")
		return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	}
	err := runGenerator.GenerateRuns(slices.Values(int32Slice), int32DSCmp, int32DSCmp, int32DSCmp, merger, createTmpFile)
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
	t.Log("Generated files:", files)
	count := 0
	//if len(files) != 12 { // 3 runs * 4 parallelism
	//	t.Fatalf("expected 12 files, but got %d", len(files))
	//}
	for _, file := range files {
		f, err := os.Open(directory + "/" + file)
		if err != nil {
			t.Fatalf("failed to open file %s: %v", file, err)
		}
		reader := io.NewFixedLenTempFileReader(f, 64, 4, int32DSCmp)
		t.Logf("Reading file: %s", file)
		previous := int32(-1)
		for r, err := range reader.All() { // just to ensure we can read all records
			if err != nil {
				t.Errorf("read failed: %v", err)
			}
			count++
			// check ordering
			if r < previous {
				t.Errorf("expected %d, but got %d", previous+1, r)
			}
			previous = r
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

func Map2[T any, U any](input iter.Seq[T], mapFunc func(T) U) iter.Seq2[U, error] {
	return func(yield func(U, error) bool) {
		for item := range input {
			if !yield(mapFunc(item), nil) {
				break
			}
		}
	}
}

func TestKWayMerger(t *testing.T) {
	k := 5
	sequences := make([]iter.Seq2[int32, error], 0)
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
		mappedIt := Map2(slices.Values(subSlice), func(v int32) int32 {
			return v
		})
		sequences = append(sequences, mappedIt)
	}
	t.Logf("Merging %d sequences", len(sequences))

	merger := HeapMerger[int32, Int32DSCmp]{}
	comparator := Int32DSCmp{}
	mergedSeq, err := merger.Merge(sequences, comparator)
	if err != nil {
		t.Fatalf("failed to merge sequences: %v", err)
	}
	previous := int32(-1)
	count := 0
	for r, err := range mergedSeq {
		if err != nil {
			t.Errorf("failed to read merged sequence: %v", err)
			continue
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		//t.Log("sorted record", r)
		previous = r
		count++
	}
	if count != allCount {
		t.Errorf("expected to read %d records, but got %d", allCount, count)
	}
}

func printTournamentTree[T any](tree []TournamentNode[T]) {
	if len(tree) == 0 {
		fmt.Println("Empty tree")
		return
	}
	for i := range tree {
		fmt.Printf("Index: %d ->  %s\n", i, tree[i].String())
	}
}

type IntCmp struct{}

func (IntCmp) Compare(a, b PullIterRecordPair[int]) int {
	return a.record - b.record
}

func initTestTournamentTree() (*TournamentTree[PullIterRecordPair[int], IntCmp], []*PullIterRecordPair[int]) {
	k := 9
	a1 := []int{3, 12, 18}
	a2 := []int{1, 16, 21, 27}
	a3 := []int{4, 7, 23, 28}
	a4 := []int{10, 11, 26, 30}
	a5 := []int{5, 13, 25}
	a6 := []int{9, 14}
	a7 := []int{2, 15, 22}
	a8 := []int{6, 17, 20}
	a9 := []int{8, 19, 24, 29}
	iterSlice := make([]*PullIterRecordPair[int], 0, k)
	// 1
	n1, s1 := iter.Pull2(Map2(slices.Values(a1), func(v int) int {
		return v
	}))
	r1, _, _ := n1()
	p1 := PullIterRecordPair[int]{record: r1, next: n1, stop: s1}
	iterSlice = append(iterSlice, &p1)
	// 2
	n2, s2 := iter.Pull2(Map2(slices.Values(a2), func(v int) int {
		return v
	}))
	r2, _, _ := n2()
	p2 := PullIterRecordPair[int]{record: r2, next: n2, stop: s2}
	iterSlice = append(iterSlice, &p2)
	// 3
	n3, s3 := iter.Pull2(Map2(slices.Values(a3), func(v int) int {
		return v
	}))
	r3, _, _ := n3()
	p3 := PullIterRecordPair[int]{record: r3, next: n3, stop: s3}
	iterSlice = append(iterSlice, &p3)
	// 4
	n4, s4 := iter.Pull2(Map2(slices.Values(a4), func(v int) int {
		return v
	}))
	r4, _, _ := n4()
	p4 := PullIterRecordPair[int]{record: r4, next: n4, stop: s4}
	iterSlice = append(iterSlice, &p4)
	// 5
	n5, s5 := iter.Pull2(Map2(slices.Values(a5), func(v int) int {
		return v
	}))
	r5, _, _ := n5()
	p5 := PullIterRecordPair[int]{record: r5, next: n5, stop: s5}
	iterSlice = append(iterSlice, &p5)
	// 6
	n6, s6 := iter.Pull2(Map2(slices.Values(a6), func(v int) int {
		return v
	}))
	r6, _, _ := n6()
	p6 := PullIterRecordPair[int]{record: r6, next: n6, stop: s6}
	iterSlice = append(iterSlice, &p6)
	// 7
	n7, s7 := iter.Pull2(Map2(slices.Values(a7), func(v int) int {
		return v
	}))
	r7, _, _ := n7()
	p7 := PullIterRecordPair[int]{record: r7, next: n7, stop: s7}
	iterSlice = append(iterSlice, &p7)
	// 8
	n8, s8 := iter.Pull2(Map2(slices.Values(a8), func(v int) int {
		return v
	}))
	r8, _, _ := n8()
	p8 := PullIterRecordPair[int]{record: r8, next: n8, stop: s8}
	iterSlice = append(iterSlice, &p8)
	// 9
	n9, s9 := iter.Pull2(Map2(slices.Values(a9), func(v int) int {
		return v
	}))
	r9, _, _ := n9()
	p9 := PullIterRecordPair[int]{record: r9, next: n9, stop: s9}
	iterSlice = append(iterSlice, &p9)
	cmp := IntCmp{}
	tournament := NewTournamentTree(cmp)
	tournament.VeryFirstTournament(iterSlice)
	return tournament, iterSlice
}

func TestTournamentTreeBuild(t *testing.T) {
	tournament, sources := initTestTournamentTree()
	// test first ten winners
	for i := range 30 {
		winner, ok := tournament.Winner()
		log.Printf("%d  Winner: %p, ok: %v, %s", i, winner, ok, winner)
		if winner.value.record != i+1 {
			t.Fatalf("expected winner to be %d, but got %d", i+1, winner.value.record)
		}
		iteratorPair := winner.value
		nextVal, _, ok := iteratorPair.next()
		if !ok {
			winner.value = nil
			winner.winner = nil
			tournament.Challenge(winner) // p2 is now 16
			continue
		}
		iteratorPair.record = nextVal
		tournament.Challenge(winner) // p2 is now 16
	}
	for _, source := range sources {
		source.stop()
	}
	printTournamentTree(tournament.tree)
	_, ok := tournament.Winner() // should be nil now
	if ok {
		t.Fatalf("expected no winner, but got one")
	}
}

func TestTournamentMerge(t *testing.T) {
	k := 5
	sequences := make([]iter.Seq2[int32, error], 0)
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
		mappedIt := Map2(slices.Values(subSlice), func(v int32) int32 {
			return v
		})
		sequences = append(sequences, mappedIt)
	}
	t.Logf("Merging %d sequences", len(sequences))
	merger := TournamentMerger[int32, Int32DSCmp]{}
	mergedSeq, err := merger.Merge(sequences, Int32DSCmp{})
	if err != nil {
		t.Fatalf("failed to merge sequences: %v", err)
	}
	previous := int32(-1)
	count := 0
	for r, err := range mergedSeq {
		if err != nil {
			t.Errorf("failed to read record: %v", err)
			continue
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		//t.Log("sorted record", r)
		previous = r
		count++
	}
	if count != allCount {
		t.Errorf("expected to read %d records, but got %d", allCount, count)
	}
}

type Int64DSCmp struct{}

func (d Int64DSCmp) Deserialize(data []byte) (int64, error) {
	return int64(binary.BigEndian.Uint64(data)), nil
}

// Serialize
func (d Int64DSCmp) Serialize(item int64, buf []byte) error {
	binary.BigEndian.PutUint64(buf, uint64(item))
	return nil
}

func (d Int64DSCmp) Compare(a, b int64) int {
	return int(a - b)
}

func (g Int64DSCmp) GetByteSize(item int64) int {
	return 8
}

func TestSimpleInt64Sort(t *testing.T) {
	elementsCount := 100
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	tmpDirectory := t.TempDir()
	prefix := "int64sort"
	suffix := "tmp"
	parallel := 1
	kWay := 4
	readBufferSize, writeBufferSize := 128, 128 // 12 bytes per page header 128 -12 = 116 bytes for data => 14 int64 per page
	runSize := 512                              // 512 /8 = 64 int64 per run
	runGenerator := NewGoSortRunGenerator[int64, Int64DSCmp, Int64DSCmp, Int64DSCmp, HeapMerger[int64, Int64DSCmp]](
		func(file *os.File, bufferSize int, serialize Int64DSCmp) io.TempFileWriter[int64] {
			return io.NewFixedSizeTempFileWriter(file, bufferSize, 8, serialize)
		},
		readBufferSize,
		runSize,
		512/8, // initial run size
		parallel,
	)
	cmpDeSer := Int64DSCmp{}
	merger := HeapMerger[int64, Int64DSCmp]{}
	sorter := NewSorter(
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		merger,
		runGenerator,
		func(file *os.File, bufferSize int, deserialize Int64DSCmp) io.TempFileReader[int64] {
			return io.NewFixedLenTempFileReader(file, bufferSize, 8, deserialize)
		},
		func(file *os.File, bufferSize int, serialize Int64DSCmp) io.TempFileWriter[int64] {
			return io.NewFixedSizeTempFileWriter(file, bufferSize, 8, serialize)
		},
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
	for r, err := range sortedSeq {
		if err != nil {
			t.Errorf("read failed: %v", err)
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		previous = r
		count++
	}
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}

}

func TestSimpleInt64SortLarge(t *testing.T) {

	if strings.Contains(t.Name(), "large") {
		t.Skip("Local run test only")
	}

	elementsCount := 100_000_000
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	tests := []struct {
		name string
		heap bool
	}{
		{"Heap", true},
		{"Tournament", false},
	}
	for _, tt := range tests {
		t.Logf("Running test: %s", tt.name)
		t.Run(tt.name, func(t *testing.T) {
			tmpDirectory := t.TempDir()
			t.Logf("Using temp directory: %s", tmpDirectory)
			prefix := "int64sort"
			suffix := "tmp"
			parallelism := 12
			kWay := 64                                            // memory is 128 KB * 100 at least
			readBufferSize, writeBufferSize := 1024*512, 1024*512 // 512 KB
			runSize := 1024 * 1024 * 16                           // 16 MB Buffer
			var sortedSeq iter.Seq2[int64, error]
			var err error
			if tt.heap {
				runGenerator := NewGoSortRunGenerator[int64, Int64DSCmp, Int64DSCmp, Int64DSCmp, HeapMerger[int64, Int64DSCmp]](
					func(file *os.File, bufferSize int, serialize Int64DSCmp) io.TempFileWriter[int64] {
						return io.NewFixedSizeTempFileWriter(file, bufferSize, 8, serialize)
					},
					readBufferSize,
					runSize,
					(runSize)/8, // initial run size
					parallelism,
				)
				cmpDeSer := Int64DSCmp{}
				sorter := NewSorter(
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					HeapMerger[int64, Int64DSCmp]{},
					runGenerator,
					func(file *os.File, bufferSize int, deserialize Int64DSCmp) io.TempFileReader[int64] {
						return io.NewFixedLenTempFileReader(file, bufferSize, 8, deserialize)
					},
					func(file *os.File, bufferSize int, serialize Int64DSCmp) io.TempFileWriter[int64] {
						return io.NewFixedSizeTempFileWriter(file, bufferSize, 8, serialize)
					},
					tmpDirectory,
					prefix,
					suffix,
					readBufferSize,
					writeBufferSize,
					kWay,
				)
				start := time.Now()
				sortedSeq, err = sorter.Sort(slices.Values(int64Slice))
				if err != nil {
					t.Fatalf("failed to sort: %v", err)
				}
				duration := time.Since(start)
				t.Logf("Sort -1 merge %s", duration)
			} else {
				runGenerator := NewGoSortRunGenerator[int64, Int64DSCmp, Int64DSCmp, Int64DSCmp, TournamentMerger[int64, Int64DSCmp]](
					func(file *os.File, bufferSize int, serialize Int64DSCmp) io.TempFileWriter[int64] {
						return io.NewFixedSizeTempFileWriter(file, bufferSize, 8, serialize)
					},
					readBufferSize,
					runSize,
					(runSize)/8, // initial run size
					parallelism,
				)
				cmpDeSer := Int64DSCmp{}
				sorter := NewSorter(
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					cmpDeSer,
					TournamentMerger[int64, Int64DSCmp]{},
					runGenerator,
					func(file *os.File, bufferSize int, deserialize Int64DSCmp) io.TempFileReader[int64] {
						return io.NewFixedLenTempFileReader(file, bufferSize, 8, deserialize)
					},
					func(file *os.File, bufferSize int, serialize Int64DSCmp) io.TempFileWriter[int64] {
						return io.NewFixedSizeTempFileWriter(file, bufferSize, 8, serialize)
					},
					tmpDirectory,
					prefix,
					suffix,
					readBufferSize,
					writeBufferSize,
					kWay,
				)
				start := time.Now()
				sortedSeq, err = sorter.Sort(slices.Values(int64Slice))
				if err != nil {
					t.Fatalf("failed to sort: %v", err)
				}
				duration := time.Since(start)
				t.Logf("Sort -1 merge %s", duration)
			}

			previous := int64(-1)
			count := 0
			start := time.Now()
			for r, err := range sortedSeq {
				if err != nil {
					t.Errorf("read failed: %v", err)
				}
				if r < previous {
					t.Errorf("expected %d, but got %d", previous, r)
				}
				previous = r
				count++
			}
			if count != elementsCount {
				t.Errorf("expected to read %d records, but got %d", elementsCount, count)
			}
			duration := time.Since(start)
			t.Logf("Last Merge %s", duration)
		})
	}
}
