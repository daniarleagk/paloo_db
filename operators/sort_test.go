//go:build !race

package operators

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daniar-achakeev/paloo_db/io"
	"github.com/daniar-achakeev/paloo_db/utils"
)

// MergeTournamentFunc
func MergeTournamentFunc[T any, C utils.Comparator[T]](sequences []iter.Seq2[T, error], cmp C) (iter.Seq2[T, error], error) {
	// create tournament tree
	iterators := make([]struct {
		next func() (T, error, bool)
		stop func()
	}, len(sequences))
	contestents := make([]TNode[T], len(sequences))
	for i, s := range sequences {
		next, stop := iter.Pull2(s)
		iterators[i].next, iterators[i].stop = next, stop
		r, err, ok := iterators[i].next()
		if !ok {
			contestents[i] = TNode[T]{idx: -1} // sentinel
			stop()
			continue
		}
		if err != nil {
			return nil, err
		}
		contestents[i] = TNode[T]{value: r, idx: i}
	}
	tournament := NewTournamentTree(cmp, contestents)
	return func(yield func(T, error) bool) {
		for { // repeatedly pull the smallest item from the tournament tree
			winner, ok := tournament.Winner()
			if !ok { // all sources exhausted
				return
			}
			winnerValue := winner.value
			if !yield(winnerValue, nil) {
				return
			}
			nextRecord, err, ok := iterators[winner.idx].next()
			if err != nil {
				yield(*new(T), err)
				iterators[winner.idx].stop()
				return
			}
			if !ok { // source exhausted
				tournament.Challenge(TNode[T]{idx: -1}, winner.idx)
				iterators[winner.idx].stop()
				continue
			}
			tournament.Challenge(TNode[T]{value: nextRecord, idx: winner.idx}, winner.idx)
		}
	}, nil
}

/* Heap merge implementation for testing purposes.
* This implementation uses a min-heap to merge multiple sorted sequences.
* It is not optimized for production use and is intended for testing and comparison purposes only.
 */

// PullIterRecordPair is a helper struct to hold the current record and the next function of an iterator
type PullIterRecordPair[T any] struct {
	record T
	next   func() (T, error, bool)
	stop   func()
}

// internal comparator for PullIterRecordPair
type PullIterRecordPairComparator[T any, C utils.Comparator[T]] struct {
	c C
}

func NewPullIterRecordPairComparator[T any, C utils.Comparator[T]](c C) PullIterRecordPairComparator[T, C] {
	return PullIterRecordPairComparator[T, C]{c: c}
}

func (c PullIterRecordPairComparator[T, C]) Compare(a, b PullIterRecordPair[T]) int {
	return c.c.Compare(a.record, b.record)
}

// MergeHeapFunc has the type of MergeFunc that uses a min-heap to merge sorted sequences.
// It takes a slice of sorted sequences and a comparator function, and returns a single merged sequence.
// It returns an error if any of the input sequences yield an error.
func MergeHeapFunc[T any, C utils.Comparator[T]](sequences []iter.Seq2[T, error], comparatorFunc C) (iter.Seq2[T, error], error) {
	// create heap
	heapCompare := NewPullIterRecordPairComparator(comparatorFunc)
	mergeHeap := &MergeHeap[PullIterRecordPair[T], PullIterRecordPairComparator[T, C]]{
		items:   []PullIterRecordPair[T]{},
		compare: heapCompare,
	}
	heap.Init(mergeHeap)
	// Implement merging logic here
	for _, s := range sequences {
		// pull the first item from each sequence
		next, stop := iter.Pull2(s)
		r, err, ok := next()
		if !ok {
			continue
		}
		if err != nil {
			return nil, err
		}
		pair := PullIterRecordPair[T]{record: r, next: next, stop: stop}
		heap.Push(mergeHeap, pair)
	}
	return func(yield func(T, error) bool) {
		for mergeHeap.Len() > 0 {
			// pull the smallest item from the heap
			item := heap.Pop(mergeHeap).(PullIterRecordPair[T])
			// yield the item
			if !yield(item.record, nil) {
				return
			}
			// push the next item from the same sequence
			next, stop := item.next, item.stop
			if r, err, ok := next(); ok {
				if err != nil {
					// stop the iteration on error
					// TODO: should I push error and Zero?
					stop()
					return
				}
				pair := PullIterRecordPair[T]{record: r, next: next, stop: stop}
				heap.Push(mergeHeap, pair)
			} else {
				stop()
			}
		}
	}, nil
}

// MergeHeap is a min-heap used for merging sorted sequences.
// uses standard library container/heap interface
type MergeHeap[T any, C utils.Comparator[T]] struct {
	items   []T
	compare C
}

func (h *MergeHeap[T, C]) Len() int {
	return len(h.items)
}

func (h *MergeHeap[T, C]) Less(i, j int) bool {
	return h.compare.Compare(h.items[i], h.items[j]) < 0
}

func (h *MergeHeap[T, C]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *MergeHeap[T, C]) Push(x any) {
	h.items = append(h.items, x.(T))
}

func (h *MergeHeap[T, C]) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}

type TNodeComparator[T any, C utils.Comparator[T]] struct {
	cmp C
}

func (t TNodeComparator[T, C]) Compare(l TNode[T], r TNode[T]) int {
	return t.cmp.Compare(l.value, r.value)
}

// HeapIterator is an iterator for testing purposes.
type HeapIterator[T any, C utils.Comparator[T]] struct {
	h         *MergeHeap[TNode[T], TNodeComparator[T, C]]
	iterators []utils.CloseableIterator[T]
}

func NewHeapIterator[T any, C utils.Comparator[T]](iterators []utils.CloseableIterator[T], cmp C) (*HeapIterator[T, C], error) {
	cmpT := TNodeComparator[T, C]{cmp: cmp}
	mergeHeap := &MergeHeap[TNode[T], TNodeComparator[T, C]]{
		items:   []TNode[T]{},
		compare: cmpT,
	}
	heap.Init(mergeHeap)
	// load heads
	for i, it := range iterators {
		r, ok, err := it.Next()
		if !ok {
			it.Close()
			continue
		}
		if err != nil {
			return nil, err
		}
		heap.Push(mergeHeap, TNode[T]{value: r, idx: i})
	}
	return &HeapIterator[T, C]{
		h:         mergeHeap,
		iterators: iterators,
	}, nil
}

func (h *HeapIterator[T, C]) Next() (T, bool, error) {
	if h.h.Len() > 0 {
		tn := heap.Pop(h.h).(TNode[T])
		next, ok, err := h.iterators[tn.idx].Next()
		if err != nil {
			return utils.Zero[T](), false, err
		}
		if ok {
			heap.Push(h.h, TNode[T]{value: next, idx: tn.idx})
		} // if exausted do nothing
		return tn.value, true, nil
	}
	return utils.Zero[T](), false, nil
}

func (h *HeapIterator[T, C]) Close() error {
	cErrs := make([]error, len(h.iterators))
	hasErr := false
	for i, it := range h.iterators {
		if err := it.Close(); err != nil {
			cErrs[i] = err
			hasErr = true
		}
	}
	if hasErr {
		return fmt.Errorf("close errors %w", errors.Join(cErrs...))
	}
	return nil
}

// Factory
func HeapIteratorFactory[T any, C utils.Comparator[T]](iterators []utils.CloseableIterator[T], cmp C) (utils.CloseableIterator[T], error) {
	it, err := NewHeapIterator(iterators, cmp)
	return it, err
}

/* END Heap merge implementation for testing purposes.
* This implementation uses a min-heap to merge multiple sorted sequences.
* It is not optimized for production use and is intended for testing and comparison purposes only.
 */

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

// Int32DSCmp is a deserializer, serializer and comparator for int32 type
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
	for i := range elementsCount {
		int32Slice = append(int32Slice, int32(i))
	}
	int32Slice = permutate(int32Slice)
	int32DSCmp := Int32DSCmp{}
	factory := func(file *os.File, serialize utils.Serializer[int32]) io.TempFileWriter[int32] {
		return io.NewFixedSizeTempFileWriter(file, readWriteBufferSize, 4, serialize)
	}
	runGenerator := NewGoStandarSortRunGenerator(
		runSizeByteMemory, // run size
		128/4,             // initial run size
		int32DSCmp,
		int32DSCmp,
		int32DSCmp,
		factory,
		1,
	)
	createTmpFile := func(currentRunIndex int, index int) (*os.File, error) {
		fileName := fmt.Sprintf("%s/%s_run_%06d_%03d.%s", directory, "sort", currentRunIndex, index, "tmp")
		return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	}
	err := runGenerator.GenerateRuns(slices.Values(int32Slice), createTmpFile)
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

func TestKWayMergeHeap(t *testing.T) {
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

	merger := MergeHeapFunc[int32, Int32DSCmp]
	comparator := Int32DSCmp{}
	mergedSeq, err := merger(sequences, comparator)
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

type IntCmp struct{}

func (d IntCmp) Compare(a, b int) int {
	return a - b
}

func initTestTournamentTree() (*TournamentTree[int, IntCmp], []iter.Seq2[int, error]) {
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
	iterSlice := make([]iter.Seq2[int, error], 0, k)
	iterSlice = append(iterSlice, Map2(slices.Values(a1), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a2), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a3), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a4), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a5), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a6), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a7), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a8), func(v int) int { return v }))
	iterSlice = append(iterSlice, Map2(slices.Values(a9), func(v int) int { return v }))
	cmp := IntCmp{}
	tournament := NewTournamentTree(cmp, []TNode[int]{
		{value: 3, idx: 0},
		{value: 1, idx: 1},
		{value: 4, idx: 2},
		{value: 10, idx: 3},
		{value: 5, idx: 4},
		{value: 9, idx: 5},
		{value: 2, idx: 6},
		{value: 6, idx: 7},
		{value: 8, idx: 8},
	})
	return tournament, iterSlice
}

func TestTournamentTreeBuild(t *testing.T) {
	tournament, sources := initTestTournamentTree()
	iterators := make([]struct {
		next func() (int, error, bool)
		stop func()
	}, len(sources))
	for i, source := range sources {
		next, stop := iter.Pull2(source)
		iterators[i].next, iterators[i].stop = next, stop
	}
	// consume initial winners
	for _, it := range iterators {
		it.next()
	}
	// test first ten winners
	for i := range 30 {
		winner, _ := tournament.Winner()
		//t.Log("Winner: ", winner, ok, "Index:", i)
		if winner.value != i+1 {
			t.Fatalf("expected winner to be %d, but got %d", i+1, winner.value)
		}
		//t.Log(tournament.tree)
		nextVal, _, ok := iterators[winner.idx].next()
		if !ok {
			tournament.Challenge(TNode[int]{idx: -1}, winner.idx)
			continue
		}
		//t.Log(nextVal)
		tournament.Challenge(TNode[int]{value: nextVal, idx: winner.idx}, winner.idx)
	}
	for _, source := range iterators {
		source.stop()
	}
	_, ok := tournament.Winner() // should nok
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
	merger := MergeTournamentFunc[int32, Int32DSCmp]
	mergedSeq, err := merger(sequences, Int32DSCmp{})
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

func TestSimpleInt64SortHeap(t *testing.T) {
	elementsCount := 15
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	tmpDirectory := t.TempDir()
	prefix := "int64sort"
	suffix := "tmp"
	kWay := 5
	readBufferSize, writeBufferSize := 1024, 1024 //
	runSize := 64                                 // 512 /8 = 64 int64 per run
	writeFactory := func(file *os.File, serialize utils.Serializer[int64]) io.TempFileWriter[int64] {
		return io.NewFixedSizeTempFileWriter(file, writeBufferSize, 8, serialize)
	}
	readFactory := func(file *os.File, deserialize utils.Deserializer[int64]) (utils.CloseableIterator[int64], error) {
		return io.NewTempFileIterator(file, readBufferSize, 8, deserialize)
	}
	cmpDeSer := Int64DSCmp{}
	runGenerator := NewGoStandarSortRunGenerator(
		runSize,
		512/8, // initial run size
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		writeFactory,
		2,
	)
	sorter := NewSorter(
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		HeapIteratorFactory[int64, Int64DSCmp],
		runGenerator,
		readFactory,
		writeFactory,
		tmpDirectory,
		prefix,
		suffix,
		kWay,
	)
	sortedSeq, err := sorter.Sort(slices.Values(int64Slice))
	if err != nil {
		t.Fatalf("failed to sort: %v", err)
	}
	previous := int64(-1)
	count := 0
	for {
		r, ok, err := sortedSeq.Next()
		if !ok {
			break
		}
		if err != nil {
			t.Errorf("read failed: %v", err)
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		previous = r
		count++
	}
	sortedSeq.Close()
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}
}

func TestSimpleInt64Sort(t *testing.T) {
	elementsCount := 1000
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	tmpDirectory := t.TempDir()
	prefix := "int64sort"
	suffix := "tmp"
	kWay := 5
	readBufferSize, writeBufferSize := 1024, 1024 //
	runSize := 512                                // 512 /8 = 64 int64 per run
	writeFactory := func(file *os.File, serialize utils.Serializer[int64]) io.TempFileWriter[int64] {
		return io.NewFixedSizeTempFileWriter(file, writeBufferSize, 8, serialize)
	}
	readFactory := func(file *os.File, deserialize utils.Deserializer[int64]) (utils.CloseableIterator[int64], error) {
		return io.NewTempFileIterator(file, readBufferSize, 8, deserialize)
	}
	cmpDeSer := Int64DSCmp{}
	runGenerator := NewGoStandarSortRunGenerator(
		runSize,
		512/8, // initial run size
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		writeFactory,
		2,
	)
	sorter := NewSorter(
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		TournamentIteratorFactory[int64, Int64DSCmp],
		runGenerator,
		readFactory,
		writeFactory,
		tmpDirectory,
		prefix,
		suffix,
		kWay,
	)
	sortedSeq, err := sorter.Sort(slices.Values(int64Slice))
	if err != nil {
		t.Fatalf("failed to sort: %v", err)
	}
	previous := int64(-1)
	count := 0
	for {
		r, ok, err := sortedSeq.Next()
		if !ok {
			break
		}
		if err != nil {
			t.Errorf("read failed: %v", err)
		}
		if r < previous {
			t.Errorf("expected %d, but got %d", previous, r)
		}
		previous = r
		count++
	}
	sortedSeq.Close()
	if count != elementsCount {
		t.Errorf("expected to read %d records, but got %d", elementsCount, count)
	}
}

func TestSimpleInt64SortLargeTournament(t *testing.T) {

	if strings.Contains(t.Name(), "large") {
		t.Skip("Local run test only")
	}

	elementsCount := 100_000_000
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	tests := []int{1, 2, 4, 6, 8, 10, 12}
	prefix := "int64sort"
	suffix := "tmp"
	kWay := 64                                            // memory is 128 KB * 100 at least
	runSize := 1024 * 1024 * 16                           // 16 MB Buffer
	readBufferSize, writeBufferSize := 1024*512, 1024*512 // 512 KB
	writeFactory := func(file *os.File, serialize utils.Serializer[int64]) io.TempFileWriter[int64] {
		return io.NewFixedSizeTempFileWriter(file, writeBufferSize, 8, serialize)
	}
	readFactory := func(file *os.File, deserialize utils.Deserializer[int64]) (utils.CloseableIterator[int64], error) {
		return io.NewTempFileIterator(file, readBufferSize, 8, deserialize)
	}
	cmpDeSer := Int64DSCmp{}
	// start tests
	for _, tt := range tests {
		t.Run(fmt.Sprint("Tournament ", tt), func(t *testing.T) {
			tmpDirectory := t.TempDir()
			t.Logf("Using temp directory: %s", tmpDirectory)
			var sortedSeq utils.CloseableIterator[int64]
			var err error
			k := tt
			runGenerator := NewGoStandarSortRunGenerator(
				runSize,
				runSize/8, // initial run size
				cmpDeSer,
				cmpDeSer,
				cmpDeSer,
				writeFactory,
				k,
			)
			sorter := NewSorter(
				cmpDeSer,
				cmpDeSer,
				cmpDeSer,
				TournamentIteratorFactory[int64, Int64DSCmp],
				runGenerator,
				readFactory,
				writeFactory,
				tmpDirectory,
				prefix,
				suffix,
				kWay,
			)
			start := time.Now()
			sortedSeq, err = sorter.Sort(slices.Values(int64Slice))
			if err != nil {
				t.Fatalf("failed to sort: %v", err)
			}
			duration := time.Since(start)
			t.Logf("Sort %s", duration)
			// verify sorting
			previous := int64(-1)
			count := 0
			start = time.Now()
			for {
				r, ok, err := sortedSeq.Next()
				if !ok {
					break
				}
				if err != nil {
					t.Fatalf("read failed: %v", err)
				}
				if r < previous {
					t.Fatalf("expected %d, but got %d", previous, r)
				}
				previous = r
				count++
			}
			if count != elementsCount {
				t.Fatalf("expected to read %d records, but got %d", elementsCount, count)
			}
			sortedSeq.Close()
			duration = time.Since(start)
			t.Logf("Last Merge %s", duration)
		})
	}
}

func TestSimpleInt64SortLargeHeap(t *testing.T) {
	if strings.Contains(t.Name(), "large") {
		t.Skip("Local run test only")
	}
	elementsCount := 100_000_000
	int64Slice := make([]int64, 0, elementsCount)
	for i := range elementsCount {
		int64Slice = append(int64Slice, int64(i))
	}
	int64Slice = permutate(int64Slice)
	prefix := "int64sort"
	suffix := "tmp"
	kWay := 64                                            // memory is 128 KB * 100 at least
	runSize := 1024 * 1024 * 32                           // 32 MB Buffer
	readBufferSize, writeBufferSize := 1024*512, 1024*512 // 512 KB
	writeFactory := func(file *os.File, serialize utils.Serializer[int64]) io.TempFileWriter[int64] {
		return io.NewFixedSizeTempFileWriter(file, writeBufferSize, 8, serialize)
	}
	readFactory := func(file *os.File, deserialize utils.Deserializer[int64]) (utils.CloseableIterator[int64], error) {
		return io.NewTempFileIterator(file, readBufferSize, 8, deserialize)
	}
	cmpDeSer := Int64DSCmp{}
	tmpDirectory := t.TempDir()
	t.Logf("Using temp directory: %s", tmpDirectory)
	var sortedSeq utils.CloseableIterator[int64]
	var err error
	runGenerator := NewGoStandarSortRunGenerator(
		runSize,
		runSize/8, // initial run size
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		writeFactory,
		2,
	)
	sorter := NewSorter(
		cmpDeSer,
		cmpDeSer,
		cmpDeSer,
		HeapIteratorFactory[int64, Int64DSCmp],
		runGenerator,
		readFactory,
		writeFactory,
		tmpDirectory,
		prefix,
		suffix,
		kWay,
	)
	start := time.Now()
	sortedSeq, err = sorter.Sort(slices.Values(int64Slice))
	if err != nil {
		t.Fatalf("failed to sort: %v", err)
	}
	duration := time.Since(start)
	t.Logf("Sort %s", duration)
	// verify sorting
	previous := int64(-1)
	count := 0
	start = time.Now()
	for {
		r, ok, err := sortedSeq.Next()
		if !ok {
			break
		}
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if r < previous {
			t.Fatalf("expected %d, but got %d", previous, r)
		}
		previous = r
		count++
	}
	if count != elementsCount {
		t.Fatalf("expected to read %d records, but got %d", elementsCount, count)
	}
	sortedSeq.Close()
	duration = time.Since(start)
	t.Logf("Last Merge %s", duration)
}

// Simple Test iterator over slices
type SliceIteratorInt64 struct {
	s   []int64
	idx int
}

func NewSliceIterator(s []int64) *SliceIteratorInt64 {
	return &SliceIteratorInt64{
		s:   s,
		idx: 0,
	}
}

func (s *SliceIteratorInt64) Next() (int64, bool, error) {
	if s.idx < len(s.s) {
		next := s.s[s.idx]
		s.idx++
		return next, true, nil
	}
	return 0, false, nil

}

func (s *SliceIteratorInt64) Close() error {
	s.idx = 0
	s.s = nil
	return nil
}

func TestIteratorBasedMergeFunction(t *testing.T) {
	iterators := []utils.CloseableIterator[int64]{
		NewSliceIterator([]int64{2, 5, 6, 9}),
		NewSliceIterator([]int64{3, 7, 10}),
		NewSliceIterator([]int64{1, 4, 8, 12, 13}),
		NewSliceIterator([]int64{0, 11}),
	}
	resIt, err := TournamentIteratorFactory(iterators, Int64DSCmp{})
	if err != nil {
		t.Fatalf("err %v", err)
	}
	idx := int64(0)
	for v := range utils.IteratorToSeq(resIt) {
		if idx != v {
			t.Fatalf("expected %d got %d", idx, v)
		}
		t.Log(v)
		idx++
	}
	resIt.Close()
}
