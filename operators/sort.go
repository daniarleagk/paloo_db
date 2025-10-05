// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory of this source tree.

package operators

// This file implements a generic external sorter that can sort large datasets that do not fit into memory.
// TODO: provide abstraction to run generating and merging in parallel
// the main sorter should be flexible enough to allow different strategies for run generation and merging
// e.g. multi-threaded, single-threaded, replacement-selection, radix,... etc.

import (
	"container/heap"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/daniarleagk/paloo_db/io"
)

type CreateTempFileWriterFactory[T any] interface {
	CreateTempFileWriter(file *os.File, bufferSize int, serialize func(item T, buf []byte) error) io.TempFileWriter[T]
}

type CreateTempFileReaderFactory[T any] interface {
	CreateTempFileReader(file *os.File, bufferSize int, deserialize func(data []byte) (T, error)) io.TempFileReader[T]
}

type RunGenerator[T any] interface {
	Initialize(
		comparatorFunc func(a, b T) int,
		getByteSize func(item T) int,
		serialize func(item T, buf []byte) error,
		createTmpFile func(currentRunIndex int, index int) (*os.File, error),
		tempFileWriterFactory CreateTempFileWriterFactory[T],
	) error
	GenerateRuns(input iter.Seq[T]) error
}

type MergeFunc[T any] func(sequences []iter.Seq[io.RecordWithError[T]], comparatorFunc func(a, b T) int) (iter.Seq[io.RecordWithError[T]], error)

// Sorter is a generic external sorter that can sort large datasets that do not fit into memory.
// main task is to orchestrate the sorting process by generating sorted runs and merging them.
// TODO: workload/resource  manager will assign memory and cpu to the sorter
// It uses a combination of in-memory sorting and external sorting techniques to achieve this.
// currently, we will implement a simple sorting with comparator on deserialized items
// TODO: implement also comparators based on serialized items to avoid deserialization overhead
// TODO: if data can fully fit into memory, we can use in-memory sorting algorithms, fall back to external sorting otherwise
// TODO: since we process data in chunks and temp files are block oriented, we could also think about parallelizing flushing to disk and reading from disk
type Sorter[T any] struct {
	comparatorFunc        func(a, b T) int
	getByteSize           func(item T) int
	serialize             func(item T, buf []byte) error
	deserialize           func(data []byte) (T, error)
	tempFileReaderFactory CreateTempFileReaderFactory[T]
	tempFileWriterFactory CreateTempFileWriterFactory[T]
	runGenerator          RunGenerator[T]
	mergeFunc             MergeFunc[T]
	directoryPath         string
	filePrefix            string
	fileExtension         string
	currentMergeRound     int
	readBufferSize        int
	writeBufferSize       int
	kWayMergeSize         int // max number of files that would be merged in each round
	runStr                string
	mergeStr              string
}

func NewSorter[T any](
	comparatorFunc func(a, b T) int,
	getByteSize func(item T) int,
	serialize func(item T, buf []byte) error,
	deserialize func(data []byte) (T, error),
	runGenerator RunGenerator[T],
	mergeFunc MergeFunc[T],
	tempFileReaderFactory CreateTempFileReaderFactory[T],
	tempFileWriterFactory CreateTempFileWriterFactory[T],
	directoryPath string,
	filePrefix string,
	fileExtension string,
	readBufferSize int,
	writeBufferSize int,
	kWayMergeSize int,
) *Sorter[T] {
	return &Sorter[T]{
		comparatorFunc:        comparatorFunc,
		getByteSize:           getByteSize,
		serialize:             serialize,
		deserialize:           deserialize,
		runGenerator:          runGenerator,
		mergeFunc:             mergeFunc,
		tempFileReaderFactory: tempFileReaderFactory,
		tempFileWriterFactory: tempFileWriterFactory,
		directoryPath:         directoryPath,
		filePrefix:            filePrefix,
		fileExtension:         fileExtension,
		readBufferSize:        readBufferSize,
		writeBufferSize:       writeBufferSize,
		kWayMergeSize:         kWayMergeSize,
		runStr:                "run",
		mergeStr:              "merge",
	}
}

func (s *Sorter[T]) Sort(input iter.Seq[T]) (iter.Seq[io.RecordWithError[T]], error) {
	if input == nil {
		return nil, fmt.Errorf("input iterator is nil")
	}
	// Initialize the run generator
	createTmpFile := func(currentRunIndex int, index int) (*os.File, error) {
		fileName := fmt.Sprintf("%s/%s_%s_%d_%05d.%s", s.directoryPath, s.filePrefix, s.runStr, currentRunIndex, index, s.fileExtension)
		return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	}
	err := s.runGenerator.Initialize(s.comparatorFunc, s.getByteSize, s.serialize, createTmpFile, s.tempFileWriterFactory)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize run generator: %v", err)
	}
	// add logging to see how long it takes to generate runs
	err = s.runGenerator.GenerateRuns(input)
	fmt.Println("Run generation completed")
	if err != nil {
		return nil, fmt.Errorf("failed to generate runs: %v", err)
	}
	// now read all files with the given prefix from the directory
	// and merge them using the merge function
	isRunStage := true
	s.currentMergeRound = 0
	files, err := s.getFilesToMerge(isRunStage, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to get files to merge: %v", err)
	}
	fmt.Println("Number of runs generated:", len(files))
	for len(files) > s.kWayMergeSize {
		// now chunk the files into batches of kWayMergeSize
		s.currentMergeRound++
		for i := 0; i < len(files); i += s.kWayMergeSize {
			end := min(i+s.kWayMergeSize, len(files))
			batch := files[i:end]
			mergeSeq, err := s.mergeFiles(batch)
			if err != nil {
				return nil, fmt.Errorf("failed to merge files: %v", err)
			}
			err = s.flushMergeSequence(mergeSeq, s.currentMergeRound, i)
			if err != nil {
				return nil, fmt.Errorf("failed to flush merge sequence: %v", err)
			}
		}
		// currently we will delete files immediately after merging
		// in the future, we can implement a more sophisticated file management system
		// fire and forget
		s.deleteFiles(files)
		isRunStage = false
		files, err = s.getFilesToMerge(isRunStage, s.currentMergeRound)
		if err != nil {
			return nil, fmt.Errorf("failed to get files to merge: %v", err)
		}
	}
	return s.mergeFiles(files)
}

func (s *Sorter[T]) mergeFiles(files []string) (iter.Seq[io.RecordWithError[T]], error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to merge")
	}
	if len(files) == 1 {
		file, err := s.openFile(files[0])
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", files[0], err)
		}
		reader := s.tempFileReaderFactory.CreateTempFileReader(file, s.readBufferSize, s.deserialize)
		return reader.All(), nil
	}
	slicesOfSeq := make([]iter.Seq[io.RecordWithError[T]], 0, len(files))
	for _, fileName := range files {
		file, err := s.openFile(fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
		}
		reader := s.tempFileReaderFactory.CreateTempFileReader(file, s.readBufferSize, s.deserialize)
		slicesOfSeq = append(slicesOfSeq, reader.All())
	}
	return s.mergeFunc(slicesOfSeq, s.comparatorFunc)
}

func (s *Sorter[T]) getFilesToMerge(isRun bool, level int) ([]string, error) {
	entries, err := os.ReadDir(s.directoryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %v", err)
	}
	var files []string
	prefixCheck := s.filePrefix + "_" + s.runStr + "_"
	if !isRun {
		prefixCheck = s.filePrefix + "_" + s.mergeStr + "_" + fmt.Sprintf("%d", level) + "_"
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefixCheck) {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

func (s *Sorter[T]) flushMergeSequence(mergeSeq iter.Seq[io.RecordWithError[T]], currentMergeRound int, index int) error {
	// create a new temporary file for the merged output
	// realistically index is 6 decimal digits
	fileName := fmt.Sprintf("%s/%s_%s_%d_%06d.%s", s.directoryPath, s.filePrefix, s.mergeStr, currentMergeRound, index, s.fileExtension)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create merge output file %s: %v", fileName, err)
	}
	defer file.Close()
	// create a writer
	writer := s.tempFileWriterFactory.CreateTempFileWriter(file, s.writeBufferSize, s.serialize)
	defer writer.Close()
	err = writer.WriteSeq(func(yield func(T) bool) {
		for r := range mergeSeq {
			if r.Error != nil {
				// stop on error
				return
			}
			if !yield(r.Record) {
				return
			}
		}
	})
	if err != nil {
		return fmt.Errorf("failed to write merged sequence to file %s: %v", fileName, err)
	}
	return nil
}

func (s *Sorter[T]) deleteFiles(files []string) error {
	// asynchronously delete files
	// FIXME: currently fire and forget
	// in the future, we can use a worker pool to limit the number of concurrent deletions
	// and also handle errors properly
	// passing the error channel back to the caller
	go func() {
		for _, file := range files {
			filePath := filepath.Join(s.directoryPath, file)
			os.Remove(filePath)
		}
	}()
	return nil
}

func (s *Sorter[T]) openFile(fileName string) (*os.File, error) {
	// simple open file for reading
	filePath := filepath.Join(s.directoryPath, fileName)
	return os.Open(filePath)
}

func (s *Sorter[T]) Close() error {
	// Implement any necessary cleanup logic here
	// removes all files in the directory with the same prefix
	return nil
}

type GoSortRunGenerator[T any] struct {
	comparatorFunc        func(a, b T) int
	getByteSize           func(item T) int
	serialize             func(item T, buf []byte) error
	createTmpFile         func(currentRunIndex int, index int) (*os.File, error)
	tempFileWriterFactory CreateTempFileWriterFactory[T]
	bufferSize            int // size of the in-memory buffer to hold items before sorting and flushing to disk
	runSize               int // maximum size of each run in bytes
	initialRunSize        int // estimated initial size of each run
	sliceBuffer           []T
	parallelism           int
	mergeFunc             MergeFunc[T]
}

func NewGoSortRunGenerator[T any](
	bufferSize int,
	runSize int,
	initialRunSize int,
	parallelism int,
	mergeFunc MergeFunc[T],
) *GoSortRunGenerator[T] {
	runGen := &GoSortRunGenerator[T]{
		bufferSize:     bufferSize,
		runSize:        runSize,
		initialRunSize: initialRunSize,
		parallelism:    parallelism,
		mergeFunc:      mergeFunc,
	}
	runGen.sliceBuffer = make([]T, 0, initialRunSize)
	return runGen
}

func (g *GoSortRunGenerator[T]) Initialize(
	comparatorFunc func(a, b T) int,
	getByteSize func(item T) int,
	serialize func(item T, buf []byte) error,
	createTmpFile func(currentRunIndex int, index int) (*os.File, error),
	tempFileWriterFactory CreateTempFileWriterFactory[T],
) error {
	// TODO add nil checks for all functions
	g.comparatorFunc = comparatorFunc
	g.getByteSize = getByteSize
	g.serialize = serialize
	g.createTmpFile = createTmpFile
	g.tempFileWriterFactory = tempFileWriterFactory
	return nil
}

func (g *GoSortRunGenerator[T]) GenerateRuns(input iter.Seq[T]) error {
	if input == nil {
		return fmt.Errorf("input iterator is nil")
	}
	currentSizeBytes := 0
	currentRunIndex := 0
	for t := range input {
		byteSize := g.getByteSize(t)
		addedSize := currentSizeBytes + byteSize
		if addedSize > g.runSize {
			// sort and flush
			if err := g.sortAndFlush(currentRunIndex); err != nil {
				return fmt.Errorf("failed to sort and flush: %v", err)
			}
			currentSizeBytes = 0
			currentRunIndex++
			// reset the slice buffer
			g.sliceBuffer = nil
		}
		if g.sliceBuffer == nil {
			g.sliceBuffer = make([]T, 0, g.initialRunSize)
		}
		g.sliceBuffer = append(g.sliceBuffer, t)
		currentSizeBytes += byteSize
	}
	// flush the remaining items
	if len(g.sliceBuffer) > 0 {
		if err := g.sortAndFlush(currentRunIndex); err != nil {
			return fmt.Errorf("failed to sort and flush remaining items: %v", err)
		}
	}
	return nil
}

// sortAndFlush sorts the current sliceBuffer and flushes it to a temporary file using multiple goroutines.
// It divides the sliceBuffer into chunks and sorts each chunk in parallel, writing each sorted chunk to a separate temporary file.
// This approach allows for concurrent sorting and writing, improving performance on multi-core systems.
func (g *GoSortRunGenerator[T]) sortAndFlush(currentRunIndex int) error {
	// current plan not to use errgroup
	// change in the future if needed
	var sortedSeq iter.Seq[io.RecordWithError[T]]
	var err error
	if g.parallelism > 1 {
		var wg sync.WaitGroup
		errorsChan := make(chan error, g.parallelism)
		chunksChan := make(chan []T, g.parallelism)
		defer close(errorsChan)
		chunkSize := (len(g.sliceBuffer) + g.parallelism - 1) / g.parallelism
		for i := 0; i < g.parallelism; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				start := i * chunkSize
				if start >= len(g.sliceBuffer) {
					// no more data to process
					return
				}
				end := min((i+1)*chunkSize, len(g.sliceBuffer))
				part := g.sliceBuffer[start:end]
				// Sort the chunk
				slices.SortFunc(part, g.comparatorFunc)
				// Send the sorted chunk to the chunks channel
				chunksChan <- part
			}(i)
		}
		wg.Wait()
		// Check for any errors
		select {
		case err := <-errorsChan:
			return err
		default:
		}
		// Close the chunks channel to signal that no more chunks will be sent
		// This is safe to do here because all goroutines have completed
		// and no more chunks will be sent
		// get all chunks from the channel
		chunks := make([]iter.Seq[io.RecordWithError[T]], 0, g.parallelism)
		mapToRecordWithError := func(seq iter.Seq[T]) iter.Seq[io.RecordWithError[T]] {
			return func(yield func(io.RecordWithError[T]) bool) {
				for item := range seq {
					if !yield(io.RecordWithError[T]{Record: item, Error: nil}) {
						break
					}
				}
			}
		}
		for len(chunksChan) > 0 {
			chunk := <-chunksChan
			chunks = append(chunks, mapToRecordWithError(slices.Values(chunk)))
		}
		// Close the chunks channel
		defer close(chunksChan)
		// single threaded merge now to
		// merge all chunks into a single sorted sequence
		sortedSeq, err = g.mergeFunc(chunks, g.comparatorFunc)
		//sortedSeq, err = MergeTournamentFunc(chunks, g.comparatorFunc)
		if err != nil {
			return fmt.Errorf("failed to merge sorted chunks: %v", err)
		}
	} else {
		// Sort the entire sliceBuffer
		slices.SortFunc(g.sliceBuffer, g.comparatorFunc)
		// Create a sequence from the sorted sliceBuffer
		sortedSeq = func(yield func(io.RecordWithError[T]) bool) {
			for _, item := range g.sliceBuffer {
				if !yield(io.RecordWithError[T]{Record: item, Error: nil}) {
					break
				}
			}
		}
	}
	tmpFile, err := g.createTmpFile(currentRunIndex, 0)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tmpFile.Close()
	writer := g.tempFileWriterFactory.CreateTempFileWriter(tmpFile, g.bufferSize, g.serialize)
	if err := writer.WriteSeq(func(yield func(T) bool) {
		for r := range sortedSeq {
			if r.Error != nil {
				// stop on error
				return
			}
			if !yield(r.Record) {
				break
			}
		}
	}); err != nil {
		return fmt.Errorf("failed to write merged sequence to temporary file: %v", err)
	}
	return nil
}

/* func (g *GoSortRunGenerator[T]) sortAndFlush(currentRunIndex int) error {
	// sort the sliceBuffer using the comparatorFunc
	// write the sorted data to a temporary file using the tempFileWriterFactory
	// current length of the sliceBuffer
	// current plan not to use errgroup
	// change in the future if needed
	var wg sync.WaitGroup
	errorsChan := make(chan error, g.parallelism)
	defer close(errorsChan)
	chunkSize := (len(g.sliceBuffer) + g.parallelism - 1) / g.parallelism
	for i := 0; i < g.parallelism; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// Create a temporary file for this chunk
			tmpFile, err := g.createTmpFile(currentRunIndex, index)
			if err != nil {
				errorsChan <- fmt.Errorf("failed to create temporary file: %v", err)
				return
			}
			defer tmpFile.Close()
			start := i * chunkSize
			if start >= len(g.sliceBuffer) {
				// no more data to process
				return
			}
			end := min((i+1)*chunkSize, len(g.sliceBuffer))
			part := g.sliceBuffer[start:end]
			// Sort the chunk
			slices.SortFunc(part, g.comparatorFunc)
			// Write the sorted chunk to the temporary file
			writer := g.tempFileWriterFactory.CreateTempFileWriter(tmpFile, g.bufferSize, g.serialize)
			if err := writer.WriteSeq(slices.Values(part)); err != nil {
				errorsChan <- fmt.Errorf("failed to write chunk to file: %v", err)
				return
			}
		}(i)
	}
	wg.Wait()
	// Check for any errors
	select {
	case err := <-errorsChan:
		return err
	default:
	}
	return nil
}
*/

// MergeHeapFunc has the type of MergeFunc that uses a min-heap to merge sorted sequences.
// It takes a slice of sorted sequences and a comparator function, and returns a single merged sequence.
// It returns an error if any of the input sequences yield an error.
func MergeHeapFunc[T any](sequences []iter.Seq[io.RecordWithError[T]], comparatorFunc func(a, b T) int) (iter.Seq[io.RecordWithError[T]], error) {
	// create heap
	heapCompare := func(a, b PullIterRecordPair[T]) int {
		return comparatorFunc(a.record, b.record)
	}
	mergeHeap := &MergeHeap[PullIterRecordPair[T]]{
		items:   []PullIterRecordPair[T]{},
		compare: heapCompare,
	}
	heap.Init(mergeHeap)

	// Implement merging logic here
	for _, s := range sequences {
		// pull the first item from each sequence
		next, stop := iter.Pull(s)
		r, ok := next()
		if !ok {
			continue
		}
		if r.Error != nil {
			return nil, r.Error
		}
		pair := PullIterRecordPair[T]{record: r.Record, next: next, stop: stop}
		heap.Push(mergeHeap, pair)
	}
	// repeatedly pull the smallest item from the heap and push the next item from the same sequence
	// until all sequences are exhausted
	// return an iterator that yields the merged items
	return func(yield func(io.RecordWithError[T]) bool) {
		for mergeHeap.Len() > 0 {
			// pull the smallest item from the heap
			item := heap.Pop(mergeHeap).(PullIterRecordPair[T])
			// yield the item
			if !yield(io.RecordWithError[T]{Record: item.record}) {
				return
			}
			// push the next item from the same sequence
			next, stop := item.next, item.stop
			if r, ok := next(); ok {
				if r.Error != nil {
					// stop the iteration on error
					stop()
					return
				}
				pair := PullIterRecordPair[T]{record: r.Record, next: next, stop: stop}
				heap.Push(mergeHeap, pair)
			} else {
				stop()
			}
		}
	}, nil
}

// PullIterRecordPair is a helper struct to hold the current record and the next function of an iterator
type PullIterRecordPair[T any] struct {
	record T
	next   func() (io.RecordWithError[T], bool)
	stop   func()
}

// MergeHeap is a min-heap used for merging sorted sequences.
// uses standard library container/heap interface
type MergeHeap[T any] struct {
	items   []T
	compare func(a, b T) int
}

func (h *MergeHeap[T]) Len() int {
	return len(h.items)
}

func (h *MergeHeap[T]) Less(i, j int) bool {
	return h.compare(h.items[i], h.items[j]) < 0
}

func (h *MergeHeap[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *MergeHeap[T]) Push(x any) {
	h.items = append(h.items, x.(T))
}

func (h *MergeHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}

func MergeTournamentFunc[T any](sequences []iter.Seq[io.RecordWithError[T]], comparatorFunc func(a, b T) int) (iter.Seq[io.RecordWithError[T]], error) {
	// create tournament tree
	tournament := NewTournamentTree(func(a, b PullIterRecordPair[T]) int {
		return comparatorFunc(a.record, b.record)
	})
	contestents := make([]*PullIterRecordPair[T], len(sequences))
	for i, s := range sequences {
		next, stop := iter.Pull(s)
		r, ok := next()
		if !ok {
			contestents[i] = nil // sentinel
			continue
		}
		if r.Error != nil {
			return nil, r.Error
		}
		contestents[i] = &PullIterRecordPair[T]{record: r.Record, next: next, stop: stop}
	}
	tournament.VeryFirstTournament(contestents)
	return func(yield func(io.RecordWithError[T]) bool) {
		// repeatedly pull the smallest item from the tournament tree
		for {
			winner, ok := tournament.Winner()
			if !ok || winner.value == nil {
				// all sources exhausted
				return
			}
			winnerValue := winner.value
			yieldRecord := io.RecordWithError[T]{Record: winnerValue.record}
			if !yield(yieldRecord) {
				return
			}
			nextRecord, ok := winnerValue.next()
			if !ok {
				// source exhausted
				winner.value = nil
				winner.winner = nil

				tournament.Challenge(winner)
				winnerValue.stop()
				continue
			}
			winnerValue.record = nextRecord.Record
			tournament.Challenge(winner)
		}
	}, nil
}

// TournamentNode: represents a node in the tournament tree used for k-way merging.
type TournamentNode[T any] struct {
	winner *TournamentNode[T]
	idx    int // source index of the looser
	value  *T  // value of the looser
}

func (t TournamentNode[T]) String() string {
	strValue := fmt.Sprintf("%v", t.value)
	hasWinner := "nil"
	if t.winner != nil {
		hasWinner = fmt.Sprintf("idx=%d val=%v", t.winner.idx, t.winner.value)
	}
	return fmt.Sprintf("Node(idx=%d, value=%s) winner [ %s  prnt %p ]", t.idx, strValue, hasWinner, t.winner)
}

// TournamentTree: a k-way merge algorithm using a tournament tree
type TournamentTree[T any] struct {
	tree           []TournamentNode[T]
	comparatorFunc func(a, b T) int
}

func NewTournamentTree[T any](comparatorFunc func(a, b T) int) *TournamentTree[T] {
	return &TournamentTree[T]{tree: nil, comparatorFunc: comparatorFunc}
}

// VeryFirstTournament initializes the tournament tree with the given contestants.
func (t *TournamentTree[T]) VeryFirstTournament(contestants []*T) {
	// passes the winners and bulds the users, every node touches once
	// contestants are the leaves of the tree
	// build the tree from the leaves to the root
	// each node stores the index of the looser and its value
	// if a node is a sentinel, it means it has no value
	k := len(contestants)
	if k == 0 {
		return
	}
	d := 1
	levels := 0
	for d < k {
		d *= 2
		levels++
	}
	len := 2 * d // 1 based index, 0 is unused
	t.tree = make([]TournamentNode[T], len)
	for idx, value := range contestants {
		t.tree[d+idx].value = value
		t.tree[d+idx].idx = d + idx
		t.tree[d+idx].winner = &t.tree[d+idx]
	}
	// build the tree
	for level := levels - 1; level >= 0; level-- {
		start := 1 << level
		end := (1 << (level + 1)) - 1
		for i := start; i <= end; i++ {
			leftChild := t.tree[2*i]
			rightChild := t.tree[2*i+1]
			// at very start both children can be sentinels
			if leftChild.winner == nil && rightChild.winner == nil {
				// both children are sentinels
				t.tree[i].winner = nil
				t.tree[i].idx = -1 // store left child index
				t.tree[i].value = nil
				continue
			}
			// one child is a sentinel
			if leftChild.winner == nil && rightChild.winner != nil {
				// right child wins
				t.tree[i].winner = rightChild.winner
				t.tree[i].idx = -1
				t.tree[i].value = nil
				continue
			}
			if leftChild.winner != nil && rightChild.winner == nil {
				// left child wins
				t.tree[i].winner = leftChild.winner
				t.tree[i].idx = -1
				t.tree[i].value = nil
				continue
			}
			// both children have values
			if leftChild.winner.value == nil {
				t.tree[i].winner = &rightChild
				t.tree[i].idx = leftChild.idx
				t.tree[i].value = nil
				continue
			}
			if rightChild.winner.value == nil {
				t.tree[i].winner = &leftChild
				t.tree[i].idx = rightChild.idx
				t.tree[i].value = nil
				continue
			}
			left := *leftChild.winner.value
			right := *rightChild.winner.value
			// wrestle
			if t.comparatorFunc(left, right) <= 0 {
				t.tree[i].winner = leftChild.winner
				t.tree[i].idx = rightChild.winner.idx // store looser index
				t.tree[i].value = rightChild.winner.value
			} else {
				t.tree[i].winner = rightChild.winner
				t.tree[i].idx = leftChild.winner.idx
				t.tree[i].value = leftChild.winner.value
			}
		}
	}
}

// Winner returns the current winner of the tournament
func (t *TournamentTree[T]) Winner() (winner *TournamentNode[T], ok bool) {
	if len(t.tree) == 0 {
		return nil, false
	}
	if t.tree[1].winner.value == nil {
		return nil, false
	}
	return t.tree[1].winner, true
}

// Challenge updates the tournament tree with a new challenger
func (t *TournamentTree[T]) Challenge(challenger *TournamentNode[T]) {
	// get the leaf node at index  at update its looser value
	index := challenger.idx
	currentChallenger := challenger
	pIdx := index / 2
	for pIdx >= 1 {
		parent := &t.tree[pIdx]
		if parent.value == nil {
			// parent looser is sentinel then challenger wins automatically
			parent.winner = currentChallenger
			pIdx /= 2
			currentChallenger = parent.winner
			continue
		}
		if currentChallenger.value == nil {
			parent.winner = &t.tree[parent.idx]
			parent.idx = currentChallenger.idx
			parent.value = nil
			pIdx /= 2
			currentChallenger = parent.winner
			continue
		}
		parentLooserValue := *parent.value
		challengerValue := *currentChallenger.value
		if t.comparatorFunc(parentLooserValue, challengerValue) <= 0 {
			// parent looser wins
			parent.winner = &t.tree[parent.idx]
			parent.idx = currentChallenger.idx
			parent.value = currentChallenger.value

		} else {
			parent.winner = currentChallenger
		}
		pIdx /= 2
		currentChallenger = parent.winner
	}

}
