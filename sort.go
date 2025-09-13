// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory of this source tree.

package paloo_db

// This file implements a generic external sorter that can sort large datasets that do not fit into memory.
// TODO: provide abstraction to run generating and merging in parallel
// the main sorter should be flexible enough to allow different strategies for run generation and merging
// e.g. multi-threaded, single-threaded, replacement-selection, radix,... etc.

import (
	"container/heap"
	"fmt"
	"iter"
	"os"
	"slices"
	"sync"
)

type RunGenerator[T any] interface {
	GenerateRuns(input iter.Seq[T]) error
}

type Merger[T any] interface {
	MergeSeq(sequences []iter.Seq[RecordWithError[T]]) (iter.Seq[T], error)
}

type CreateTempFileWriterFactory[T any] interface {
	CreateTempFileWriter(file *os.File, bufferSize int, serialize func(item T) ([]byte, error)) TempFileWriter[T]
}

type CreateTempFileReaderFactory[T any] interface {
	CreateTempFileReader() TempFileReader[T]
}

// Sorter is a generic external sorter that can sort large datasets that do not fit into memory.
// main task is to orchestrate the sorting process by generating sorted runs and merging them.
// TODO: workload/resource  manager will assign memory and cpu to the sorter
// It uses a combination of in-memory sorting and external sorting techniques to achieve this.
// currently, we will implement a simple sorting with comparator on deserialized items
// TODO: implement also comparators based on serialized items to avoid deserialization overhead
type Sorter[T any] struct {
	comparatorFunc    func(a, b T) int
	getByteSize       func(item T) int
	serialize         func(item T) ([]byte, error)
	deserialize       func(data []byte) (T, error)
	directoryPath     string
	filePrefix        string
	fileExtension     string
	currentMergeRound int
	readBufferSize    int
	writeBufferSize   int
	kWayMergeSize     int // number of files that would be merged in each round
}

func (s *Sorter[T]) Sort(input iter.Seq[T]) (iter.Seq[T], error) {
	if input == nil {
		return nil, fmt.Errorf("input iterator is nil")
	}

	// Implement sorting logic here
	// For now, we'll just return the input as-is
	return input, nil
}

func (s *Sorter[T]) GenerateRuns(input iter.Seq[T]) (iter.Seq[T], error) {
	if input == nil {
		return nil, fmt.Errorf("input iterator is nil")
	}

	// Implement run generation logic here
	// For now, we'll just return the input as-is
	return input, nil
}

func (s *Sorter[T]) Close() error {
	// Implement any necessary cleanup logic here
	// removes all files in the directory with the same prefix
	return nil
}

type GoSortRunGenerator[T any] struct {
	comparatorFunc        func(a, b T) int
	getByteSize           func(item T) int
	serialize             func(item T) ([]byte, error)
	bufferSize            int // size of the in-memory buffer to hold items before sorting and flushing to disk
	runSize               int // maximum size of each run in bytes
	initialRunSize        int // estimated initial size of each run
	directoryPath         string
	filePrefix            string
	fileExtension         string
	tempFileWriterFactory CreateTempFileWriterFactory[T]
	sliceBuffer           []T
	parallelism           int
}

func NewGoSortRunGenerator[T any](
	comparatorFunc func(a, b T) int,
	getByteSize func(item T) int,
	serialize func(item T) ([]byte, error),
	bufferSize int,
	runSize int,
	initialRunSize int,
	directoryPath string,
	filePrefix string,
	fileExtension string,
	tempFileWriterFactory CreateTempFileWriterFactory[T],
	parallelism int,
) *GoSortRunGenerator[T] {
	runGen := &GoSortRunGenerator[T]{
		comparatorFunc:        comparatorFunc,
		getByteSize:           getByteSize,
		serialize:             serialize,
		bufferSize:            bufferSize,
		runSize:               runSize,
		initialRunSize:        initialRunSize,
		directoryPath:         directoryPath,
		filePrefix:            filePrefix,
		fileExtension:         fileExtension,
		tempFileWriterFactory: tempFileWriterFactory,
		parallelism:           parallelism,
	}
	runGen.sliceBuffer = make([]T, 0, initialRunSize)
	return runGen
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

func (g *GoSortRunGenerator[T]) sortAndFlush(currentRunIndex int) error {
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

func (g *GoSortRunGenerator[T]) createTmpFile(currentRunIndex int, index int) (*os.File, error) {
	fileName := fmt.Sprintf("%s/%s_run_%06d_%03d.%s", g.directoryPath, g.filePrefix, currentRunIndex, index, g.fileExtension)
	return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
}

type KWayMerger[T any] struct {
	comparatorFunc func(a, b T) int
}

func NewKWayMerger[T any](comparatorFunc func(a, b T) int) *KWayMerger[T] {
	return &KWayMerger[T]{comparatorFunc: comparatorFunc}
}

func (m *KWayMerger[T]) MergeSeq(sequences []iter.Seq[RecordWithError[T]]) (iter.Seq[T], error) {
	// create heap
	heapCompare := func(a, b PullIterRecordPair[T]) int {
		return m.comparatorFunc(a.record, b.record)
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
	return func(yield func(T) bool) {
		for mergeHeap.Len() > 0 {
			// pull the smallest item from the heap
			item := heap.Pop(mergeHeap).(PullIterRecordPair[T])
			// yield the item
			if !yield(item.record) {
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
	next   func() (RecordWithError[T], bool)
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
