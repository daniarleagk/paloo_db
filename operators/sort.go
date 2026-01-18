// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory of this source tree.

package operators

// This file implements a generic external sorter that can sort large datasets that do not fit into memory.
import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/daniar-achakeev/paloo_db/utils"
)

// MergeFuncIterator is iterator based factory function
type MergeIteratorFactoryFunc[T any, C utils.Comparator[T]] func(iterators []utils.CloseableIterator[T], cmp C) (utils.CloseableIterator[T], error)

// RunGenerator is an interface for generating sorted runs from an input sequence.
type RunGenerator[T any, C utils.Comparator[T]] interface {
	GenerateRuns(input iter.Seq[T], createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error
}

// GoSortRunGenerator uses golang standard slices.sort
// runs on k-partitions and then merges using tournament tree if k > 1
type GoSortRunGenerator[T any, C utils.Comparator[T]] struct {
	runSize               int // maximum size of each run in bytes
	initialRunSize        int // estimated initial size of each run
	sliceBuffer           []T //
	comparatorFunc        C
	getByteSize           utils.GetByteSize[T]
	serialize             utils.Serializer[T]
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) TempFileWriter[T]
	k                     int // number of parallel sorts
}

func NewGoSortRunGenerator[T any, C utils.Comparator[T]](
	runSize int,
	initialRunSize int,
	comparatorFunc C,
	getByteSize utils.GetByteSize[T],
	serialize utils.Serializer[T],
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) TempFileWriter[T],
	k int,
) *GoSortRunGenerator[T, C] {
	return &GoSortRunGenerator[T, C]{
		runSize:               runSize,
		initialRunSize:        initialRunSize,
		comparatorFunc:        comparatorFunc,
		getByteSize:           getByteSize,
		serialize:             serialize,
		tempFileWriterFactory: tempFileWriterFactory,
		sliceBuffer:           make([]T, 0, initialRunSize),
		k:                     k,
	}
}

func (g *GoSortRunGenerator[T, C]) GenerateRuns(input iter.Seq[T], createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
	if input == nil {
		return fmt.Errorf("input iterator is nil")
	}
	currentSizeBytes := 0
	currentRunIndex := 0
	for t := range input {
		byteSize := g.getByteSize.GetByteSize(t)
		addedSize := currentSizeBytes + byteSize
		if addedSize > g.runSize {
			if err := g.sortAndFlush(currentRunIndex, createTmpFile); err != nil {
				return fmt.Errorf("failed to sort and flush: %v", err)
			}
			currentSizeBytes = 0
			currentRunIndex++
			g.sliceBuffer = nil // reset
		}
		if g.sliceBuffer == nil {
			g.sliceBuffer = make([]T, 0, g.initialRunSize)
		}
		g.sliceBuffer = append(g.sliceBuffer, t)
		currentSizeBytes += byteSize
	}
	// flush the remaining items
	if len(g.sliceBuffer) > 0 {
		if err := g.sortAndFlush(currentRunIndex, createTmpFile); err != nil {
			return fmt.Errorf("failed to sort and flush remaining items: %v", err)
		}
	}
	//
	return nil
}

// sortAndFlush sorts the current sliceBuffer and writes to temp file
func (g *GoSortRunGenerator[T, C]) sortAndFlush(currentRunIndex int, createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
	var tIt utils.CloseableIterator[T]
	if g.k > 1 { // parallel sort
		var err error
		var wg sync.WaitGroup
		sliceResultCh := make(chan []T, g.k)
		defer close(sliceResultCh)
		partSize := (len(g.sliceBuffer) + g.k - 1) / g.k
		for i, start := 0, 0; i < g.k; i, start = i+1, start+partSize {
			end := min(start+partSize, len(g.sliceBuffer))
			wg.Add(1)
			go func() {
				defer wg.Done()
				part := g.sliceBuffer[start:end]
				slices.SortFunc(part, g.comparatorFunc.Compare)
				sliceResultCh <- part
			}()
		}
		wg.Wait() // sort all
		its := make([]utils.CloseableIterator[T], g.k)
		for i := range g.k {
			sp := <-sliceResultCh
			its[i] = utils.NewSliceIt(sp)
		}
		tIt, err = NewTournamentIt(its, g.comparatorFunc)
		if err != nil {
			return fmt.Errorf("merger problem %v", err)
		}
	} else { // single sort
		slices.SortFunc(g.sliceBuffer, g.comparatorFunc.Compare)
		tIt = utils.NewSliceIt(g.sliceBuffer)
	}
	// write to file
	tmpFile, err := createTmpFile(currentRunIndex, 0)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tmpFile.Close()
	writer := g.tempFileWriterFactory(tmpFile, g.serialize)
	if err := writer.Write(tIt); err != nil {
		return fmt.Errorf("failed to write merged sequence to temporary file: %v", err)
	}
	return nil
}

// TNode: represents a node in the tournament tree used for k-way merging.
type TNode[T any] struct {
	value T   // value of the looser
	idx   int // source index of the looser
}

func (t TNode[T]) String() string {
	return fmt.Sprintf("TNode{value: %v, idx: %d}", t.value, t.idx)
}

// TournamentTree: a k-way merge algorithm using a tournament tree
type TournamentTree[T any, C utils.Comparator[T]] struct {
	tree   []TNode[T]
	cmp    C
	height int
}

func NewTournamentTree[T any, C utils.Comparator[T]](cmp C, fContestents []TNode[T]) *TournamentTree[T, C] {
	k := len(fContestents)
	height := 0
	for (1 << height) < k {
		height++
	}
	tree := make([]TNode[T], 1<<height)
	for i := 0; i < cap(tree); i++ {
		tree[i] = TNode[T]{value: utils.Zero[T](), idx: -1} // all sentinel
	}
	cand := make([]TNode[T], 0, len(fContestents))
	cand = append(cand, fContestents...)
	for h := height - 1; h >= 0; h-- {
		start := 1 << h
		winners := make([]TNode[T], 0, 1<<h)
		for i := 0; i < len(cand); i, start = i+2, start+1 {
			if i+1 >= len(cand) {
				winners = append(winners, cand[i]) // winner
				tree[start] = TNode[T]{idx: -1}    // loser
				continue
			}
			if cand[i].idx == -1 && cand[i+1].idx == -1 {
				winners = append(winners, TNode[T]{idx: -1}) // winner
				tree[start] = TNode[T]{idx: -1}              // loser
				continue
			}
			if cand[i].idx == -1 && cand[i+1].idx != -1 {
				winners = append(winners, cand[i+1]) //winner
				tree[start] = TNode[T]{idx: -1}      // loser
				continue
			}
			if cand[i].idx != -1 && cand[i+1].idx == -1 {
				winners = append(winners, cand[i]) //winner
				tree[start] = TNode[T]{idx: -1}    // loser
				continue
			}
			if cmp.Compare(cand[i].value, cand[i+1].value) < 0 {
				winners = append(winners, cand[i])
				tree[start] = cand[i+1]
			} else {
				winners = append(winners, cand[i+1])
				tree[start] = cand[i]
			}
		}
		cand = winners
	}
	tree[0] = cand[0] // // set winner
	return &TournamentTree[T, C]{tree: tree, cmp: cmp, height: height}
}

// Winner returns the current winner of the tournament
func (t *TournamentTree[T, C]) Winner() (winner TNode[T], ok bool) {
	return t.tree[0], t.tree[0].idx != -1
}

// Challenge updates the tournament tree with a new challenger
func (t *TournamentTree[T, C]) Challenge(challenger TNode[T], index int) {
	w := challenger
	pIdx := (len(t.tree) + index) / 2
	for pIdx >= 1 {
		p := t.tree[pIdx] //
		if p.idx == -1 {  // sentinel
			pIdx /= 2
			continue
		}
		if w.idx == -1 || t.cmp.Compare(w.value, p.value) >= 0 { // sentinel
			w, t.tree[pIdx] = p, w // swap
		}
		pIdx /= 2
	}
	t.tree[0] = w
}

type TournamentIterator[T any, C utils.Comparator[T]] struct {
	tt        *TournamentTree[T, C]
	iterators []utils.CloseableIterator[T]
}

func NewTournamentIt[T any, C utils.Comparator[T]](iterators []utils.CloseableIterator[T], cmp C) (*TournamentIterator[T, C], error) {
	contestents := make([]TNode[T], len(iterators))
	for i, it := range iterators {
		r, ok, err := it.Next()
		if !ok {
			contestents[i] = TNode[T]{idx: -1} // sentinel
			it.Close()
			continue
		}
		if err != nil {
			return nil, err
		}
		contestents[i] = TNode[T]{value: r, idx: i}
	}
	tournament := NewTournamentTree(cmp, contestents)
	return &TournamentIterator[T, C]{
		tt:        tournament,
		iterators: iterators,
	}, nil
}

func (m *TournamentIterator[T, C]) Next() (T, bool, error) {
	t, ok := m.tt.Winner()
	if !ok { // stop sentinel winner
		return t.value, ok, nil
	}
	nt, ok, err := m.iterators[t.idx].Next()
	if err != nil {
		return t.value, false, err
	}
	if !ok { // source exhausted
		m.tt.Challenge(TNode[T]{idx: -1}, t.idx)
	} else {
		m.tt.Challenge(TNode[T]{value: nt, idx: t.idx}, t.idx)
	}
	return t.value, true, nil
}

func (m *TournamentIterator[T, C]) Close() error {
	cErrs := make([]error, len(m.iterators))
	hasErr := false
	for i, it := range m.iterators {
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

func TournamentIteratorFactory[T any, C utils.Comparator[T]](iterators []utils.CloseableIterator[T], cmp C) (utils.CloseableIterator[T], error) {
	it, err := NewTournamentIt(iterators, cmp)
	return it, err
}

// Sorter is a generic external sorter that can sort large datasets that do not fit into memory.
// TODO: workload/resource  manager will assign memory and cpu to the sorter
type Sorter[T any, C utils.Comparator[T]] struct {
	comparatorFunc        C
	serialize             utils.Serializer[T]
	deserialize           utils.Deserializer[T]
	mergeFunc             MergeIteratorFactoryFunc[T, C]
	runGenerator          RunGenerator[T, C]
	tempFileReaderFactory func(file *os.File, deserialize utils.Deserializer[T]) (utils.CloseableIterator[T], error)
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) TempFileWriter[T]
	kWayMergeSize         int // max number of files that would be merged in each round
	directoryPath         string
	filePrefix            string
	fileExtension         string
	runStr                string
	mergeStr              string
	currentMergeRound     int
}

func NewSorter[T any, C utils.Comparator[T]](
	comparatorFunc C,
	serialize utils.Serializer[T],
	deserialize utils.Deserializer[T],
	mergeFunc MergeIteratorFactoryFunc[T, C],
	runGenerator RunGenerator[T, C],
	tempFileReaderFactory func(file *os.File, deserialize utils.Deserializer[T]) (utils.CloseableIterator[T], error),
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) TempFileWriter[T],
	directoryPath string,
	filePrefix string,
	fileExtension string,
	kWayMergeSize int,
) *Sorter[T, C] {
	return &Sorter[T, C]{
		comparatorFunc:        comparatorFunc,
		serialize:             serialize,
		deserialize:           deserialize,
		runGenerator:          runGenerator,
		mergeFunc:             mergeFunc,
		tempFileReaderFactory: tempFileReaderFactory,
		tempFileWriterFactory: tempFileWriterFactory,
		directoryPath:         directoryPath,
		filePrefix:            filePrefix,
		fileExtension:         fileExtension,
		kWayMergeSize:         kWayMergeSize,
		runStr:                "run",
		mergeStr:              "merge",
	}
}

func (s *Sorter[T, C]) Sort(input iter.Seq[T]) (utils.CloseableIterator[T], error) {
	if input == nil {
		return nil, fmt.Errorf("input iterator is nil")
	}
	// Initialize the run generator
	createTmpFile := func(currentRunIndex int, index int) (*os.File, error) {
		fileName := fmt.Sprintf("%s/%s_%s_%d_%05d.%s", s.directoryPath, s.filePrefix, s.runStr, currentRunIndex, index, s.fileExtension)
		return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	}
	err := s.runGenerator.GenerateRuns(input, createTmpFile)
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
	for len(files) > s.kWayMergeSize {
		// now chunk the files into batches of kWayMergeSize
		s.currentMergeRound++
		for i := 0; i < len(files); i += s.kWayMergeSize {
			end := min(i+s.kWayMergeSize, len(files))
			batch := files[i:end]
			mergeIt, err := s.mergeFiles(batch)
			if err != nil {
				return nil, fmt.Errorf("failed to merge files: %v", err)
			}
			err = s.flushMergeSequence(mergeIt, s.currentMergeRound, i)
			if err != nil {
				return nil, fmt.Errorf("failed to flush merge sequence: %v", err)
			}
			err = mergeIt.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to close merged iterator: %v", err)
			}
		}
		s.deleteFiles(files)
		isRunStage = false
		files, err = s.getFilesToMerge(isRunStage, s.currentMergeRound)
		if err != nil {
			return nil, fmt.Errorf("failed to get files to merge: %v", err)
		}
	}
	return s.mergeFiles(files)
}

func (s *Sorter[T, C]) mergeFiles(files []string) (utils.CloseableIterator[T], error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to merge")
	}
	if len(files) == 1 {
		file, err := s.openFile(files[0])
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", files[0], err)
		}
		return s.tempFileReaderFactory(file, s.deserialize)
	}
	slicesOfSeq := make([]utils.CloseableIterator[T], 0, len(files))
	for _, fileName := range files {
		file, err := s.openFile(fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
		}
		it, err := s.tempFileReaderFactory(file, s.deserialize)
		if err != nil {
			return nil, err
		}
		slicesOfSeq = append(slicesOfSeq, it)
	}
	return s.mergeFunc(slicesOfSeq, s.comparatorFunc)
}

func (s *Sorter[T, C]) getFilesToMerge(isRun bool, level int) ([]string, error) {
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

func (s *Sorter[T, C]) flushMergeSequence(it utils.CloseableIterator[T], currentMergeRound int, index int) error {
	// create a new temporary file for the merged output
	// realistically index is 6 decimal digits
	fileName := fmt.Sprintf("%s/%s_%s_%d_%06d.%s", s.directoryPath, s.filePrefix, s.mergeStr, currentMergeRound, index, s.fileExtension)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create merge output file %s: %v", fileName, err)
	}
	defer file.Close()
	// create a writer
	writer := s.tempFileWriterFactory(file, s.serialize)
	defer writer.Close()
	err = writer.Write(it)
	if err != nil {
		return fmt.Errorf("failed to write merged sequence to file %s: %v", fileName, err)
	}
	return nil
}

func (s *Sorter[T, C]) deleteFiles(files []string) error {
	// asynchronously delete files
	// FIXME: currently fire and forget
	go func() {
		for _, file := range files {
			filePath := filepath.Join(s.directoryPath, file)
			os.Remove(filePath)
		}
	}()
	return nil
}

func (s *Sorter[T, C]) openFile(fileName string) (*os.File, error) {
	// simple open file for reading
	filePath := filepath.Join(s.directoryPath, fileName)
	return os.Open(filePath)
}

func (s *Sorter[T, C]) Close() error {
	// FIXME: Implement any necessary cleanup logic here
	// removes all files in the directory with the same prefix
	return nil
}

// GoSortRunGenerator uses golang standard slices.sort
// runs on k-partitions and then merges using tournament tree if k > 1
type GoSortOrdRunGenerator[T cmp.Ordered] struct {
	runSize               int // maximum size of each run in bytes
	initialRunSize        int // estimated initial size of each run
	sliceBuffer           []T //
	getByteSize           utils.GetByteSize[T]
	serialize             utils.Serializer[T]
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) TempFileWriter[T]
	k                     int // number of parallel sorts
}

func NewGoSortOrdRunGenerator[T cmp.Ordered](
	runSize int,
	initialRunSize int,
	getByteSize utils.GetByteSize[T],
	serialize utils.Serializer[T],
	tempFileWriterFactory func(file *os.File, serialize utils.Serializer[T]) TempFileWriter[T],
	k int,
) *GoSortOrdRunGenerator[T] {
	return &GoSortOrdRunGenerator[T]{
		runSize:               runSize,
		initialRunSize:        initialRunSize,
		getByteSize:           getByteSize,
		serialize:             serialize,
		tempFileWriterFactory: tempFileWriterFactory,
		sliceBuffer:           make([]T, 0, initialRunSize),
		k:                     k,
	}

}

func (g *GoSortOrdRunGenerator[T]) GenerateRuns(input iter.Seq[T], createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
	if input == nil {
		return fmt.Errorf("input iterator is nil")
	}
	currentSizeBytes := 0
	currentRunIndex := 0
	for t := range input {
		byteSize := g.getByteSize.GetByteSize(t)
		addedSize := currentSizeBytes + byteSize
		if addedSize > g.runSize {
			if err := g.sortAndFlush(currentRunIndex, createTmpFile); err != nil {
				return fmt.Errorf("failed to sort and flush: %v", err)
			}
			currentSizeBytes = 0
			currentRunIndex++
			g.sliceBuffer = nil // reset
		}
		if g.sliceBuffer == nil {
			g.sliceBuffer = make([]T, 0, g.initialRunSize)
		}
		g.sliceBuffer = append(g.sliceBuffer, t)
		currentSizeBytes += byteSize
	}
	// flush the remaining items
	if len(g.sliceBuffer) > 0 {
		if err := g.sortAndFlush(currentRunIndex, createTmpFile); err != nil {
			return fmt.Errorf("failed to sort and flush remaining items: %v", err)
		}
	}
	//
	return nil
}

// sortAndFlush sorts the current sliceBuffer and writes to temp file
func (g *GoSortOrdRunGenerator[T]) sortAndFlush(currentRunIndex int, createTmpFile func(currentRunIndex int, index int) (*os.File, error)) error {
	var tIt utils.CloseableIterator[T]
	if g.k > 1 { // parallel sort
		var err error
		var wg sync.WaitGroup
		sliceResultCh := make(chan []T, g.k)
		defer close(sliceResultCh)
		partSize := (len(g.sliceBuffer) + g.k - 1) / g.k
		for i, start := 0, 0; i < g.k; i, start = i+1, start+partSize {
			end := min(start+partSize, len(g.sliceBuffer))
			wg.Add(1)
			go func() {
				defer wg.Done()
				part := g.sliceBuffer[start:end]
				slices.Sort(part)
				sliceResultCh <- part
			}()
		}
		wg.Wait() // sort all
		its := make([]utils.CloseableIterator[T], g.k)
		for i := range g.k {
			sp := <-sliceResultCh
			its[i] = utils.NewSliceIt(sp)
		}
		tIt, err = NewTournamentIt(its, utils.CmpFunc[T](cmp.Compare[T]))
		if err != nil {
			return fmt.Errorf("merger problem %v", err)
		}
	} else { // single sort
		slices.SortFunc(g.sliceBuffer, utils.CmpFunc[T](cmp.Compare[T]))
		tIt = utils.NewSliceIt(g.sliceBuffer)
	}
	// write to file
	tmpFile, err := createTmpFile(currentRunIndex, 0)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tmpFile.Close()
	writer := g.tempFileWriterFactory(tmpFile, g.serialize)
	if err := writer.Write(tIt); err != nil {
		return fmt.Errorf("failed to write merged sequence to temporary file: %v", err)
	}
	return nil
}
