// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory of this source tree.
package paloo_db

import (
	"fmt"
	"iter"
)

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

type RunGenerator[T any] struct {
	comparatorFunc  func(a, b T) int
	getByteSize     func(item T) int
	serialize       func(item T) ([]byte, error) //
	deserialize     func(data []byte) (T, error)
	maxRunSize      int // in bytes
	directoryPath   string
	filePrefix      string
	fileExtension   string
	writeBufferSize int
	sliceBuffer     []T
}

func (g *RunGenerator[T]) GenerateRuns(input iter.Seq[T]) error {
	if input == nil {
		return fmt.Errorf("input iterator is nil")
	}
	currentSizeBytes := 0
	for t := range input {
		byteSize := g.getByteSize(t)
		addedSize := currentSizeBytes + byteSize
		if addedSize > g.maxRunSize {
			// sort and flush
			// reset slice buffer
			g.sliceBuffer = nil
		}
		if g.sliceBuffer == nil {
			g.sliceBuffer = make([]T, 0, g.maxRunSize/byteSize)
		}
		g.sliceBuffer = append(g.sliceBuffer, t)

	}
	// Implement run generation logic here
	// For now, we'll just return the input as-is
	return nil
}

type KWayMerger[T any] struct {
	comparatorFunc  func(a, b T) int
	directoryPath   string
	filePrefix      string
	fileExtension   string
	readBufferSize  int
	writeBufferSize int
	kWayMergeSize   int
}

func (m *KWayMerger[T]) MergeRuns() (iter.Seq[T], error) {
	// Implement merging logic here
	return nil, nil
}
