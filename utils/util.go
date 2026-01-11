// Provides utils to work with I/O and serialization
package utils

import "iter"

// Zero element for generic types
func Zero[T any]() T {
	var zero T
	return zero
}

// Serialize function
type SerFunc[T any] func(s T, data []byte) (n int, err error)

// Deserialize function
type DeSerFunc[T any] func(data []byte) (st T, err error)

// Comparator function as interface to use in generic algorithms
// for inline implementations
type Comparator[T any] interface {
	Compare(a, b T) int
}

// Function to get the byte size of an item
type GetByteSize[T any] interface {
	GetByteSize(item T) int
}

// Serializer function as interface to use in generic algorithms
type Serializer[T any] interface {
	Serialize(item T, buf []byte) error
}

// Deserializer function as interface to use in generic algorithms
type Deserializer[T any] interface {
	Deserialize(data []byte) (T, error)
}

// Iterator interface
type Iterator[T any] interface {
	Next() (T, bool, error) // returns (value, hasNext, error)
}

// CloseableIterator interface
type CloseableIterator[T any] interface {
	Iterator[T]
	Close() error
}

func IteratorToSeq[T any](it Iterator[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		// ignore err on purpose
		for {
			t, ok, err := it.Next()
			if !ok || err != nil {
				return
			}
			if !yield(t) {
				return
			}
		}
	}
}

type SliceIt[T any] struct {
	idx int
	s   []T
}

func NewSliceIt[T any](slice []T) *SliceIt[T] {
	return &SliceIt[T]{
		idx: 0,
		s:   slice,
	}
}

func (s *SliceIt[T]) Next() (T, bool, error) {
	if s.idx < len(s.s) {
		next := s.s[s.idx]
		s.idx++
		return next, true, nil
	}
	return Zero[T](), false, nil
}

func (s *SliceIt[T]) Close() error {
	return nil
}
