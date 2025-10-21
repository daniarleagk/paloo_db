// Provides utils to work with I/O and serialization
package utils

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
