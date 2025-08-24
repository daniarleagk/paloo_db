// Provides utils to work with I/O and serialization
package paloo_db

// Zero element for generic types
func Zero[T any]() T {
	var zero T
	return zero
}

// Serialize function
type SerFunc[T any] func(s T, data []byte) (n int, err error)

// Deserialize function
type DeSerFunc[T any] func(data []byte) (st T, err error)
