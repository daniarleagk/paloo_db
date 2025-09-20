package utils

// This file provides custom collection implementations.
import (
	"errors"
	"sync"
)

var ErrQueueFull = errors.New("queue is full")
var ErrQueueEmpty = errors.New("queue is empty")

// ConcurrentQueue interface
type ConcurrentQueue[T any] interface {
	Enqueue(value T) error
	Dequeue() (T, error)
}

// ThreadSafeRingBufferQueue is a generic, thread-safe bounded queue
// uses locks on each operation
type ThreadSafeRingBufferQueue[T any] struct {
	data        []T
	head, tail  int
	size, count int
	lock        sync.Mutex
}

// NewThreadSafeRingBuffer creates a new thread-safe ring buffer
func NewThreadSafeRingBuffer[T any](size int) *ThreadSafeRingBufferQueue[T] {
	return &ThreadSafeRingBufferQueue[T]{
		data:  make([]T, size),
		size:  size,
		tail:  0,
		head:  0,
		count: 0,
		lock:  sync.Mutex{},
	}
}

// Enqueue adds an element to the buffer
func (q *ThreadSafeRingBufferQueue[T]) Enqueue(value T) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.count == q.size {
		return ErrQueueFull
	}
	q.data[q.tail] = value
	q.tail = (q.tail + 1) % q.size
	q.count++
	return nil
}

// Dequeue removes and returns the front element
func (q *ThreadSafeRingBufferQueue[T]) Dequeue() (T, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.count == 0 {
		var zero T
		return zero, ErrQueueEmpty
	}
	value := q.data[q.head]
	q.head = (q.head + 1) % q.size
	q.count--
	return value, nil
}

// IsEmpty checks if the buffer is empty
func (q *ThreadSafeRingBufferQueue[T]) IsEmpty() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.count == 0
}

// IsFull checks if the buffer is full
func (q *ThreadSafeRingBufferQueue[T]) IsFull() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.count == q.size
}

// Size returns the number of elements in the buffer
func (q *ThreadSafeRingBufferQueue[T]) Size() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.count
}

// BoundedChannelBackedQueue is a bounded queue based on bounded channel.
type BoundedChannelBackedQueue[T any] struct {
	ch chan T
}

// NewBoundedChannelBackedQueue constructor
func NewBoundedChannelBackedQueue[T any](size int) *BoundedChannelBackedQueue[T] {
	return &BoundedChannelBackedQueue[T]{
		ch: make(chan T, size),
	}
}

// Enqueue
func (q BoundedChannelBackedQueue[T]) Enqueue(value T) error {
	select {
	case q.ch <- value:
		return nil
	default:
		return ErrQueueFull
	}
}

// Dequeue
func (q BoundedChannelBackedQueue[T]) Dequeue() (T, error) {
	select {
	case value := <-q.ch:
		return value, nil
	default:
		var zero T
		return zero, ErrQueueEmpty
	}
}
