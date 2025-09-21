//go:build !race

package utils

import (
	"testing"
)

func TestRingBufferQueue(t *testing.T) {
	size := 4
	rb := NewThreadSafeRingBuffer[int](size)
	// Test Enqueue
	for i := range 5 {
		err := rb.Enqueue(i)
		t.Logf("Enqueue %d: %v", i, err)
		if i == 4 {
			// Expect error on the 5th enqueue
			if err == nil {
				t.Error("Expected error on Enqueue when full, but got none")
			}
		} else {
			// Expect no error for the first 4 enqueues
			if err != nil {
				t.Errorf("Unexpected error on Enqueue %d: %v", i, err)
			}
		}
	}
	isFull := rb.IsFull()
	if !isFull {
		t.Error("Expected buffer to be full after 4 enqueues, but it is not")
	}
	// Check size
	size = rb.Size()
	if size != 4 {
		t.Errorf("Expected size to be 4 after 4 enqueues, but got %d", size)
	}
	// Test Dequeue
	for i := range 5 {
		value, err := rb.Dequeue()
		t.Logf("Dequeue %d: %v", i, err)
		if i < 4 {
			if err != nil {
				t.Errorf("Unexpected error on Dequeue %d: %v", i, err)
			}
			if value != i {
				t.Errorf("Expected value %d, got %d", i, value)
			}
		} else if i == 4 {
			// Expect error on the 5th dequeue
			if err == nil {
				t.Error("Expected error on Dequeue when empty, but got none")
			}
		}
	}
	// Check if buffer is empty
	isEmpty := rb.IsEmpty()
	if !isEmpty {
		t.Error("Expected buffer to be empty after all dequeues, but it is not")
	}
	// check size
	size = rb.Size()
	if size != 0 {
		t.Errorf("Expected size to be 0 after all dequeues, but got %d", size)
	}
	// enqueue again to check if it handles empty state correctly
	for i := range 3 {
		err := rb.Enqueue(i + 10)
		t.Logf("Enqueue %d: %v", i+10, err)
		if err != nil {
			t.Errorf("Unexpected error on Enqueue %d: %v", i+10, err)
		}
	}
	// Check size after re-enqueueing
	size = rb.Size()
	if size != 3 {
		t.Errorf("Expected size to be 3 after re-enqueueing, but got %d", size)
	}
	// dequeue 2 elements
	for i := range 2 {
		value, err := rb.Dequeue()
		t.Logf("Dequeue %d: %v", i, err)
		if err != nil {
			t.Errorf("Unexpected error on Dequeue %d: %v", i+10, err)
		}
		expectedValue := i + 10
		if value != expectedValue {
			t.Errorf("Expected value %d, got %d", expectedValue, value)
		}
	}
	// Check size after dequeuing 2 elements
	size = rb.Size()
	if size != 1 {
		t.Errorf("Expected size to be 1 after dequeuing 2 elements, but got %d", size)
	}
}
