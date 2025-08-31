//go:build !race

package paloo_db

import (
	"errors"
	"fmt"
	"iter"
	"sync"
	"testing"
)

type Dev0WriteAheadLogger struct{}

func (d *Dev0WriteAheadLogger) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (d *Dev0WriteAheadLogger) Flush(lsn LogSequenceNumber) error {
	return nil
}

func (d *Dev0WriteAheadLogger) All() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {}
}

type QueueType int

const (
	lockBased QueueType = iota
	channelBased
)

func getQueue(qt QueueType, size int) ConcurrentQueue[*Frame[PageId, *int64]] {
	switch qt {
	case lockBased:
		return NewThreadSafeRingBuffer[*Frame[PageId, *int64]](size)
	case channelBased:
		return NewBoundedChannelBackedQueue[*Frame[PageId, *int64]](size)
	default:
		return nil
	}
}

// preloadStorageWithInts initializes a storage with integers for testing purposes.
func preloadStorageWithInts(storageId string, bufferSize int, count int) (*Buffer[PageId, *int64], error) {
	return preloadStorageWithIntsQueue(storageId, bufferSize, count, channelBased)
}

// preloadStorageWithInts initializes a storage with integers for testing purposes.
func preloadStorageWithIntsQueue(storageId string, bufferSize int, count int, qt QueueType) (*Buffer[PageId, *int64], error) {
	sm := NewStorageManager[PageId, *int64]()
	index := int64(0)
	ms := NewMapStorage[PageId, *int64](storageId, func() (PageId, error) {
		pageId := NewPageId(storageId, index)
		index++
		return pageId, nil
	})
	sm.Register(ms)
	bm := NewBuffer(bufferSize, sm, getQueue(qt, bufferSize), NewClockEvictionPolicy[PageId, *int64](), &Dev0WriteAheadLogger{})
	for i := range count {
		pageId, err := ms.Reserve()
		if err != nil {
			return nil, fmt.Errorf("Failed to reserve page ID: %v", err)
		}
		var c int64 = int64(i)
		content := &c
		err = ms.Write(pageId, content)
		if err != nil {
			return nil, fmt.Errorf("Failed to write content to storage: %v", err)
		}
	}
	return bm, nil
}

func printHashTableContent(t *testing.T, table *StaticHashFrameNodeTable[PageId, *int64]) {
	t.Log("Hash Table Content:")
	for node := range table.All() {
		if node != nil {
			t.Logf("Index %v: %v", node.id, &node.frame)
		} else {
			t.Logf("Index %v: nil", node)
		}
	}
}

// simple test fix frame and unfix it
func TestBufferFixUnfix(t *testing.T) {
	storageId := "test"
	bm, err := preloadStorageWithInts(storageId, 4, 4)
	if err != nil {
		t.Fatalf("Failed to preload storage: %v", err)
	}
	t.Log("Buffer manager initialized:", bm)
	pageId := NewPageId(storageId, 0)
	frame, err := bm.Fix(pageId)
	if err != nil {
		t.Fatalf("Failed to fix page: %v", err)
	}
	if frame == nil {
		t.Fatal("Frame should not be nil after fixing")
	}
	t.Logf("Fixed frame: %v with content: %d", frame.id, *frame.object)
	// Check if the content matches what we expect
	if *frame.object != 0 {
		t.Errorf("Expected content 0, got %d", *frame.object)
	}
	// Unfix the frame
	err = bm.Unfix(frame)
	if err != nil {
		t.Fatalf("Failed to unfix frame: %v", err)
	}
	t.Logf("Unfixed frame: %v", frame.id)
}

func TestBufferFixAllUnfixAll(t *testing.T) {
	count := 4
	storageId := "test"
	bm, err := preloadStorageWithInts(storageId, count, count)
	if err != nil {
		t.Fatalf("Failed to preload storage: %v", err)
	}
	for i := range count {
		pageId := NewPageId(storageId, int64(i))
		frame, err := bm.Fix(pageId)
		if err != nil {
			t.Fatalf("Failed to fix page: %v", err)
		}
		if frame == nil {
			t.Fatal("Frame should not be nil after fixing")
		}
		t.Logf("Fixed frame: %v with content: %d", frame.id, *frame.object)
	}

	for _, frame := range bm.frames {
		pageId := frame.id

		if frame == nil {
			t.Fatalf("Frame for page ID %v should not be nil", pageId)
		}
		err := bm.Unfix(frame)
		if err != nil {
			t.Fatalf("Failed to unfix frame: %v", err)
		}
		t.Logf("Unfixed frame: %v", frame.id)
	}
}

func TestBufferFixModifyUnfix(t *testing.T) {
	storageId := "test"
	bm, err := preloadStorageWithInts(storageId, 4, 8)
	if err != nil {
		t.Fatalf("Failed to preload storage: %v", err)
	}
	for i := range 4 {
		pageId := NewPageId(storageId, int64(i))
		frame, err := bm.Fix(pageId)
		if err != nil {
			t.Fatalf("Failed to fix page: %v", err)
		}
		if frame == nil {
			t.Fatal("Frame should not be nil after fixing")
		}
		bm.Update(frame, func(o *int64) *int64 {
			// Example modification: increment the value by 10
			newValue := *o + 10
			return &newValue
		})
		//
		err = bm.Unfix(frame)
		if err != nil {
			t.Fatalf("Failed to unfix frame: %v", err)
		}
	}
	printHashTableContent(t, bm.ht)
	// Verify that the content was modified
	t.Log("Verifying modified content in frames")
	for id, frame := range bm.frames {
		obj := frame.object
		t.Logf("Id  %v Frame %v %v", id, frame, *obj)
		// print refBit
		t.Logf("Frame %d reference bit: %t", id, frame.refBit.Load())
	}
	printHashTableContent(t, bm.ht)
	// now fix pages that will flush modified content
	// this will ensure that the modified content is flushed to the storage
	t.Log("Fixing pages to flush modified content")
	for i := range 2 {
		pageId := NewPageId(storageId, int64(i+4))
		frame, _ := bm.Fix(pageId)
		if *frame.object != int64(i+4) {
			t.Errorf("Expected modified content %d, got %d", i+4, *frame.object)
		}
		bm.Unfix(frame)
	}
	t.Log("Verifying modified content in frames")
	for id, frame := range bm.frames {
		obj := frame.object
		t.Logf("Id  %v Frame %v %v", id, frame, *obj)
		t.Logf("Frame %d reference bit: %t", id, frame.refBit.Load())
	}
	printHashTableContent(t, bm.ht)
	// Verify that the content was modified and flushed
	for i := range 4 {
		pageId := NewPageId(storageId, int64(i))
		frame, _ := bm.Fix(pageId)
		if frame == nil {
			t.Fatalf("Frame for page ID %v should not be nil", pageId)
		}
		if *frame.object != int64(i+10) {
			t.Errorf("Expected modified content %d, got %d", i+10, *frame.object)
		}
		bm.Unfix(frame)
	}
	t.Log("Verifying modified content in frames")
	for id, frame := range bm.frames {
		obj := frame.object
		t.Logf("Id  %v Frame %v %v", id, frame, *obj)
		t.Logf("Frame %d reference bit: %t", id, frame.refBit.Load())
	}
	printHashTableContent(t, bm.ht)
}

func TestConcurrentBufferAccessNL(t *testing.T) {
	storageId := "test"
	// simulate nested loop
	elementCount := 100
	numberOfLoops := 10
	bm, err := preloadStorageWithInts(storageId, 10, elementCount)
	if err != nil {
		t.Fatalf("Failed to preload storage: %v", err)
	}
	var wg sync.WaitGroup
	// number of loops
	for i := range elementCount * numberOfLoops {
		wg.Add(1)
		go func(pageIdx int) {
			defer wg.Done()
			pageId := NewPageId(storageId, int64(pageIdx%elementCount))
			frame, err := bm.Fix(pageId)
			if errors.Is(err, ErrNoEmptyOrEvicted) {
				// expected error
				t.Logf("Expected error occurred: %v", err)
				return
			}
			if err != nil {
				t.Errorf("Failed to fix page: %v", err)
				return
			}
			if frame == nil {
				t.Error("Frame should not be nil after fixing")
				return
			}
			frame.frameRWmu.RLock() // acquire the frame lock
			defer frame.frameRWmu.RUnlock()
			t.Logf("Fixed frame cycle: %v with content: %d", frame.id, *frame.object)
			bm.Unfix(frame)
		}(i)
	}
	wg.Wait()
}
