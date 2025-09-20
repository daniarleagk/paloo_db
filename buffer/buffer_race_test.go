package buffer

import (
	"sync"
	"testing"

	"github.com/daniarleagk/paloo_db/io"
)

// this function is meant to be called with a -race flag
func TestConcurrentBufferAccessRace(t *testing.T) {
	storageId := "test"
	// simulate nested loop
	// buffer size is 10 and the storage contains with int from 0 to 8
	elementCount := 8
	numberOfLoops := 4
	bm, err := preloadStorageWithInts(storageId, 4, elementCount)
	if err != nil {
		t.Fatalf("Failed to preload storage: %v", err)
	}
	// go routines fixes the same block id with id 4
	var wg sync.WaitGroup
	// number of loops
	for i := range elementCount * numberOfLoops {
		wg.Add(1)
		// this go routine always fixes the same block id with id 4
		go func() {
			defer wg.Done()
			pageId := io.NewPageId(storageId, 4)
			frame, err := bm.Fix(pageId)
			if err != nil {
				t.Logf("Failed to fix page: %v", err)
				return
			}
			if frame == nil {
				t.Logf("Frame should not be nil after fixing")
				return
			}
			frame.frameRWmu.RLock() // acquire the frame lock
			defer frame.frameRWmu.RUnlock()
			t.Logf("Fixed frame static: %v with content: %d", frame.id, *frame.object)
			bm.Unfix(frame)
		}()
		// this go routine cycles through all pages
		wg.Add(1)
		go func() {
			defer wg.Done()
			pageId := io.NewPageId(storageId, int64(i%elementCount))
			frame, err := bm.Fix(pageId)
			if err != nil {
				t.Logf("Failed to fix page: %v", err)
				return
			}
			if frame == nil {
				t.Logf("Frame should not be nil after fixing")
				return
			}
			frame.frameRWmu.RLock() // acquire the frame lock
			defer frame.frameRWmu.RUnlock()
			t.Logf("Fixed frame cycle: %v with content: %d", frame.id, *frame.object)
			bm.Unfix(frame)
		}()
		//
	}
	wg.Wait()
}
