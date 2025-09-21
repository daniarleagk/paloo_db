//go:build !race

package buffer

import (
	"sync/atomic"
	"testing"

	"github.com/daniarleagk/paloo_db/io"
)

func BenchmarkConcurrentBufferAccessLockBased(b *testing.B) {
	storageId := "test"
	// simulate nested loop
	// buffer size is 10 and the storage contains with int from 0 to 8
	elementCount := 8
	bm, err := preloadStorageWithInts(storageId, 4, elementCount)
	if err != nil {
		b.Fatalf("Failed to preload storage: %v", err)
	}
	// atomic counter
	var idx int32
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			caller := func() {
				i := atomic.LoadInt32(&idx)
				pageId := io.NewPageId(storageId, int64(int(i)%elementCount))
				b.StopTimer()
				frame, err := bm.Fix(pageId)
				if err != nil {
					// if atomic.LoadInt32(&idx)%10000 == 0 {
					// 	b.Logf("Failed to fix page: %v", err)
					// }
					//b.Logf("Failed to fix page: %v", err)
					return
				}
				b.StartTimer()
				frame.frameRWmu.RLock() // acquire the frame lock
				defer frame.frameRWmu.RUnlock()
				bm.Unfix(frame)
				atomic.AddInt32(&idx, 1)
				// if atomic.LoadInt32(&idx)%10000 == 0 {
				// 	b.Logf("inc %d Fixed frame: %v with content: %d", atomic.LoadInt32(&idx), frame.id, *frame.object)
				// }
			}
			caller()
		}
	})
	b.Logf("Final index: %d", atomic.LoadInt32(&idx))
}

func BenchmarkConcurrentBufferAccessChannel(b *testing.B) {
	storageId := "test"
	// simulate nested loop
	// buffer size is 10 and the storage contains with int from 0 to 8
	elementCount := 8
	bm, err := preloadStorageWithIntsQueue(storageId, 4, elementCount, channelBased)
	if err != nil {
		b.Fatalf("Failed to preload storage: %v", err)
	}
	// atomic counter
	var idx int32
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			caller := func() {
				i := atomic.LoadInt32(&idx)
				pageId := io.NewPageId(storageId, int64(int(i)%elementCount))
				b.StopTimer()
				frame, err := bm.Fix(pageId)
				if err != nil {
					// if atomic.LoadInt32(&idx)%10000 == 0 {
					// 	b.Logf("Failed to fix page: %v", err)
					// }
					//b.Logf("Failed to fix page: %v", err)
					return
				}
				b.StartTimer()
				frame.frameRWmu.RLock() // acquire the frame lock
				defer frame.frameRWmu.RUnlock()
				bm.Unfix(frame)
				atomic.AddInt32(&idx, 1)
				// if atomic.LoadInt32(&idx)%10000 == 0 {
				// 	b.Logf("inc %d Fixed frame: %v with content: %d", atomic.LoadInt32(&idx), frame.id, *frame.object)
				// }
			}
			caller()
		}
	})
	b.Logf("Final index: %d", atomic.LoadInt32(&idx))
}
