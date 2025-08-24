// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory

package paloo_db

import (
	"errors"
	"fmt"
	"hash/maphash"
	"iter"
	"sync"
	"sync/atomic"
	"unsafe"
)

var ErrNoEmptyOrEvicted = errors.New("paloo_db: no empty or evicted frames available")
var ErrFrameHashTableError = errors.New("paloo_db: frame not found in hash table")

// EvictionPolicy is interface for buffer eviction policy.
type EvictionPolicy[I DbObjectId, O any] interface {
	register(frame *Frame[I, O])                         // register
	selectVictim(bf *Buffer[I, O]) (*Frame[I, O], error) // select a victim page to evict
}

// ClockEvictionPolicy is a basic clock eviction policy.
type ClockEvictionPolicy[I DbObjectId, O any] struct {
	clockPointer atomic.Int32
	framesBits   []atomic.Bool // reference bits for each frame
	maxLoops     int           // max loops for checking reference
	//  bits if after two check still no false bit found take the next unfixed frame at most with one additional loop
}

func NewClockEvictionPolicy[I DbObjectId, O any](size int) *ClockEvictionPolicy[I, O] {
	return &ClockEvictionPolicy[I, O]{
		clockPointer: atomic.Int32{},            // initialize the clock pointer to 0
		framesBits:   make([]atomic.Bool, size), // initialize reference bits for each frame
		maxLoops:     2,
	}
}

func (c *ClockEvictionPolicy[I, O]) register(frame *Frame[I, O]) {
	// register the frame by setting its reference bit to true
	c.framesBits[frame.index].Store(true) // set the reference bit for the frame
}

func (c *ClockEvictionPolicy[I, O]) selectVictim(bf *Buffer[I, O]) (*Frame[I, O], error) {
	// select a victim frame using the clock algorithm
	size := len(c.framesBits)
	atomicPointer := c.clockPointer.Load() // get the current clock pointer
	atomicPointer %= int32(size)           // ensure the pointer is within bounds
	for i := int(atomicPointer); i < (size*c.maxLoops + 1); i++ {
		index := int(i) % size // calculate the index of the frame to check
		// update the clock pointer to the next index
		if c.framesBits[index].CompareAndSwap(true, false) { // if the reference bit is set, clear it and continue
			c.clockPointer.Store(int32(index + 1))
		} else { // false if the reference bit is not set, it means the frame is not in use
			frame := bf.frames[index] // get the frame from the buffer manager
			if frame.IsUnfixed() {    // if the frame is not fixed, return it as a victim
				return frame, nil
			}
		}
		//TODO implement backoff time after full revolution?
		// Or two handed clock
		// move reference bit into frame reduce overhead
	}
	return nil, fmt.Errorf("no victim frame found after %d loops", c.maxLoops)
}

// Buffer is a simple buffer pool implementation.
// Uses FIX do UnFIX protocol to work with storage content.
type Buffer[I DbObjectId, O any] struct {
	frames         []*Frame[I, O]                  // buffer pages list
	ht             *StaticHashFrameNodeTable[I, O] // static hash table for fast access to frames by id
	emptyFrames    ConcurrentQueue[*Frame[I, O]]   // queue of empty frames
	storageManager *StorageManager[I, O]           // used to store the pages in the persistent storage
	sxLock         sync.RWMutex                    // lock to protect concurrent access to the buffer manager
	evictionPolicy EvictionPolicy[I, O]            // eviction policy to select a victim frame
	seed           maphash.Seed                    // hasher for computing hash index for the frames
}

func NewBuffer[I DbObjectId, O any](size int, storageManager *StorageManager[I, O], emptyFramesQueue ConcurrentQueue[*Frame[I, O]]) *Buffer[I, O] {
	b := &Buffer[I, O]{}
	b.frames = make([]*Frame[I, O], 0, size)
	b.ht = NewStaticHashFrameNodeTable[I, O](size) // initialize the static hash table
	// initializes empty pages
	for i := range size {
		p := &Frame[I, O]{
			id:        Zero[I](),      // assuming I is a pointer type
			object:    Zero[O](),      // assuming O is a pointer type
			fixCount:  0,              // initially not fixed
			dirty:     0,              // initially not dirty
			frameRWmu: sync.RWMutex{}, // initialize the lock for the frame
			index:     i,              // index will be set later
		}
		b.frames = append(b.frames, p)
	}
	b.sxLock = sync.RWMutex{} // initialize the lock for the buffer manager
	b.seed = maphash.MakeSeed()
	b.storageManager = storageManager
	b.evictionPolicy = NewClockEvictionPolicy[I, O](size) // initialize the eviction policy
	b.emptyFrames = emptyFramesQueue
	for _, page := range b.frames {
		b.emptyFrames.Enqueue(page) // add all pages to the empty frames queue
	}
	return b
}

func (bm *Buffer[I, O]) computeHash(id I) int {
	// compute a hash index for the use hash from standard library map
	hasher := maphash.Hash{}
	hasher.SetSeed(bm.seed) // set the seed for the hasher
	bytes := fmt.Appendf(nil, "%v", id)
	hasher.Write(bytes)
	return int(hasher.Sum64() % uint64(bm.ht.size)) // return the hash index within the bounds of the hash table size
}

func (bm *Buffer[I, O]) Fix(id I) (*Frame[I, O], error) {
	// do not use the lock here instead we access ht table is  thread safe
	// compute hash index for the id
	hashIndex := bm.computeHash(id)
	// uses optimistic strategy
	// it id is not found in the hash table
	// it creates a new frame node with id
	frameNode, err := bm.ht.FindByIdIndexAndSet(hashIndex, id)
	if err != nil {
		return nil, ErrFrameHashTableError
	}
	for {
		frameState := atomic.LoadInt32(&frameNode.loadState) // wait for the state to be set to 0
		// return the Frame only if the state is 0
		if frameState == 0 {
			bm.evictionPolicy.register(frameNode.frame)
			return frameNode.frame, nil
		}
		// frame is not yet loaded
		// check atomically if the state is equal to 2 and set to 1
		if atomic.CompareAndSwapInt32(&frameNode.loadState, 2, 1) {
			// if the state is 2 then we can return the frame
			frame, err := bm.getEmptyFrame() // locate the empty frame
			if err != nil {
				// reset back
				atomic.StoreInt32(&frameNode.loadState, 2)
				bm.ht.removeByIdIndex(hashIndex, id)
				// remove from since we created proactively
				return nil, ErrNoEmptyOrEvicted
			}
			// lock exclusive access to the frame
			frame.frameRWmu.Lock()         // lock the frame for exclusive access
			defer frame.frameRWmu.Unlock() // unlock the frame after the operation
			// remove the frame from the idFrames map
			if !frame.IsEmpty() {
				// FIXME: better to return frame node store index of ht in the frame Node
				victimHash := bm.computeHash(frame.id)      // compute the hash index for the victim frame
				bm.ht.removeByIdIndex(victimHash, frame.id) // remove the frame from the hash table
			}
			frame.Clear() // clear the frame for reuse
			// now we can read the object from the storage manager
			o, err := bm.storageManager.Read(id)
			if err != nil {
				return nil, fmt.Errorf("failed to read object for id %v: %w", id, err)
			}
			frame.id = id
			frame.object = o                           // load the object into the frame
			frame.Fix()                                // increment the pin count
			bm.evictionPolicy.register(frame)          // register the frame in the eviction policy
			frameNode.frame = frame                    // update the frame in the hash table node
			atomic.StoreInt32(&frameNode.loadState, 0) // set the state to 0
			return frame, nil                          // return the frame with the object loaded
		}

	}
}

// used for new storage pages that not yet have data
// this is an empty storage page with a no data
// used to reserve a page in the storage manager
func (bm *Buffer[I, O]) FixEmpty(id I, object O) (*Frame[I, O], error) {
	return nil, nil // placeholder for fix logic, if needed
}

func (bm *Buffer[I, O]) Unfix(frame *Frame[I, O]) error {
	frame.Unfix() // decrement the pin count
	return nil    // placeholder for unfix logic, if needed
}

func (bm *Buffer[I, O]) Flush(frame *Frame[I, O]) error {
	// flush if dirty
	if frame.IsDirty() {
		frame.frameRWmu.RLock()         // lock the frame for exclusive access
		defer frame.frameRWmu.RUnlock() // unlock the frame after the operation
		if err := bm.storageManager.Write(frame.id, frame.object); err != nil {
			return fmt.Errorf("failed to flush frame %v: %w", frame.id, err)
		}
		frame.UnsetDirty() // mark the frame as not dirty after flushing
	}
	return nil // placeholder for flush logic, if needed
}

// update using the update function
// it sets the dirty flag
func (bm *Buffer[I, O]) Update(frame *Frame[I, O], updateFunction func(O) O) error {
	frame.frameRWmu.Lock()         // lock the frame for exclusive access
	defer frame.frameRWmu.Unlock() // unlock the frame after the operation
	if frame.fixCount == 0 {
		return fmt.Errorf("frame %v is not fixed, cannot update", frame.id)
	}
	// apply the update function to the object in the frame
	frame.object = updateFunction(frame.object)
	frame.SetDirty() // mark the frame as dirty
	return nil       // return nil if successful
}

// either get an empty frame or select a victim frame to evict
func (bm *Buffer[I, O]) getEmptyFrame() (*Frame[I, O], error) {
	// get an empty frame from the queue
	// guarded by lock in dequeue method
	frame, err := bm.emptyFrames.Dequeue()
	if err != nil {
		// no empty frame available, we need to evict a frame
		frame, err = bm.getVictim()
		if err != nil {
			return nil, fmt.Errorf("no available frames to fix page: %w", err)
		}
		// flush the victim frame if it is dirty
		if err := bm.Flush(frame); err != nil {
			return nil, fmt.Errorf("failed to flush victim frame %v: %w", frame.id, err)
		}
	}
	return frame, nil // return the empty frame
}

func (bm *Buffer[I, O]) getVictim() (*Frame[I, O], error) {
	// default eviction policy is clock
	return bm.evictionPolicy.selectVictim(bm) // use the eviction policy to select a victim frame
}

// Frame represents a single frame in the buffer manager.
type Frame[I comparable, O any] struct {
	object    O
	fixCount  int32        // atomic counter
	id        I            // id of the frame, used to identify the object in the storage
	dirty     int32        // indicates if the frame is dirty (modified)
	frameRWmu sync.RWMutex // to protect concurrent access to the frame
	index     int          // index of the frame in the buffer manager
}

func (f *Frame[I, O]) Fix() O {
	// increment atomic
	atomic.AddInt32(&(f.fixCount), 1) // increment the pin count
	return f.object                   // return the object in the frame
}

func (f *Frame[I, O]) Unfix() {
	// check if the fix count is greater than zero atomically
	// compare and swap if 0 to 0
	if atomic.CompareAndSwapInt32(&(f.fixCount), 0, 0) {
		return
	} else {
		atomic.AddInt32(&(f.fixCount), -1) // decrement the pin count
	}
}

func (f *Frame[I, O]) IsUnfixed() bool {
	return atomic.LoadInt32(&(f.fixCount)) == 0
}

func (f *Frame[I, O]) SetDirty() {
	atomic.StoreInt32(&(f.dirty), 1)
}

func (f *Frame[I, O]) UnsetDirty() {
	atomic.StoreInt32(&(f.dirty), 0)
}

func (f *Frame[I, O]) IsDirty() bool {
	return atomic.LoadInt32(&(f.dirty)) == 1
}

func (f *Frame[I, O]) Clear() {
	f.id = Zero[I]()     // reset the id to zero value
	f.object = Zero[O]() // reset the object to zero value
	// set atomic
	// write 0 atomically
	atomic.StoreInt32(&(f.fixCount), 0) // reset the pin count
	atomic.StoreInt32(&(f.dirty), 0)    // mark as not dirty
}

func (f *Frame[I, O]) IsEmpty() bool {
	return atomic.LoadInt32(&(f.fixCount)) == 0 && atomic.LoadInt32(&(f.dirty)) == 0 && f.id == Zero[I]()
}

func (f *Frame[I, O]) String() string {
	return fmt.Sprintf("Frame[Id: %v, FixCount: %d, Dirty: %t, Object: %v]", f.id, f.fixCount, atomic.LoadInt32(&(f.dirty)) == 1, f.object)
}

// FrameNode represents static chain hashing node.
type FrameNode[I comparable, O any] struct {
	id          I
	frame       *Frame[I, O]
	loadState   int32 // load state of the frame
	isAnchor    bool  // indicates if this is an anchor node
	bucketIndex int   // index of the bucket in the hash table
	// next is a pointer to the next node in the chain
	// using unsafe.Pointer for atomic operations
	next unsafe.Pointer
}

func (fn *FrameNode[I, O]) String() string {
	return fmt.Sprintf("FrameNode[Id: %v, Frame: %v, LoadState: %d, IsAnchor: %t, BucketIndex: %d]", fn.id, fn.frame, fn.loadState, fn.isAnchor, fn.bucketIndex)
}

// StaticHashFrameNodeTable manages frame nodes for a fast associative access.
type StaticHashFrameNodeTable[I comparable, O any] struct {
	nodes []*FrameNode[I, O] // array of pointers to FrameNodes hash function returns an index
	size  int
	lock  sync.RWMutex
}

// NewStaticHashTable creates a new static hash table
func NewStaticHashFrameNodeTable[I comparable, O any](size int) *StaticHashFrameNodeTable[I, O] {
	ht := &StaticHashFrameNodeTable[I, O]{
		nodes: make([]*FrameNode[I, O], size),
		size:  size,
		lock:  sync.RWMutex{},
	}
	for i := range size {
		ht.nodes[i] = &FrameNode[I, O]{id: Zero[I](), frame: nil, bucketIndex: i, isAnchor: true, next: nil}
	}
	return ht
}

// if there is no node in chain with a key equals to a key then it will add a new node at the end of the chain
func (ht *StaticHashFrameNodeTable[I, O]) FindByIdIndexAndSet(index int, id I) (*FrameNode[I, O], error) {
	ht.lock.RLock()
	defer ht.lock.RUnlock()
	if index < 0 || index >= ht.size {
		return nil, errors.New("index out of bounds")
	}
	anchor := ht.nodes[index]
	for {
		current := (*FrameNode[I, O])(atomic.LoadPointer(&anchor.next))
		if current == nil {
			// if no more nodes in chain create a dummy node with a key and empty value
			newNode := &FrameNode[I, O]{id: id, frame: nil, bucketIndex: index, isAnchor: false, loadState: 2, next: nil}
			if atomic.CompareAndSwapPointer(&anchor.next, nil, unsafe.Pointer(newNode)) {
				return newNode, nil
			}
			continue
		}
		if current.id == id {
			return current, nil
		}
		anchor = current
	}
}

// removeByIdIndex removes a node by id from the chain at the given index
// it locks the table for exclusive access, however it locks only the bucket chain
// this reduce lock contention, other threads can access other buckets
func (ht *StaticHashFrameNodeTable[I, O]) removeByIdIndex(index int, id I) error {
	ht.lock.Lock()
	defer ht.lock.Unlock()
	if index < 0 || index >= ht.size {
		return errors.New("index out of bounds")
	}
	anchor := ht.nodes[index]
	for {
		current := (*FrameNode[I, O])(atomic.LoadPointer(&anchor.next))
		if current == nil {
			return fmt.Errorf("node with id %v not found", id)
		}
		node := (*FrameNode[I, O])(current)
		if node.id == id {
			// found the node, remove it
			atomic.StorePointer(&anchor.next, atomic.LoadPointer(&node.next))
			return nil
		}
		anchor = current
	}
}

// returns all hash table nodes
func (ht *StaticHashFrameNodeTable[I, O]) All() iter.Seq[*FrameNode[I, O]] {
	return (func(yield func(*FrameNode[I, O]) bool) {
		ht.lock.RLock()
		defer ht.lock.RUnlock()
		for _, node := range ht.nodes {
			current := node
			for current != nil {
				if current.isAnchor {
					// Skip anchor nodes
					current = (*FrameNode[I, O])(atomic.LoadPointer(&current.next))
					continue
				}
				if !yield(current) {
					return // stop iteration if yield returns false
				}
				current = (*FrameNode[I, O])(atomic.LoadPointer(&current.next))
			}
		}
	})
}
