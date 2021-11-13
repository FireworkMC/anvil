package anvil

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/yehan2002/errors"
)

type file = File

// CachedFile a cached file
type CachedFile struct {
	*cachedFile
	closer sync.Once
}

type cachedFile struct {
	*file
	cache *Cache

	// useCount the number of users for this file
	// This should only be modified atomically while holding read or write lock of `cache`
	useCount int32
}

// Close closes the file.
// Calling any methods after calling this will cause a panic.
func (c *CachedFile) Close() (err error) {
	c.closer.Do(func() { err = c.cache.free(c.cachedFile); c.cachedFile = nil })
	return
}

// Cache a cached version of anvil.
type Cache struct {
	anvil *Anvil
	inUse map[Region]*cachedFile

	lru     *simplelru.LRU
	lruSize int

	mux sync.RWMutex
}

// get gets the anvil get for the given coords
func (a *Cache) get(rg Region) (f *cachedFile, err error) {
	a.mux.RLock()
	f, ok := a.getFile(rg)
	a.mux.RUnlock()

	if !ok {
		a.mux.Lock()
		defer a.mux.Unlock()
		// check if the file was opened while we were waiting for the mux
		if f, ok = a.getFile(rg); !ok {
			var file *File
			if v, ok := a.lru.Get(rg); ok { // check if the file is in the lru cache
				a.lru.Remove(rg)
				file = v.(*File)
			} else { // read file from the disk
				file, err = a.anvil.File(rg)
			}

			if err == nil {
				f = &cachedFile{file: file, cache: a, useCount: 1}
				a.inUse[rg] = f
			}
		}
	}

	return
}

func (a *Cache) free(f *cachedFile) (err error) {
	a.mux.RLock()
	newCount := atomic.AddInt32(&f.useCount, -1)
	a.mux.Unlock()

	if newCount == 0 {
		a.mux.Lock()
		defer a.mux.Unlock()
		if newCount = atomic.LoadInt32(&f.useCount); newCount == 0 {

			// evict the oldest file from the lru if adding a new element will cause a element to be evicted
			// We do this to insure the file gets closed properly and to free all associated resources
			if a.lru.Len() == a.lruSize {
				if _, old, ok := a.lru.RemoveOldest(); ok {
					if err = old.(*File).Close(); err != nil {
						err = errors.Wrap("anvil.Cache: error occurred while evicting file", err)
					}
				}
			}

			evicted := a.lru.Add(f.pos, f.file)
			if evicted {
				// This should never happen since we manually evicted the oldest element
				panic("anvil.Cache: File was incorrectly evicted")
			}

			delete(a.inUse, f.pos)
		}
	}
	return
}

func (a *Cache) getFile(rg Region) (f *cachedFile, ok bool) {
	f, ok = a.inUse[rg]
	if ok {
		atomic.AddInt32(&f.useCount, 1)
	}
	return
}

// Read reads the chunk data for the given location
func (a *Cache) Read(c Chunk, read io.ReaderFrom) (n int64, err error) {
	var f *cachedFile
	if f, err = a.get(c.Region()); err == nil {
		n, err = f.Read(uint8(c.X&0x1f), uint8(c.Z&0x1f), read)
	}
	return
}

// Write writes the chunk data for the given location
func (a *Cache) Write(c Chunk, p []byte) (err error) {
	var f *cachedFile
	if f, err = a.get(c.Region()); err == nil {
		err = f.Write(uint8(c.X&0x1f), uint8(c.Z&0x1f), p)
	}
	return
}
