package anvil

import (
	"sync"
	"sync/atomic"
)

// CachedFile a cached anvil file
type CachedFile struct {
	*cachedFile
	closer sync.Once
}

type cachedFile struct {
	*File
	cache *Anvil

	// useCount the number of users for this file
	// This should only be modified atomically while holding read or write lock of `cache`
	useCount atomic.Int32
}

// Close closes the file.
// Calling any methods after calling this will cause a panic.
// This function can be called multiple times.
func (c *CachedFile) Close() (err error) {
	c.closer.Do(func() {
		err = c.cache.free(c.cachedFile)
		c.cachedFile = nil
	})
	return
}
