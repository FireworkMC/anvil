package anvil

import (
	"io"
	"sync"
	"sync/atomic"
)

// CachedFile a cached anvil file
type CachedFile struct {
	*cachedFile

	closeMux sync.RWMutex
	closed   bool
}

type cachedFile struct {
	*File
	cache *Anvil

	// useCount the number of users for this file
	// This should only be modified while holding read or write lock of `cache`
	useCount atomic.Int32
}

// Close closes the file.
// This function can be called multiple times.
// This will block until all Read and Write calls return.
func (c *CachedFile) Close() (err error) {
	c.closeMux.Lock()
	defer c.closeMux.Unlock()

	if !c.closed {
		err = c.cache.free(c.cachedFile)
		c.closed = true
	}

	return
}

// Read reads the entry at x,z to the given `reader`.
// `reader` must not retain the [io.Reader] passed to it.
// `reader` must not return before reading has completed.
func (c *CachedFile) Read(x, z uint8, reader io.ReaderFrom) (n int64, err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return 0, ErrClosed
	}

	return c.cachedFile.Read(x, z, reader)
}

// Write updates the data for the entry at x,z to the given buffer.
// The given buffer is compressed and written to the anvil file.
// The compression method used can be changed using the [CompressMethod] method.
// If the data is larger than 1MB after compression, the data is stored externally.
// Calling this function with an empty buffer is the equivalent of calling `Remove(x,z)`.
func (c *CachedFile) Write(x, z uint8, b []byte) (err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return ErrClosed
	}

	return c.cachedFile.Write(x, z, b)
}

// Remove removes the given entry from the file.
func (c *CachedFile) Remove(x, z uint8) (err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return ErrClosed
	}

	return c.cachedFile.Remove(x, z)
}

// CompressionMethod sets the compression method to be used by the writer.
func (c *CachedFile) CompressionMethod(m CompressMethod) (err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return ErrClosed
	}

	return c.cachedFile.CompressionMethod(m)
}
