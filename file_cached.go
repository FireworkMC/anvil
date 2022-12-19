package anvil

import (
	"io"
	"sync"
)

// cachedFile a cached anvil file
type cachedFile struct {
	*file

	closeMux sync.RWMutex
	closed   bool
}

// Close closes the file.
// This function can be called multiple times.
// This will block until all Read and Write calls return.
func (c *cachedFile) Close() (err error) {
	c.closeMux.Lock()
	defer c.closeMux.Unlock()

	if !c.closed {
		err = c.file.cache.free(c.file)
		c.closed = true
	}

	return
}

// Read reads the entry at x,z to the given `reader`.
// `reader` must not retain the [io.Reader] passed to it.
// `reader` must not return before reading has completed.
func (c *cachedFile) Read(x, z uint8, reader io.ReaderFrom) (n int64, err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return 0, ErrClosed
	}

	return c.file.Read(x, z, reader)
}

// Write updates the data for the entry at x,z to the given buffer.
// The given buffer is compressed and written to the anvil file.
// The compression method used can be changed using the [CompressMethod] method.
// If the data is larger than 1MB after compression, the data is stored externally.
// Calling this function with an empty buffer is the equivalent of calling [File.Remove](x,z).
func (c *cachedFile) Write(x, z uint8, b []byte) (err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return ErrClosed
	}

	return c.file.Write(x, z, b)
}

// Remove removes the given entry from the file.
func (c *cachedFile) Remove(x, z uint8) (err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return ErrClosed
	}

	return c.file.Remove(x, z)
}

// CompressionMethod sets the compression method to be used by the writer.
func (c *cachedFile) CompressionMethod(m CompressMethod) (err error) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return ErrClosed
	}

	return c.file.CompressionMethod(m)
}

// Info gets information stored in the anvil header for the given entry.
func (c *cachedFile) Info(x, z uint8) (entry Entry, exists bool) {
	c.closeMux.RLock()
	defer c.closeMux.RUnlock()
	if c.closed {
		return
	}
	return c.file.Info(x, z)
}
