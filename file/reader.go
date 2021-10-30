package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/bits-and-blooms/bitset"
	"github.com/yehan2002/errors"
)

const (
	// ErrExternal returned if the chunk is in an external file.
	// This error is only returned if the region file was opened as a single file.
	ErrExternal = errors.Error("anvil/file: chunk is in separate file")
	// ErrNotGenerated returned if the chunk has not been generated yet
	ErrNotGenerated = errors.Error("anvil/file: chunk has not been generated")
)

const (
	compressionGzip = 1 + iota
	compressionZlib
	compressionNone

	externalMask = 0x80
)

// Dir TODO
type Dir struct{}

func (d *Dir) readExternal(x, z int) (io.ReadCloser, error) { return nil, ErrExternal }

// Reader a region file reader
type Reader struct {
	mux    sync.RWMutex
	header *header
	used   *bitset.BitSet
	reader io.ReaderAt
	dir    *Dir
}

// Read returns the chunk at the given position
func (r *Reader) Read(x, z int) (reader io.ReadCloser, err error) {
	if x < 0 || z < 0 || x > 31 || z > 31 {
		return nil, fmt.Errorf("anvil/file: invalid chunk position")
	}

	r.mux.RLock()
	defer r.mux.RUnlock()

	chunk := r.header.get(x, z)

	if chunk.location == 0 && chunk.size == 0 {
		return nil, ErrNotGenerated
	}

	offset := int64(chunk.location) * sectionSize

	header := [5]byte{}
	if _, err := r.reader.ReadAt(header[:], offset); err != nil {
		return nil, errors.Wrap("anvil/file: unable to read header", err)
	}

	length := binary.BigEndian.Uint32(header[:])

	if length/sectionSize > uint32(chunk.size) {
		return nil, errors.Error("anvil/file: chunk size mismatch")
	}

	var src io.ReadCloser

	if header[4]&externalMask == 0 {
		src = io.NopCloser(io.NewSectionReader(r.reader, offset+5, int64(length)))
	} else if r.dir != nil {
		if src, err = r.dir.readExternal(x, z); err != nil {
			return nil, err
		}
	} else {
		return nil, ErrExternal
	}

	return r.readerFor(src, header[4]&^externalMask)
}

func (r *Reader) readerFor(src io.ReadCloser, compression byte) (reader io.ReadCloser, err error) {
	switch compression {
	case compressionGzip:
		reader, err = newGzipReader(src)
	case compressionZlib:
		reader, err = newZlibReader(src)
	case compressionNone:
		reader = io.NopCloser(src)
	default:
		err = errors.Error("unsupported compression method")
	}
	return reader, errors.Wrap("anvil/file: unable to decompress", err)
}
