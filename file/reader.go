package file

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/yehan2002/errors"
)

const (
	// ErrExternal returned if the chunk is in an external file.
	// This error is only returned if the region file was opened as a single file.
	ErrExternal = errors.Error("anvil/file: chunk is in separate file")
	// ErrNotGenerated returned if the chunk has not been generated yet
	ErrNotGenerated = errors.Error("anvil/file: chunk has not been generated")
	// ErrSize the given file has an invalid file size
	ErrSize = errors.Error("anvil/file: invalid file size")
)

// Read returns the chunk at the given position
func (f *File) Read(x, z int) (reader io.ReadCloser, err error) {
	if x < 0 || z < 0 || x > 31 || z > 31 {
		return nil, fmt.Errorf("anvil/file: invalid chunk position")
	}

	f.mux.RLock()
	defer f.mux.RUnlock()

	chunk := f.header.Get(x, z)

	if chunk.Offset == 0 && chunk.Size == 0 {
		return nil, ErrNotGenerated
	}

	offset := int64(chunk.Offset) * SectionSize

	header := [5]byte{}
	if _, err := f.read.ReadAt(header[:], offset); err != nil {
		return nil, errors.Wrap("anvil/file: unable to read header", err)
	}

	length := binary.BigEndian.Uint32(header[:])

	if length/SectionSize > uint32(chunk.Size) {
		return nil, errors.Error("anvil/file: chunk size mismatch")
	}

	var src io.ReadCloser

	method := CompressMethod(header[4] &^ externalMask)
	external := header[4]&externalMask != 0

	if !external {
		src = io.NopCloser(io.NewSectionReader(f.read, offset+5, int64(length)))
	} else if f.dir != nil {
		if src, err = f.dir.ReadExternal(x, z); err != nil {
			return nil, err
		}
	} else {
		return nil, ErrExternal
	}

	return method.decompressor(src)
}
