package anvil

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/yehan2002/errors"
)

const entryHeaderSize = 5

var zeroHeader [entryHeaderSize]byte

// Read reads the entry at the given position to `r`.
// `r` must not retain the reader passed to it.
func (f *File) Read(x, z uint8, r io.ReaderFrom) (n int64, err error) {
	if x > 31 || z > 31 {
		return 0, fmt.Errorf("anvil: invalid chunk position")
	}

	f.mux.RLock()
	defer f.mux.RUnlock()

	if f.header == nil {
		return 0, ErrClosed
	}

	entry := f.header.Get(x, z)

	if !entry.Generated() {
		return 0, ErrNotGenerated
	}

	offset := entry.OffsetBytes()
	var length int64
	var method CompressMethod
	var external bool

	if length, method, external, err = f.readEntryHeader(entry); err == nil {
		var src io.ReadCloser
		if src, err = f.readerForEntry(x, z, offset, length, external); err == nil {
			if src, err = method.decompressor(src); err == nil {
				n, err = r.ReadFrom(src)
				closeErr := src.Close()
				if err == nil {
					err = closeErr
				}
			}
		}
	}

	return 0, err
}

// readerForEntry returns a reader that reads the given entry.
// The reader is only valid until the next call to `Write`
func (f *File) readerForEntry(x, z uint8, offset, length int64, external bool) (src io.ReadCloser, err error) {
	if !external {
		return io.NopCloser(io.NewSectionReader(f.read, offset+entryHeaderSize, length)), nil
	} else if f.anvil != nil {
		return f.anvil.fs.ReadExternal(f.pos.Chunk(x, z))
	}
	return nil, ErrExternal
}

// readEntryHeader reads the header for the given entry.
func (f *File) readEntryHeader(entry *Entry) (length int64, method CompressMethod, external bool, err error) {
	header := [entryHeaderSize]byte{}
	if _, err = f.read.ReadAt(header[:], entry.OffsetBytes()); err == nil {
		// the first 4 bytes in the header holds the length of the data as a big endian uint32
		length = int64(binary.BigEndian.Uint32(header[:]))
		// the top bit of the 5th byte of the header indicates if the entry is stored externally
		external = header[4]&externalMask != 0
		// the lower bits hold the compression method used to compress the data
		method = CompressMethod(header[4] &^ externalMask)

		// reduce the length by 1 since we already read the compression byte
		length--

		if length/SectionSize > int64(entry.Size) {
			return 0, 0, false, errors.CauseStr(ErrCorrupted, "chunk size mismatch")
		}
	}
	return
}
