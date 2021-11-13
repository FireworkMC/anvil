package anvil

import (
	"encoding/binary"
	"fmt"
	"io"
	"runtime"

	"github.com/yehan2002/errors"
)

const entryHeaderSize = 5

var zeroHeader [entryHeaderSize]byte

// Read reads the entry at the given position to `r`.
// `r` must not retain the reader passed to it.
func (f *File) Read(x, z uint8, r io.ReaderFrom) (n int64, err error) {
	var src io.ReadCloser
	if src, err = f.ReaderFor(x, z); err == nil {
		n, err = r.ReadFrom(src)
		src.Close()
	}
	return 0, err
}

// ReaderFor returns a reader that reads the chunk at the given position.
// The returned reader must be closed or any calls to Write may hang forever.
// `Read` should be used in most cases.
func (f *File) ReaderFor(x, z uint8) (reader io.ReadCloser, err error) {
	if x > 31 || z > 31 {
		return nil, fmt.Errorf("anvil: invalid chunk position")
	}

	f.mux.RLock()

	if f.header == nil {
		f.mux.RUnlock()
		return nil, ErrClosed
	}

	entry := f.header.Get(x, z)

	if !entry.Generated() {
		f.mux.RUnlock()
		return nil, ErrNotGenerated
	}

	offset := entry.OffsetBytes()
	var length int64
	var method CompressMethod
	var external bool

	if length, method, external, err = f.readEntryHeader(entry); err == nil {
		if reader, err = f.readerForEntry(x, z, offset, length, external); err == nil {
			if reader, err = method.decompressor(reader); err == nil {
				mr := &muxReader{ReadCloser: reader, mux: &f.mux}
				runtime.SetFinalizer(mr, func(m *muxReader) { m.Close() })
				return mr, nil
			}
		}
	}

	f.mux.RUnlock()
	return nil, err
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
