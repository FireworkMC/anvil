package anvil

import (
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/yehan2002/errors"
)

// File is a single anvil file.
// All functions can be called concurrently from multiple goroutines.
type File struct {
	mux    sync.RWMutex
	header *Header

	pos pos
	fs  *Fs

	size  int64
	write writer
	read  reader

	c  compressor
	cm CompressMethod
}

// OpenFile opens the given anvil file.
// If readonly is set any attempts to modify the file will return an error.
// If any data is stored in external files, any attempt to read it will return ErrExternal.
// If an attempt is made to write a data that is over 1MB after compression, ErrExternal will be returned.
// To allow reading and writing to external files use [Open] instead.
func OpenFile(path string, readonly bool) (f *File, err error) {
	var r reader
	var size int64
	if path, err = filepath.Abs(path); err == nil {
		if r, size, err = openFile(fs, path, readonly); err == nil {
			f, err = NewAnvil(0, 0, NewFs(fs), r, readonly, size)
		}
	}
	return
}

// NewAnvil reads an anvil file from the given ReadAtCloser.
// This has the same limitations as [OpenFile] if `fs` is nil.
// If fileSize is 0, no attempt is made to read any headers.
func NewAnvil(rgx, rgz int32, fs *Fs, r io.ReaderAt, readonly bool, fileSize int64) (a *File, err error) {
	// check if the file size is 0 or a multiple of 4096
	if fileSize&sectionSizeMask != 0 || (fileSize != 0 && fileSize < SectionSize*2) {
		return nil, ErrSize
	}

	if fs != nil && fs.fs == nil {
		return nil, errors.New("anvil: invalid anvil.Fs provided")
	}

	anvil := &File{fs: fs, pos: pos{x: rgx, z: rgz}, size: fileSize}

	if closer, ok := r.(reader); ok {
		anvil.read = closer
	} else {
		if !readonly {
			return nil, errors.Error("anvil: ReadFile: `r` must implement io.Closer to be opened in write mode")
		}
		anvil.read = &readAtCloser{ReaderAt: r}
	}

	if !readonly {
		var canWrite bool
		anvil.write, canWrite = r.(writer)
		if !canWrite {
			return nil, errors.Error("anvil: ReadFile: `r` must implement anvil.Writer to be opened in write mode")
		}
	}

	if fileSize == 0 { // fast path for empty files
		anvil.header = newHeader()
		anvil.header.clear()
		anvil.header.used = bitset.New(Entries)
		return anvil, nil
	}

	maxSection := uint(fileSize / SectionSize)
	if anvil.header, err = ReadHeader(r, maxSection); err != nil {
		return
	}

	return anvil, nil
}

// Read reads the entry at x,z to the given `reader`.
// `reader` must not retain the [io.Reader] passed to it.
// `reader` must not return before reading has completed.
func (a *File) Read(x, z uint8, reader io.ReaderFrom) (n int64, err error) {
	if x > 31 || z > 31 {
		return 0, fmt.Errorf("anvil: invalid chunk position")
	}

	a.mux.RLock()
	defer a.mux.RUnlock()

	if a.header == nil {
		return 0, ErrClosed
	}

	entry := a.header.Get(x, z)

	if !entry.Generated() {
		return 0, ErrNotGenerated
	}

	offset := entry.OffsetBytes()
	var length int64
	var method CompressMethod
	var external bool

	if length, method, external, err = a.readEntryHeader(entry); err == nil {
		var src io.ReadCloser
		if src, err = a.readerForEntry(x, z, offset, length, external); err == nil {
			if src, err = method.decompressor(src); err == nil {
				n, err = reader.ReadFrom(src)
				closeErr := src.Close()
				if err == nil {
					err = closeErr
				}
			}
		}
	}

	return 0, err
}

// Write updates the data for the entry at x,z to the given buffer.
// The given buffer is compressed and written to the anvil file.
// The compression method used can be changed using the [CompressMethod] method.
// If the data is larger than 1MB after compression, the data is stored externally.
// Calling this function with an empty buffer is the equivalent of calling `Remove(x,z)`.
func (a *File) Write(x, z uint8, b []byte) (err error) {
	if len(b) == 0 {
		return a.Remove(x, z)
	}

	a.mux.Lock()
	defer a.mux.Unlock()

	if a.header == nil {
		return nil
	}

	// check if the write is valid
	if err = a.checkWrite(x, z); err != nil {
		return err
	}

	// compress the given buffer
	var buf *buffer
	if buf, err = a.compress(b); err != nil {
		return errors.Wrap("anvil: error compressing data", err)
	}
	defer buf.Free()

	size := sections(uint(buf.Len()))

	if size > 255 {
		if a.fs == nil {
			return ErrExternal
		}

		cx, cz := a.pos.External(x, z)
		if err = a.fs.writeExternal(cx, cz, buf); err != nil {
			return
		}

		method := buf.compress
		buf.Free()
		buf.Write([]byte{0})
		buf.CompressMethod(method | externalMask)
		size = 1
	}

	// try to find space to store the data
	offset, hasSpace := a.header.FindSpace(size)

	// If we don't have enough space, grow the file to to make space
	if !hasSpace {
		if offset, err = a.growFile(size); err != nil {
			return errors.Wrap("anvil: unable to grow file", err)
		}
	}

	if err = buf.WriteAt(a.write, int64(offset)*SectionSize, true); err != nil {
		return errors.Wrap("anvil: unable to write entry data", err)
	}
	if err = a.write.Sync(); err != nil {
		return errors.Wrap("anvil: unable to write entry data", err)
	}

	return a.updateHeader(x, z, offset, uint8(size))
}

// Remove removes the given entry from the file.
func (a *File) Remove(x, z uint8) (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()

	if a.header == nil {
		return ErrClosed
	}

	// check if the write is valid
	if err = a.checkWrite(x, z); err == nil {
		a.header.Remove(x, z)
		// grow the file so that it has at least enough space to fit the header
		if _, err = a.growFile(0); err == nil {
			err = a.updateHeader(x, z, 0, 0)
		}
	}

	return
}

// CompressionMethod sets the compression method to be used by the writer.
func (a *File) CompressionMethod(m CompressMethod) (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()

	if a.header == nil {
		return ErrClosed
	}

	var c compressor
	if c, err = m.compressor(); err == nil {
		a.cm, a.c = m, c
	}
	return
}

// Close closes the anvil file.
func (a *File) Close() (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()
	if a.header != nil {
		a.header.Free()
		a.header = nil
		if a.write != nil {
			if err = a.write.Sync(); err != nil {
				return
			}
		}
		err = a.read.Close()
	}

	return
}

// readerForEntry returns a reader that reads the given entry.
// The reader is only valid until the next call to `Write`
func (a *File) readerForEntry(x, z uint8, offset, length int64, external bool) (src io.ReadCloser, err error) {
	if !external {
		return io.NopCloser(io.NewSectionReader(a.read, offset+entryHeaderSize, length)), nil
	} else if a.fs != nil {
		return a.fs.readExternal(a.pos.External(x, z))
	}
	return nil, ErrExternal
}

// readEntryHeader reads the header for the given entry.
func (a *File) readEntryHeader(entry *Entry) (length int64, method CompressMethod, external bool, err error) {
	header := [entryHeaderSize]byte{}
	if _, err = a.read.ReadAt(header[:], entry.OffsetBytes()); err == nil {
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

// checkWrite checks if the write is valid.
// This checks if x,z are within bounds
// and if the file was opened for writing and has not been closed.
func (a *File) checkWrite(x, z uint8) error {
	if x > 31 || z > 31 {
		return fmt.Errorf("anvil: invalid entry position")
	}

	if a.header == nil {
		return ErrClosed
	}

	if a.write == nil {
		return fmt.Errorf("anvil: file is opened in read-only mode")
	}

	return nil
}

// growFile grows the file to fit `size` more sections.
func (a *File) growFile(size uint) (offset uint, err error) {
	fileSize := a.size

	// make space for the header if the file does not have one.
	if fileSize < SectionSize*2 {
		fileSize = SectionSize * 2
	}

	offset = sections(uint(fileSize))
	a.size = int64(offset+size) * SectionSize // insure the file size is a multiple of 4096 bytes
	err = a.write.Truncate(a.size)
	return
}

// updateHeader updates the offset, size and timestamp in the main header for the entry at x,z.
func (a *File) updateHeader(x, z uint8, offset uint, size uint8) (err error) {
	if x > 31 || z > 31 {
		panic("invalid position")
	}

	headerOffset := int64(x)<<2 | int64(z)<<7
	entry := Entry{Offset: uint32(offset), Size: uint8(size), Timestamp: int32(time.Now().Unix())}

	if err = a.writeUint32At(uint32(offset)<<8|uint32(size), headerOffset); err != nil {
		return errors.Wrap("anvil: unable to update header", err)
	}

	a.header.Set(x, z, entry)

	if err = a.writeUint32At(uint32(entry.Timestamp), headerOffset+SectionSize); err != nil {
		return errors.Wrap("anvil: unable to update timestamp", err)
	}
	return
}

// writeUint32 writes the given uint32 at the given position
// and syncs the changes to disk.
func (a *File) writeUint32At(v uint32, offset int64) (err error) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	if _, err = a.write.WriteAt(tmp[:], offset); err == nil {
		err = a.write.Sync()
	}

	return
}

// compress compresses the given byte slice and writes it to a buffer.
func (a *File) compress(b []byte) (buf *buffer, err error) {
	if a.cm == 0 || a.c == nil {
		a.cm = DefaultCompression
		if a.c, err = a.cm.compressor(); err != nil {
			return nil, err
		}
	}

	buf = &buffer{}
	buf.CompressMethod(a.cm)
	a.c.Reset(buf)

	if _, err = a.c.Write(b); err == nil {
		if err = a.c.Close(); err == nil {
			return buf, nil
		}
	}

	return nil, err
}
