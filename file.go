package anvil

import (
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"sync/atomic"

	"github.com/bits-and-blooms/bitset"
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

// File is a single anvil file.
// All functions can be called concurrently from multiple goroutines.
type File interface {
	// Read reads the content of the entry at the given coordinates to a
	// a byte slice and returns it.
	Read(x, z uint8) (buf []byte, err error)

	// ReadTo reads the entry at x,z to the given [io.ReaderFrom].
	// `reader` must not retain the [io.Reader] passed to it.
	// `reader` must not return before reading has completed.
	ReadTo(x, z uint8, reader io.ReaderFrom) (n int64, err error)

	// ReadWith reads the entry at x,z to using the given readFn.
	// `readFn` must not retain the [io.Reader] passed to it.
	// `readFn` must not return before reading has completed.
	ReadWith(x, z uint8, readFn func(io.Reader) error) (err error)

	// Write updates the data for the entry at x,z to the given buffer.
	// The given buffer is compressed and written to the anvil file.
	// The compression method used can be changed using the [CompressMethod] method.
	// If the data is larger than 1MB after compression, the data is stored externally.
	// Calling this function with an empty buffer is the equivalent of calling [File.Remove](x,z).
	Write(x, z uint8, b []byte) (err error)

	// Remove removes the given entry from the file.
	Remove(x, z uint8) (err error)

	// CompressionMethod sets the compression method to be used by the writer.
	CompressionMethod(m CompressMethod) (err error)

	// Info gets information stored in the anvil header for the given entry.
	Info(x, z uint8) (entry Entry, exists bool)

	// Close closes the anvil file.
	Close() (err error)
}

// file is a single anvil file.
// All functions can be called concurrently from multiple goroutines.
type file struct {
	mux    sync.RWMutex
	header *Header

	pos pos

	settings Settings

	size   int64
	writer writer
	reader reader

	c  compressor
	cm CompressMethod

	// This is nil unless this was opened by Anvil
	cache *Anvil

	// useCount the number of users for this file
	// This should only be modified while holding read or write lock of `cache`.
	// This is unused if `cache` is nil
	useCount atomic.Int32
}

// OpenFile opens the given anvil file.
// If readonly is set any attempts to modify the file will return an error.
// If any data is stored in external files, any attempt to read it will return [ErrExternal].
// If an attempt is made to write a data that is over 1MB after compression, [ErrExternal] will be returned.
// To allow reading and writing to external files use [Open] instead.
func OpenFile(path string, opt ...Settings) (f File, err error) {
	settings := getSettings(opt, filesystem)

	var read reader
	var size int64
	if path, err = filepath.Abs(path); err == nil {
		if read, size, err = openFile(path, settings); err == nil {
			f, err = newAnvil(0, 0, read, size, settings)
		}
	}
	return
}

// ReadAnvil reads an anvil file from the given ReadAtCloser.
// This has the same limitations as [OpenFile] if `fs` is nil.
// If fileSize is 0, no attempt is made to read any headers.
func ReadAnvil(rgx, rgz int32, r io.ReaderAt, fileSize int64, fs afero.Fs, opt ...Settings) (a File, err error) {
	return newAnvil(rgx, rgz, r, fileSize, getSettings(opt, fs))
}

func newAnvil(rgx, rgz int32, r io.ReaderAt, fileSize int64, settings Settings) (a *file, err error) {

	// check if the file size is 0 or a multiple of 4096
	if fileSize&sectionSizeMask != 0 || (fileSize != 0 && fileSize < SectionSize*2) {
		return nil, ErrSize
	}

	anvil := &file{settings: settings, pos: pos{x: rgx, z: rgz}, size: fileSize}

	if closer, ok := r.(reader); ok {
		anvil.reader = closer
	} else {
		if !settings.ReadOnly {
			return nil, errors.New("anvil: ReadFile: `r` must implement io.Closer to be opened in write mode")
		}
		anvil.reader = &noopReadAtCloser{ReaderAt: r}
	}

	if !settings.ReadOnly {
		var canWrite bool
		anvil.writer, canWrite = r.(writer)
		if !canWrite {
			return nil, errors.New("anvil: ReadFile: `r` must implement anvil.Writer to be opened in write mode")
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

// Read reads the content of the entry at the given coordinates to a
// a byte slice and returns it.
func (a *file) Read(x, z uint8) (buf []byte, err error) {
	a.mux.RLock()
	defer a.mux.RUnlock()

	src, length, err := a.read(x, z)
	if err != nil {
		return nil, err
	}

	buf, err = io.ReadAll(src)
	if len(buf) < int(length) {
		// err = fmt.Errorf("anvil: expected to read %d bytes read %d bytes", length, len(buf))
	}

	closeErr := src.Close()
	if err == nil {
		err = closeErr
	}

	if err != nil {
		return nil, err
	}

	return buf, nil
}

type readFromWrapper struct {
	fn func(io.Reader) error
}

func (r *readFromWrapper) ReadFrom(src io.Reader) (n int64, err error) {
	return -1, r.fn(src)
}

// Read reads the entry at x,z to using the given readFn.
// `readFn` must not retain the [io.Reader] passed to it.
// `readFn` must not return before reading has completed.
func (a *file) ReadWith(x, z uint8, readFn func(io.Reader) error) (err error) {
	_, err = a.ReadTo(x, z, &readFromWrapper{fn: readFn})
	return
}

// ReadTo reads the entry at x,z to the given [io.ReaderFrom].
// `reader` must not retain the [io.Reader] passed to it.
// `reader` must not return before reading has completed.
func (a *file) ReadTo(x, z uint8, reader io.ReaderFrom) (n int64, err error) {
	a.mux.RLock()
	defer a.mux.RUnlock()

	src, _, err := a.read(x, z)
	if err != nil {
		return 0, err
	}

	n, err = reader.ReadFrom(src)
	closeErr := src.Close()
	if err == nil {
		err = closeErr
	}

	return 0, err
}

func (a *file) read(x, z uint8) (src io.ReadCloser, length int64, err error) {
	if x > 31 || z > 31 {
		return nil, 0, fmt.Errorf("anvil: invalid chunk position")
	}

	if a.header == nil {
		return nil, 0, ErrClosed
	}

	entry := a.header.Get(x, z)

	if !entry.Exists() {
		return nil, 0, ErrNotExist
	}

	offset := entry.Offset() * SectionSize
	var method CompressMethod
	var external bool

	if length, method, external, err = a.readEntryHeader(entry); err == nil {
		var src io.ReadCloser
		if src, err = a.readerForEntry(x, z, offset, length, external); err == nil {
			if src, err = method.decompressor(src); err == nil {
				return src, length, nil
			}
		}
	}

	return nil, 0, err
}

// Write updates the data for the entry at x,z to the given buffer.
// The given buffer is compressed and written to the anvil file.
// The compression method used can be changed using the [CompressMethod] method.
// If the data is larger than 1MB after compression, the data is stored externally.
// Calling this function with an empty buffer is the equivalent of calling `Remove(x,z)`.
func (a *file) Write(x, z uint8, b []byte) (err error) {
	if len(b) == 0 {
		return a.Remove(x, z)
	}

	a.mux.Lock()
	defer a.mux.Unlock()

	// check if the write is valid and if the file is open
	if err = a.checkWrite(x, z); err != nil {
		return err
	}

	// compress the given buffer
	var buf *buffer
	if buf, err = a.compress(b); err != nil {
		return errors.Wrap("anvil: error compressing data", err)
	}
	defer buf.Reset()

	size := sections(uint(buf.Len()))

	if size > 255 {
		if a.settings.fs == nil {
			return ErrExternal
		}

		cx, cz := a.pos.External(x, z)

		var f afero.File

		filename := fmt.Sprintf(a.settings.ChunkFmt, cx, cz)
		if f, err = a.settings.fs.Create(filename); err != nil {
			return errors.Wrap("anvil: unable to create external file", err)
		}

		if err = buf.WriteTo(f, false); err != nil {
			return errors.Wrap("anvil: unable to write external file", err)
		}

		method := buf.compress
		buf.Reset()

		buf.AppendBytes([]byte{0})
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

	if err = buf.WriteAt(a.writer, int64(offset)*SectionSize, true); err != nil {
		return errors.Wrap("anvil: unable to write entry data", err)
	}
	if err = a.writer.Sync(); err != nil {
		return errors.Wrap("anvil: unable to write entry data", err)
	}

	return a.updateHeader(x, z, offset, uint8(size))
}

// Remove removes the given entry from the file.
func (a *file) Remove(x, z uint8) (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()

	// check if the write is valid and if the file is open
	if err = a.checkWrite(x, z); err != nil {
		return
	}

	if err = a.header.Remove(x, z); err != nil {
		return
	}

	// grow the file so that it has at least enough space to fit the header
	if _, err = a.growFile(0); err == nil {
		err = a.updateHeader(x, z, 0, 0)
	}

	return
}

// CompressionMethod sets the compression method to be used by the writer.
func (a *file) CompressionMethod(m CompressMethod) (err error) {
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

// Info gets information stored in the anvil header for the given entry.
func (a *file) Info(x, z uint8) (entry Entry, exists bool) {
	if x > 31 || z > 31 {
		return
	}

	entry = *a.header.Get(x, z)
	return entry, entry.Exists()
}

// Close closes the anvil file.
func (a *file) Close() (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()
	if a.header != nil {
		a.header.Free()
		a.header = nil
		if a.writer != nil {
			if err = a.writer.Sync(); err != nil {
				return
			}
		}
		err = a.reader.Close()
	}

	return
}

// readerForEntry returns a reader that reads the given entry.
// The reader is only valid until the next call to `Write`
func (a *file) readerForEntry(x, z uint8, offset, length int64, external bool) (src io.ReadCloser, err error) {
	if !external {
		return io.NopCloser(io.NewSectionReader(a.reader, offset+entryHeaderSize, length)), nil
	} else if a.settings.fs != nil {
		entryX, entryZ := a.pos.External(x, z)
		filename := fmt.Sprintf(a.settings.ChunkFmt, entryX, entryZ)

		if src, err = a.settings.fs.Open(filename); err != nil {
			return nil, errors.Wrap("anvil: unable to open external file", err)
		}
		return
	}
	return nil, ErrExternal
}

// readEntryHeader reads the header for the given entry.
func (a *file) readEntryHeader(entry *Entry) (length int64, method CompressMethod, external bool, err error) {
	header := [entryHeaderSize]byte{}
	if _, err = a.reader.ReadAt(header[:], entry.Offset()*SectionSize); err == nil {
		// the first 4 bytes in the header holds the length of the data as a big endian uint32
		length = int64(binary.BigEndian.Uint32(header[:]))
		// the top bit of the 5th byte of the header indicates if the entry is stored externally
		external = header[4]&externalMask != 0
		// the lower bits hold the compression method used to compress the data
		method = CompressMethod(header[4] &^ externalMask)

		// reduce the length by 1 since we already read the compression byte
		length--

		if length/SectionSize > int64(entry.size) {
			return 0, 0, false, errors.CauseStr(ErrCorrupted, "chunk size mismatch")
		}
	}
	return
}

// checkWrite checks if the write is valid.
// This checks if x,z are within bounds
// and if the file was opened for writing and has not been closed.
func (a *file) checkWrite(x, z uint8) error {
	if x > 31 || z > 31 {
		return fmt.Errorf("anvil: invalid entry position")
	}

	if a.header == nil {
		return ErrClosed
	}

	if a.writer == nil {
		return ErrReadOnly
	}

	return nil
}

// growFile grows the file to fit `size` more sections.
func (a *file) growFile(size uint) (offset uint, err error) {
	fileSize := a.size

	// make space for the header if the file does not have one.
	if fileSize < SectionSize*2 {
		fileSize = SectionSize * 2
	}

	offset = sections(uint(fileSize))
	a.size = int64(offset+size) * SectionSize // insure the file size is a multiple of 4096 bytes
	err = a.writer.Truncate(a.size)
	return
}

// updateHeader updates the offset, size and timestamp in the main header for the entry at x,z.
func (a *file) updateHeader(x, z uint8, offset uint, size uint8) (err error) {
	if x > 31 || z > 31 {
		panic("invalid position")
	}

	headerOffset := int64(x)<<2 | int64(z)<<7
	entry := Entry{offset: uint32(offset), size: uint8(size), timestamp: int32(time.Now().Unix())}

	if err = a.writeUint32At(uint32(offset)<<8|uint32(size), headerOffset); err != nil {
		return errors.Wrap("anvil: unable to update header", err)
	}

	if err = a.header.Set(x, z, entry); err != nil {
		return
	}

	if err = a.writeUint32At(uint32(entry.timestamp), headerOffset+SectionSize); err != nil {
		return errors.Wrap("anvil: unable to update timestamp", err)
	}
	return
}

// writeUint32 writes the given uint32 at the given position
// and syncs the changes to disk.
func (a *file) writeUint32At(v uint32, offset int64) (err error) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	if _, err = a.writer.WriteAt(tmp[:], offset); err == nil {
		err = a.writer.Sync()
	}

	return
}

// compress compresses the given byte slice and writes it to a buffer.
func (a *file) compress(b []byte) (buf *buffer, err error) {
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
