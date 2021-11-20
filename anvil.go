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
	"github.com/yehan2002/fastbytes/v2"
)

const (
	// ErrExternal returned if the chunk is in an external file.
	// This error is only returned if the region file was opened as a single file.
	ErrExternal = errors.Const("anvil: chunk is in separate file")
	// ErrNotGenerated returned if the chunk has not been generated yet.
	ErrNotGenerated = errors.Const("anvil: chunk has not been generated")
	// ErrSize returned if the size of the anvil file is not a multiple of 4096.
	ErrSize = errors.Const("anvil: invalid file size")
	// ErrCorrupted the given file contains invalid/corrupted data
	ErrCorrupted = errors.Const("anvil: corrupted file")
	// ErrClosed the given file has already been closed
	ErrClosed = errors.Const("anvil: file closed")
)

const (
	// entries the number of entries in a region file
	entries = 32 * 32
	// sectionSize the size of a section
	sectionSize     = 1 << sectionShift
	sectionSizeMask = sectionSize - 1
	sectionShift    = 12
	entryHeaderSize = 5
	// maxFileSections the maximum number of sections a file can contain
	maxFileSections = 255 * entries
)

// Anvil is a single anvil region file.
type Anvil struct {
	mux    sync.RWMutex
	header *Header

	pos Region
	fs  *Fs

	size  int64
	write writer
	read  reader

	closed bool

	c  compressor
	cm CompressMethod
}

// OpenAnvil opens the given anvil file.
// If readonly is set any attempts to modify the file will return an error.
// If any data is stored in external files, any attempt to read it will return ErrExternal.
// If an attempt is made to write a data that is over 1MB after compression, ErrExternal will be returned.
// To allow reading and writing to external files use `Open` instead.
func OpenAnvil(path string, readonly bool) (f *Anvil, err error) {
	var r reader
	var size int64
	if path, err = filepath.Abs(path); err == nil {
		if r, size, err = openFile(fs, path, readonly); err == nil {
			f, err = NewAnvil(Region{0, 0}, NewFs(fs), r, readonly, size)
		}
	}
	return
}

// NewAnvil reads an anvil file from the given ReadAtCloser.
// This has the same limitations as `OpenFile` if `fs` is nil.
// If fileSize is 0, no attempt is made to read any headers.
func NewAnvil(rg Region, fs *Fs, r reader, readonly bool, fileSize int64) (a *Anvil, err error) {
	// check if the file size is 0 or a multiple of 4096
	if fileSize&sectionSizeMask != 0 || (fileSize != 0 && fileSize < sectionSize*2) {
		return nil, ErrSize
	}

	if fs != nil && fs.fs == nil {
		return nil, errors.New("anvil: invalid anvil.Fs provided")
	}

	anvil := &Anvil{header: newHeader(), fs: fs, pos: rg, read: r, size: fileSize}

	if !readonly {
		var canWrite bool
		anvil.write, canWrite = r.(writer)
		if !canWrite {
			return nil, errors.Error("anvil: ReadFile: `r` must implement anvil.Writer to be opened in write mode")
		}
	}

	if fileSize == 0 { // fast path for empty files
		anvil.header.clear()
		anvil.header.used = bitset.New(entries)
		return anvil, nil
	}

	maxSection := uint(fileSize / sectionSize)
	anvil.header.used = bitset.New(maxSection)

	// read the file header
	var size, timestamps [entries]uint32
	if err = anvil.readUint32Section(size[:], 0); err == nil {
		if err = anvil.readUint32Section(timestamps[:], sectionSize); err == nil {
			if err = anvil.header.Read(&size, &timestamps, uint32(maxSection)); err == nil {
				return anvil, nil
			}
		}
	}
	return
}

// Read reads the entry at x,z to the given `reader`.
// `reader` must not retain the `io.Reader` passed to it.
// `reader` must not return before reading has completed.
func (a *Anvil) Read(x, z uint8, reader io.ReaderFrom) (n int64, err error) {
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
// The compression method used can be changed using the `CompressionMethod` method.
// If the data is larger than 1MB after compression, the data is stored externally.
// Calling this function with an empty buffer is the equivalent of calling `Remove(x,z)`.
func (a *Anvil) Write(x, z uint8, b []byte) (err error) {
	if len(b) == 0 {
		return a.Remove(x, z)
	}

	a.mux.Lock()
	defer a.mux.Unlock()

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
		if a.fs != nil {
			cx, cz := a.pos.Chunk(x, z)
			return a.fs.writeExternal(cx, cz, buf)
		}
		return ErrExternal
	}

	// try to find space to store the data
	offset, hasSpace := a.header.FindSpace(size)

	// If we don't have enough space, grow the file to to make space
	if !hasSpace {
		if offset, err = a.growFile(size); err != nil {
			return errors.Wrap("anvil: unable to grow file", err)
		}
	}

	if err = buf.WriteAt(a.write, int64(offset)*sectionSize, true); err != nil {
		return errors.Wrap("anvil: unable to write entry data", err)
	}
	if err = a.write.Sync(); err != nil {
		return errors.Wrap("anvil: unable to write entry data", err)
	}

	return a.updateHeader(x, z, offset, uint8(size))
}

// Remove removes the given entry from the file.
func (a *Anvil) Remove(x, z uint8) (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()

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
func (a *Anvil) CompressionMethod(m CompressMethod) (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()
	var c compressor
	if c, err = m.compressor(); err == nil {
		a.cm, a.c = m, c
	}
	return
}

// Close closes the anvil file.
func (a *Anvil) Close() (err error) {
	a.mux.Lock()
	defer a.mux.Unlock()
	if !a.closed {
		a.header.Free()
		a.header = nil
		if a.write != nil {
			if err = a.write.Sync(); err != nil {
				return
			}
		}
		err = a.read.Close()
		a.closed = true
	}

	return
}

// readUint32Section reads a 4096 byte section at the given offset into the given uint32 slice.
func (a *Anvil) readUint32Section(dst []uint32, offset int) error {
	tmp := sectionPool.Get().(*section)
	defer tmp.Free()

	if n, err := a.read.ReadAt(tmp[:], int64(offset)); err != nil {
		return errors.Wrap("anvil: unable to read file header", err)
	} else if n != sectionSize {
		return errors.Wrap("anvil: Incorrect number of bytes read", io.EOF)
	}

	fastbytes.BigEndian.ToU32(tmp[:], dst)
	return nil
}

// readerForEntry returns a reader that reads the given entry.
// The reader is only valid until the next call to `Write`
func (a *Anvil) readerForEntry(x, z uint8, offset, length int64, external bool) (src io.ReadCloser, err error) {
	if !external {
		return io.NopCloser(io.NewSectionReader(a.read, offset+entryHeaderSize, length)), nil
	} else if a.fs != nil {
		return a.fs.readExternal(a.pos.Chunk(x, z))
	}
	return nil, ErrExternal
}

// readEntryHeader reads the header for the given entry.
func (a *Anvil) readEntryHeader(entry *Entry) (length int64, method CompressMethod, external bool, err error) {
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

		if length/sectionSize > int64(entry.Size) {
			return 0, 0, false, errors.CauseStr(ErrCorrupted, "chunk size mismatch")
		}
	}
	return
}

// checkWrite checks if the write is valid.
// This checks if x,z are within bounds
// and if the file was opened for writing and has not been closed.
func (a *Anvil) checkWrite(x, z uint8) error {
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
func (a *Anvil) growFile(size uint) (offset uint, err error) {
	fileSize := a.size

	// make space for the header if the file does not have one.
	if fileSize < sectionSize*2 {
		fileSize = sectionSize * 2
	}

	offset = sections(uint(fileSize))
	a.size = int64(offset+size) * sectionSize // insure the file size is a multiple of 4096 bytes
	err = a.write.Truncate(a.size)
	return
}

// updateHeader updates the offset, size and timestamp in the main header for the entry at x,z.
func (a *Anvil) updateHeader(x, z uint8, offset uint, size uint8) (err error) {
	headerOffset := int64(x)<<2 | int64(z)<<7
	entry := Entry{Offset: uint32(offset), Size: uint8(size), Timestamp: int32(time.Now().Unix())}

	if err = a.writeUint32At(uint32(offset)<<8|uint32(size), headerOffset); err != nil {
		return errors.Wrap("anvil: unable to update header", err)
	}

	a.header.Set(x, z, entry)

	if err = a.writeUint32At(uint32(entry.Timestamp), headerOffset+sectionSize); err != nil {
		return errors.Wrap("anvil: unable to update timestamp", err)
	}
	return
}

// writeUint32 writes the given uint32 at the given position
// and syncs the changes to disk.
func (a *Anvil) writeUint32At(v uint32, offset int64) (err error) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	if _, err = a.write.WriteAt(tmp[:], offset); err == nil {
		err = a.write.Sync()
	}

	return
}

// compress compresses the given byte slice and writes it to a buffer.
func (a *Anvil) compress(b []byte) (buf *buffer, err error) {
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
