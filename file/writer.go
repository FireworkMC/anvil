package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

// File is a single anvil region file.
type File struct {
	mux    sync.RWMutex
	header *Header
	used   *bitset.BitSet
	dir    *dir
	size   int64

	write file
	read  io.ReaderAt
	close io.Closer

	c  compressor
	cm CompressMethod
}

type file interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
}

var _ file = afero.File(nil)

func (f *File) Write(x, z int, b []byte) (err error) {
	if x < 0 || z < 0 || x > 31 || z > 31 {
		return fmt.Errorf("anvil/file: invalid chunk position")
	}

	if f.write == nil {
		return fmt.Errorf("anvil/file: file is opened in read-only mode")
	}

	if len(b) == 0 {
		f.mux.Lock()
		if _, err = f.growFile(0); err == nil {
			err = f.updateHeader(x, z, 0, 0)
		}
		f.mux.Unlock()
		return
	}

	if err = f.initCompression(); err != nil {
		return
	}

	var buf *buffer
	if buf, err = f.compress(b); err != nil {
		return errors.Wrap("anvil/file: error compressing data", err)
	}
	defer buf.Free()

	size := sections(uint(buf.Len()))

	if size > 255 {
		return f.dir.WriteExternal(x, z, buf)
	}

	f.mux.Lock()
	defer f.mux.Unlock()

	offset, hasSpace := f.findSpace(size)

	if !hasSpace {
		if offset, err = f.growFile(size); err != nil {
			return errors.Wrap("anvil/file: unable to grow file", err)
		}
	}

	if err = buf.WriteTo(f.write, int64(offset)*SectionSize); err != nil {
		return errors.Wrap("anvil/file: unable to write chunk data", err)
	}
	if err = f.write.Sync(); err != nil {
		return errors.Wrap("anvil/file: unable to write chunk data", err)
	}

	return f.updateHeader(x, z, offset, uint8(size))
}

// CompressionMethod sets the compression method to be used by the writer
func (f *File) CompressionMethod(m CompressMethod) (err error) {
	var c compressor
	if c, err = m.compressor(); err == nil {
		f.cm, f.c = m, c
	}
	return
}

func (f *File) initCompression() (err error) {
	if f.cm == 0 {
		return f.CompressionMethod(CompressionZlib)
	}
	return
}

// growFile grows the file to fit `size` more sections.
func (f *File) growFile(size uint) (offset uint, err error) {
	fileSize := f.size

	// make space for the header if the file does not have one.
	if fileSize < SectionSize*2 {
		fileSize = SectionSize * 2
	}

	offset = sections(uint(fileSize))
	f.size = int64(offset+size) * SectionSize // insure the file size is a multiple of 4096 bytes
	err = f.write.Truncate(f.size)
	return
}

func (f *File) updateHeader(x, z int, offset uint, size uint8) (err error) {
	headerOffset := int64(x|(z<<5)) << 2

	if err = f.writeUint32At(uint32(offset)<<8|uint32(size), headerOffset); err != nil {
		return errors.Wrap("anvil/file: unable to update header", err)
	}

	entry := f.header.Get(x, z)
	f.clearUsed(entry)

	*entry = Entry{Offset: uint32(offset), Size: uint8(size), Timestamp: int32(time.Now().Unix())}
	f.setUsed(entry)

	if err = f.writeUint32At(uint32(entry.Timestamp), headerOffset+SectionSize); err != nil {
		return errors.Wrap("anvil/file: unable to update timestamp", err)
	}
	return
}

// writeUint32 writes the given uint32 at the given position
// and syncs the changes to disk.
func (f *File) writeUint32At(v uint32, offset int64) (err error) {
	var tmp [4]byte

	binary.BigEndian.PutUint32(tmp[:], v)
	if _, err = f.write.WriteAt(tmp[:], offset); err == nil {
		err = f.write.Sync()
	}

	return
}

// setUsed marks the space used by the given chunk in the `used` bitset as used.
func (f *File) setUsed(c *Entry) {
	end := uint(c.Offset) + uint(c.Size)
	for i := uint(c.Offset); i < end; i++ {
		if f.used.Test(i) {
			panic("set overflows into used region")
		}

		f.used.Set(i)
	}
}

// clearUsed marks the space used by the given chunk in the `used` bitset as unused.
func (f *File) clearUsed(c *Entry) {
	if c.Offset == 0 || c.Size == 0 {
		return
	}

	end := uint(c.Offset) + uint(c.Size)
	for i := uint(c.Offset); i < end; i++ {
		if !f.used.Test(i) {
			panic("invalid clear")
		}
		f.used.Clear(i)
	}
}

var zeroHeader [5]byte

func (f *File) compress(b []byte) (buf *buffer, err error) {
	buf = &buffer{}
	f.c.Reset(buf)
	if _, err = f.c.Write(b); err == nil {
		if err = f.c.Close(); err == nil {
			buf.Header(f.cm)
			return buf, nil
		}
	}

	return nil, err
}

// findSpace finds the next free space large enough to store `size` sections
func (f *File) findSpace(size uint) (offset uint, found bool) {
	// ignore the first two section since they are used for the header
	offset = 2

	var hasSpace = true
	for hasSpace {
		var next uint

		offset, hasSpace = f.used.NextClear(offset)
		if !hasSpace {
			break
		}

		next, hasSpace = f.used.NextSet(offset)
		if hasSpace && next-offset >= size {
			return offset, true
		}

		offset = next
	}

	return 0, false
}
