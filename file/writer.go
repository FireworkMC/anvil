package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zlib"
	"github.com/spf13/afero"
	"github.com/valyala/bytebufferpool"
	"github.com/yehan2002/errors"
)

// File is a single anvil region file.
type File struct {
	*Reader
	zlib *zlib.Writer
	f    afero.File
}

// sections returns the minimum number of sections to store the given number of bytes
func sections(v uint) uint {
	return (v + sectionSizeMask) / sectionSize
}

func (f *File) Write(x, z int, b []byte) (err error) {
	if x < 0 || z < 0 || x > 31 || z > 31 {
		return fmt.Errorf("anvil/file: invalid chunk position")
	}

	if len(b) == 0 {
		f.mux.Lock()
		err = f.updateHeader(x, z, 0, 0)
		f.mux.Unlock()
		return
	}

	var buf *bytebufferpool.ByteBuffer
	if buf, err = f.compress(b); err != nil {
		return errors.Wrap("anvil/file: error compressing data", err)
	}
	defer bufferpool.Put(buf)

	size := sections(uint(len(buf.B)) + 5)

	if size > 255 {
		panic("TODO")
	}

	f.mux.Lock()
	defer f.mux.Unlock()

	offset, hasSpace := f.findSpace(size)

	if !hasSpace {
		if offset, err = f.growFile(size); err != nil {
			return errors.Wrap("anvil/file: unable to grow file", err)
		}
	}

	if err = f.writeSync(buf.B, int64(offset)*sectionSize); err != nil {
		return errors.Wrap("anvil/file: unable to write chunk data", err)
	}

	return f.updateHeader(x, z, offset, uint8(size))
}

// growFile grows the file to fit `size` more sections.
func (f *File) growFile(size uint) (offset uint, err error) {
	var fileSize int64
	if fileSize, err = f.f.Seek(0, io.SeekEnd); err == nil {

		// make space for the header if the file does not have one.
		if fileSize < sectionSize*2 {
			fileSize = sectionSize * 2
		}

		offset = sections(uint(fileSize))
		newSize := int64(offset+size) * sectionSize // insure the file size is a multiple of 4096 bytes
		err = f.f.Truncate(newSize)
	}
	return
}

func (f *File) updateHeader(x, z int, offset uint, size uint8) (err error) {
	headerOffset := int64(x<<4 | z<<2)

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(offset)<<8|uint32(size))
	if err = f.writeSync(header[:], headerOffset); err != nil {
		return errors.Wrap("anvil/file: unable to update header", err)
	}

	chunkHeader := f.header.get(x, z)
	f.clearUsed(chunkHeader)

	*chunkHeader = chunk{location: uint32(offset), size: uint8(size), timestamp: uint32(time.Now().Unix())}
	f.setUsed(chunkHeader)

	binary.BigEndian.PutUint32(header[:], chunkHeader.timestamp)
	if err = f.writeSync(header[:], headerOffset+sectionSize); err != nil {
		return errors.Wrap("anvil/file: unable to update timestamp", err)
	}
	return
}

// setUsed marks the space used by the given chunk in the `used` bitset as used.
func (f *File) setUsed(c *chunk) {
	end := uint(c.location) + uint(c.size)
	for i := uint(c.location); i < end; i++ {
		if f.used.Test(i) {
			panic("set overflows into used region")
		}

		f.used.Set(i)
	}
}

// clearUsed marks the space used by the given chunk in the `used` bitset as unused.
func (f *File) clearUsed(c *chunk) {
	if c.location == 0 || c.size == 0 {
		return
	}

	end := uint(c.location) + uint(c.size)
	for i := uint(c.location); i < end; i++ {
		if !f.used.Test(i) {
			panic("invalid clear")
		}
		f.used.Clear(i)
	}
}

// writeSync writes the given byte slice to the given position
// and syncs the changes to disk.
func (f *File) writeSync(p []byte, at int64) (err error) {
	if _, err = f.f.WriteAt(p, at); err == nil {
		err = f.f.Sync()
	}
	return
}

var zeroHeader [5]byte

func (f *File) compress(b []byte) (buf *bytebufferpool.ByteBuffer, err error) {
	buf = bufferpool.Get()
	buf.Reset()
	_, _ = buf.Write(zeroHeader[:])

	f.zlib.Reset(buf)
	if _, err = f.zlib.Write(b); err == nil {
		if err = f.zlib.Close(); err == nil {
			binary.BigEndian.PutUint32(buf.B, uint32(buf.Len()-5))
			buf.B[4] = compressionZlib
			return buf, nil
		}
	}

	bufferpool.Put(buf)
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
