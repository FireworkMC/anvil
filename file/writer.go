package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/klauspost/compress/zlib"
	"github.com/valyala/bytebufferpool"
	"github.com/yehan2002/errors"
)

// File is a single anvil region file.
type File struct {
	*Reader
	zlib *zlib.Writer
	f    *os.File
}

func (f *File) Write(x, z int, b []byte) (err error) {
	if x < 0 || z < 0 || x > 31 || z > 31 {
		return fmt.Errorf("anvil/file: invalid chunk position")
	}

	buf := f.compress(b)
	size := uint((len(b) + 5) / sectionSize)
	if size > 255 {
		panic("TODO")
	}

	f.mux.Lock()
	defer f.mux.Unlock()

	offset, hasSpace := f.findSpace(size)
	var fileOffset = int64(offset) * sectionSize

	if !hasSpace {
		if fileOffset, err = f.f.Seek(0, io.SeekEnd); err != nil {
			return err
		}
		for i := 0; i < int(size); i++ {
			f.used.Set(f.used.Len())
		}
	}

	if _, err = f.f.WriteAt(buf.B, fileOffset); err != nil {
		return errors.Wrap("anvil/file: unable to write", err)
	}
	if err = f.f.Sync(); err != nil {
		return errors.Wrap("anvil/file: unable to sync to disk", err)
	}
	bufferpool.Put(buf)

	headerOffset := int64(x<<4 | z<<2)

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(offset)<<8|uint32(size))
	if _, err = f.f.WriteAt(header[:], headerOffset); err != nil {
		return errors.Wrap("anvil/file: unable to update location", err)
	}
	if err = f.f.Sync(); err != nil {
		return errors.Wrap("anvil/file: unable to sync location to disk", err)
	}

	chunk := f.header.get(x, z)
	for i := 0; i < int(chunk.size); i++ {
		f.used.Clear(uint(chunk.location) + uint(i))
	}

	chunk.location = uint32(offset)
	chunk.size = uint8(size)
	chunk.timestamp = uint32(time.Now().Unix())

	binary.BigEndian.PutUint32(header[:], chunk.timestamp)
	if _, err = f.f.WriteAt(header[:], headerOffset+sectionSize); err != nil {
		return errors.Wrap("anvil/file: unable to update timestamp", err)
	}
	if err = f.f.Sync(); err != nil {
		return errors.Wrap("anvil/file: unable to sync timestamp to disk", err)
	}

	return nil
}

var zeroHeader [5]byte

func (f *File) compress(b []byte) *bytebufferpool.ByteBuffer {
	buf := bufferpool.Get()
	buf.Reset()
	buf.Write(zeroHeader[:])
	f.zlib.Reset(buf)
	f.zlib.Write(b)
	f.zlib.Close()
	binary.BigEndian.PutUint32(buf.B, uint32(buf.Len()-5))
	buf.B[4] = compressionZlib
	return buf
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
		if next-offset >= size {
			return offset, true
		}

		offset = next
	}

	return 0, false
}
