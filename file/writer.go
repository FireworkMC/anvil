package file

import (
	"encoding/binary"
	"fmt"
	"os"

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
	buf := f.compress(b)
	size := uint((len(b) + 5) / sectionSize)
	if size > 256 {
		panic("TODO")
	}

	f.mux.Lock()
	defer f.mux.Unlock()

	offset, hasSpace := f.findSpace(size)
	var fileOffset = int64(offset) * sectionSize

	if !hasSpace {
		if fileOffset, err = f.f.Seek(0, os.SEEK_END); err != nil {
			return err
		}

	}

	n, err := f.f.WriteAt(buf.B, fileOffset)
	if n != buf.Len() {
		return fmt.Errorf("anvil/file: unexpected number of bytes written")
	}
	if err != nil {
		return errors.Wrap("anvil/file: unable to write", err)
	}

	if err = f.f.Sync(); err != nil {
		return errors.Wrap("anvil/file: unable to sync to disk", err)
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
