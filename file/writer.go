package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

// Writer is a single anvil region file.
type Writer struct {
	*Reader
	f afero.File

	c  compressor
	cm CompressMethod
}

func (w *Writer) Write(x, z int, b []byte) (err error) {
	if x < 0 || z < 0 || x > 31 || z > 31 {
		return fmt.Errorf("anvil/file: invalid chunk position")
	}

	if len(b) == 0 {
		w.mux.Lock()
		if _, err = w.growFile(0); err == nil {
			err = w.updateHeader(x, z, 0, 0)
		}
		w.mux.Unlock()
		return
	}

	if err = w.initCompression(); err != nil {
		return
	}

	var buf *buffer
	if buf, err = w.compress(b); err != nil {
		return errors.Wrap("anvil/file: error compressing data", err)
	}
	defer buf.Free()

	size := sections(uint(buf.Len()))

	if size > 255 {
		return w.dir.writeExternal(x, z, buf)
	}

	w.mux.Lock()
	defer w.mux.Unlock()

	offset, hasSpace := w.findSpace(size)

	if !hasSpace {
		if offset, err = w.growFile(size); err != nil {
			return errors.Wrap("anvil/file: unable to grow file", err)
		}
	}

	if err = buf.WriteTo(w.f, int64(offset)*SectionSize); err != nil {
		return errors.Wrap("anvil/file: unable to write chunk data", err)
	}
	if err = w.f.Sync(); err != nil {
		return errors.Wrap("anvil/file: unable to write chunk data", err)
	}

	return w.updateHeader(x, z, offset, uint8(size))
}

// CompressionMethod sets the compression method to be used by the writer
func (w *Writer) CompressionMethod(m CompressMethod) (err error) {
	var c compressor
	if c, err = m.compressor(); err == nil {
		w.cm, w.c = m, c
	}
	return
}

func (w *Writer) initCompression() (err error) {
	if w.cm == 0 {
		return w.CompressionMethod(CompressionZlib)
	}
	return
}

// growFile grows the file to fit `size` more sections.
func (w *Writer) growFile(size uint) (offset uint, err error) {
	var fileSize int64
	if fileSize, err = w.f.Seek(0, io.SeekEnd); err == nil {

		// make space for the header if the file does not have one.
		if fileSize < SectionSize*2 {
			fileSize = SectionSize * 2
		}

		offset = sections(uint(fileSize))
		newSize := int64(offset+size) * SectionSize // insure the file size is a multiple of 4096 bytes
		err = w.f.Truncate(newSize)
	}
	return
}

func (w *Writer) updateHeader(x, z int, offset uint, size uint8) (err error) {
	headerOffset := int64(x|(z<<5)) << 2

	if err = w.writeUint32At(uint32(offset)<<8|uint32(size), headerOffset); err != nil {
		return errors.Wrap("anvil/file: unable to update header", err)
	}

	entry := w.header.Get(x, z)
	w.clearUsed(entry)

	*entry = Entry{Offset: uint32(offset), Size: uint8(size), Timestamp: int32(time.Now().Unix())}
	w.setUsed(entry)

	if err = w.writeUint32At(uint32(entry.Timestamp), headerOffset+SectionSize); err != nil {
		return errors.Wrap("anvil/file: unable to update timestamp", err)
	}
	return
}

// writeUint32 writes the given uint32 at the given position
// and syncs the changes to disk.
func (w *Writer) writeUint32At(v uint32, offset int64) (err error) {
	var tmp [4]byte

	binary.BigEndian.PutUint32(tmp[:], v)
	if _, err = w.f.WriteAt(tmp[:], offset); err == nil {
		err = w.f.Sync()
	}

	return
}

// setUsed marks the space used by the given chunk in the `used` bitset as used.
func (w *Writer) setUsed(c *Entry) {
	end := uint(c.Offset) + uint(c.Size)
	for i := uint(c.Offset); i < end; i++ {
		if w.used.Test(i) {
			panic("set overflows into used region")
		}

		w.used.Set(i)
	}
}

// clearUsed marks the space used by the given chunk in the `used` bitset as unused.
func (w *Writer) clearUsed(c *Entry) {
	if c.Offset == 0 || c.Size == 0 {
		return
	}

	end := uint(c.Offset) + uint(c.Size)
	for i := uint(c.Offset); i < end; i++ {
		if !w.used.Test(i) {
			panic("invalid clear")
		}
		w.used.Clear(i)
	}
}

var zeroHeader [5]byte

func (w *Writer) compress(b []byte) (buf *buffer, err error) {
	buf = &buffer{}
	w.c.Reset(buf)
	if _, err = w.c.Write(b); err == nil {
		if err = w.c.Close(); err == nil {
			buf.Header(w.cm)
			return buf, nil
		}
	}

	return nil, err
}

// findSpace finds the next free space large enough to store `size` sections
func (w *Writer) findSpace(size uint) (offset uint, found bool) {
	// ignore the first two section since they are used for the header
	offset = 2

	var hasSpace = true
	for hasSpace {
		var next uint

		offset, hasSpace = w.used.NextClear(offset)
		if !hasSpace {
			break
		}

		next, hasSpace = w.used.NextSet(offset)
		if hasSpace && next-offset >= size {
			return offset, true
		}

		offset = next
	}

	return 0, false
}
