package file

import (
	"encoding/binary"
	"io"
	"sync"
)

var sectionPool = sync.Pool{New: func() interface{} { return &section{} }}

type section [SectionSize]byte

func (b *section) Free() { sectionPool.Put(b) }

type buffer struct {
	length int64
	buf    []*section
}

func (b *buffer) Write(p []byte) (n int, err error) {
	if b.buf == nil {
		b.grow()
		// reserve space for the header
		b.length = 5
	}

	idx := b.length >> sectionShift
	offset := b.length & sectionSizeMask

	for n < len(p) {
		if idx >= int64(len(b.buf)) {
			b.grow()
		}

		n += copy(b.buf[idx][offset:], p[n:])

		idx++
		offset = 0
	}

	b.length += int64(n)
	return n, nil
}

// Header writes the header for the data.
// Calling this before calling Write will panic.
func (b *buffer) Header(compressionMethod CompressMethod) {
	binary.BigEndian.PutUint32(b.buf[0][:], uint32(b.length-5))
	b.buf[0][4] = byte(compressionMethod)
}

// WriteTo writes this buffer to the given writer at the given position.
// If header is set, this also writes a 5 byte header at the start of the data
// that includes the length of the data and the compression method used
func (b *buffer) WriteTo(w io.WriterAt, off int64, header bool) error {
	var i int

	if !header {
		if len(b.buf) == 1 {
			_, err := w.WriteAt(b.buf[0][5:b.length&sectionSizeMask], off)
			return err
		}
		if _, err := w.WriteAt(b.buf[0][5:], off); err != nil {
			return err
		}
		off += SectionSize - 5
		i++
	}

	for ; i < len(b.buf)-1; i++ {
		if _, err := w.WriteAt(b.buf[i][:], off); err != nil {
			return err
		}
		off += SectionSize
	}

	_, err := w.WriteAt(b.buf[len(b.buf)-1][:b.length&sectionSizeMask], off)
	return err
}

// Free frees the buffer for reuse.
func (b *buffer) Free() {
	for _, s := range b.buf {
		s.Free()
	}
	*b = buffer{}
}

// Len returns the length of the buffer.
// This includes the length of the header
func (b *buffer) Len() int { return int(b.length) }

func (b *buffer) grow() {
	b.buf = append(b.buf, sectionPool.Get().(*section))
}
