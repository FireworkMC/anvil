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
func (b *buffer) Header(compressionMethod byte) {
	binary.BigEndian.PutUint32(b.buf[0][:], uint32(b.length-5))
	b.buf[0][4] = compressionMethod
}

func (b *buffer) WriteTo(w io.WriterAt, at int64) error {
	for i := 0; i < len(b.buf)-1; i++ {
		if _, err := w.WriteAt(b.buf[i][:], at); err != nil {
			return err
		}
		at += SectionSize
	}
	_, err := w.WriteAt(b.buf[len(b.buf)-1][:b.length&sectionSizeMask], at)
	return err
}

func (b *buffer) Free() {
	for _, s := range b.buf {
		s.Free()
	}
	*b = buffer{}
}

func (b *buffer) Len() int { return int(b.length) }

func (b *buffer) grow() {
	b.buf = append(b.buf, sectionPool.Get().(*section))
}
