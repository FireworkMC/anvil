package anvil

import (
	"encoding/binary"
	"io"
	"sync"
)

var sectionPool = sync.Pool{New: func() interface{} { return &section{} }}

type section [SectionSize]byte

func (b *section) Free() { sectionPool.Put(b) }

// Buffer a reuseable buffer for writing data
type Buffer struct {
	length   int64
	compress CompressMethod
	buf      []*section
}

// Write appends data to this buffer.
// This never returns an error.
func (b *Buffer) Write(p []byte) (n int, err error) {
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

// CompressMethod sets the compression method used by the data in the buffer.
// This is only used to set the compression byte in the header.
// Callers must compress the data before writing it to this buffer.
// If this is not called, DefaultCompression is used.
func (b *Buffer) CompressMethod(c CompressMethod) { b.compress = c }

// WriteAt writes this buffer to the given writer at the given position.
// If header is set, this also writes a 5 byte header at the start of the data
// that includes the length of the data and the compression method used
func (b *Buffer) WriteAt(w io.WriterAt, off int64, header bool) error {
	startOffset := 5

	if header {
		binary.BigEndian.PutUint32(b.buf[0][:4], uint32(b.length-4))
		if b.compress == 0 {
			b.compress = DefaultCompression
		}
		b.buf[0][4] = byte(b.compress)
		startOffset = 0
	}

	for i := 0; i < len(b.buf); i++ {
		buf := b.buf[i][startOffset:]

		if i == len(b.buf)-1 {
			// TODO: check if this works properly
			buf = b.buf[i][startOffset : (b.length-int64(startOffset))&sectionSizeMask]
		}

		if _, err := w.WriteAt(buf, off); err != nil {
			return err
		}

		off += int64(len(buf))
		startOffset = 0
	}

	return nil
}

// WriteTo same as `WriteAt` but writes to the start of the given writer
func (b *Buffer) WriteTo(w io.Writer, header bool) (err error) {
	return b.WriteAt(&writeAtWrapper{w}, 0, header)
}

// Free frees the buffer for reuse.
func (b *Buffer) Free() {
	for _, s := range b.buf {
		s.Free()
	}
	*b = Buffer{}
}

// Len returns the length of the buffer.
// This includes the length of the header.
// If the buffer is completely empty other than the header this returns 0.
func (b *Buffer) Len() int {
	if b.length == 5 {
		return 0
	}
	return int(b.length)
}

func (b *Buffer) grow() { b.buf = append(b.buf, sectionPool.Get().(*section)) }

type writeAtWrapper struct{ io.Writer }

func (w *writeAtWrapper) WriteAt(p []byte, off int64) (n int, err error) { return w.Writer.Write(p) }
