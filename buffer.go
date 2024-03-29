package anvil

import (
	"encoding/binary"
	"io"
	"sync"
)

var sectionPool = sync.Pool{New: func() interface{} { return &section{} }}

type section [SectionSize]byte

func (b *section) Free() { sectionPool.Put(b) }

// buffer a reuseable buffer for writing data
type buffer struct {
	length   int64
	compress CompressMethod
	buf      []*section
}

// Write appends data to this buffer.
// This never returns an error.
func (b *buffer) Write(p []byte) (n int, err error) { return b.AppendBytes(p), nil }

// AppendBytes appends the given byte slice to the buffer.
func (b *buffer) AppendBytes(p []byte) (n int) {
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
	return n
}

// CompressMethod sets the compression method used by the data in the buffer.
// This is only used to set the compression byte in the header.
// Callers must compress the data before writing it to this buffer.
// If this is not called, DefaultCompression is used.
func (b *buffer) CompressMethod(c CompressMethod) { b.compress = c }

// WriteAt writes this buffer to the given writer at the given position.
// If header is set, this also writes a 5 byte header at the start of the data
// that includes the length of the data and the compression method used
func (b *buffer) WriteAt(w io.WriterAt, off int64, header bool) error {
	startOffset := 5

	if header {
		binary.BigEndian.PutUint32(b.buf[0][:4], uint32(b.length-4))
		if b.compress == 0 {
			b.compress = DefaultCompression
		}
		b.buf[0][4] = byte(b.compress)
		startOffset = 0
	}

	if off > 0 && off < SectionSize*2 {
		panic("invalid offset")
	}

	for i := 0; i < len(b.buf); i++ {
		buf := b.buf[i][startOffset:]

		if i == len(b.buf)-1 {
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

// WriteTo same as [WriteAt] but writes to the start of the given writer
func (b *buffer) WriteTo(w io.Writer, header bool) (err error) {
	return b.WriteAt(&writeAtWrapper{Writer: w}, -1, header)
}

// Reset frees the buffer for reuse.
func (b *buffer) Reset() {
	for _, s := range b.buf {
		s.Free()
	}
	*b = buffer{}
}

// Len returns the length of the buffer.
// This includes the length of the header.
// If the buffer is completely empty other than the header this returns 0.
func (b *buffer) Len() int {
	if b.length == 5 {
		return 0
	}
	return int(b.length)
}

func (b *buffer) grow() { b.buf = append(b.buf, sectionPool.Get().(*section)) }

// writeAtWrapper wraps a io.Writer into a io.WriteAt.
// This should only be used for a continuous write at given offset.
type writeAtWrapper struct{ io.Writer }

func (w *writeAtWrapper) WriteAt(p []byte, off int64) (n int, err error) { return w.Writer.Write(p) }
