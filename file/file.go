package file

import (
	"fmt"
	"io"
	"os"

	"github.com/bits-and-blooms/bitset"
	"github.com/klauspost/compress/zlib"
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
	"github.com/yehan2002/fastbytes/v2"
)

const (
	// Entries the number of entries in a region file
	Entries = 32 * 32
	// SectionSize the size of a section
	SectionSize     = Entries * 4 // 1 << sectionShift
	sectionSizeMask = SectionSize - 1
	sectionShift    = 12
)

// sections returns the minimum number of sections to store the given number of bytes
func sections(v uint) uint {
	return (v + sectionSizeMask) / SectionSize
}

var fs afero.Fs = &afero.OsFs{}

// Open opens the given file
func Open(path string) (w *Writer, err error) {
	var fileSize int64
	if info, err := fs.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		fileSize = info.Size()
	}

	var f afero.File
	if f, err = fs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return nil, errors.Wrap("anvil/file: unable to open file", err)
	}

	var r *Reader
	if r, err = NewReader(f, fileSize); err != nil {
		return nil, err
	}

	return &Writer{f: f, Reader: r, zlib: zlib.NewWriter(io.Discard)}, nil
}

// NewReader creates a new anvil reader
func NewReader(src io.ReaderAt, fileSize int64) (*Reader, error) {

	// check if the file size is 0 or a multiple of 4096
	if fileSize&sectionSizeMask != 0 || (fileSize != 0 && fileSize < SectionSize*2) {
		return nil, ErrSize
	}

	header := headerPool.Get().(*Header)

	if fileSize == 0 { // fast path for empty files
		header.clear()
		return &Reader{header: header, used: bitset.New(Entries), reader: src}, nil
	}

	maxSection := fileSize / SectionSize
	r := &Reader{header: header, used: bitset.New(uint(maxSection)), reader: src}

	var size, timestamps [Entries]uint32
	if err := r.readHeader(src, size[:], timestamps[:]); err != nil {
		return nil, err
	}

	for i := 0; i < Entries; i++ {
		c := Entry{Timestamp: int32(timestamps[i]), Size: uint8(size[i]), Offset: size[i] >> 8}

		start := c.Offset
		for p := uint32(0); p < uint32(c.Size); p++ {
			pos := start + p
			if pos > uint32(maxSection) {
				return nil, fmt.Errorf("anvil/file: invalid chunk data location")
			}
			if r.used.Test(uint(pos)) {
				return nil, fmt.Errorf("anvil/file: invalid chunk size/location")
			}

			r.used.Set(uint(pos))
		}

		header[i] = c
	}
	return r, nil
}

// readHeader reads the region file header.
func (r *Reader) readHeader(f io.ReaderAt, size, timestamps []uint32) (err error) {
	if err = r.readUint32Section(f, size[:], 0); err == nil {
		err = r.readUint32Section(f, timestamps[:], SectionSize)
	}
	return err
}

// readUint32Section reads a 4096 byte section at the given offset into the given uint32 slice.
func (r *Reader) readUint32Section(f io.ReaderAt, dst []uint32, offset int) error {
	tmp := sectionPool.Get().(*section)
	defer tmp.Free()

	if n, err := f.ReadAt(tmp[:], int64(offset)); err != nil {
		return errors.Wrap("anvil/file: unable to read file header", err)
	} else if n != SectionSize {
		return errors.Wrap("anvil/file: Incorrect number of bytes read", io.EOF)
	}

	fastbytes.BigEndian.ToU32(tmp[:], dst)
	return nil
}
