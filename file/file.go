package file

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/bits-and-blooms/bitset"
	"github.com/klauspost/compress/zlib"
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
	"github.com/yehan2002/fastbytes/v2"
)

// chunks the number of chunks in a region file
const chunks = 32 * 32

// sectionSize the size of a section
const sectionSize = chunks * 4 // 1 << sectionShift
const sectionShift = 12
const sectionSizeMask = sectionSize - 1

var fs afero.Fs = &afero.OsFs{}

var regionHeaderPool = sync.Pool{New: func() interface{} { return &header{} }}

type header [chunks]chunk

func (h *header) clear() { *h = header{} }

func (h *header) get(x, z int) *chunk { return &h[(x&0x1f)|((z&0x1f)<<5)] }

type chunk struct {
	size      uint8
	location  uint32
	timestamp uint32
}

// Open opens the given file
func Open(path string) (*File, error) {
	var fileSize int64
	if info, err := fs.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		fileSize = info.Size()
	}

	f, err := fs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrap("anvil/file: unable to open file", err)
	}

	r, err := NewReader(f, fileSize)
	if err != nil {
		return nil, err
	}

	return &File{f: f, Reader: r, zlib: zlib.NewWriter(io.Discard)}, nil
}

// NewReader creates a new anvil reader
func NewReader(r io.ReaderAt, fileSize int64) (*Reader, error) {

	if fileSize&sectionSizeMask != 0 || (fileSize != 0 && fileSize < sectionSize*2) {
		return nil, fmt.Errorf("anvil/file: malformed region file")
	}

	header := regionHeaderPool.Get().(*header)

	if fileSize == 0 {
		header.clear()
		return &Reader{header: header, used: bitset.New(chunks), reader: r}, nil
	}

	buf := sectionPool.Get().(*section)
	defer buf.Free()

	var size [chunks]uint32
	if err := readHeaderSection(r, buf, 0); err != nil {
		return nil, err
	}
	fastbytes.BigEndian.ToU32(buf[:], size[:])

	var timestamps [chunks]uint32
	if err := readHeaderSection(r, buf, sectionSize); err != nil {
		return nil, err
	}
	fastbytes.BigEndian.ToU32(buf[:], timestamps[:])

	maxSection := fileSize / sectionSize
	used := bitset.New(uint(maxSection))

	for i := 0; i < chunks; i++ {
		entry := chunk{
			timestamp: timestamps[i],
			size:      uint8(size[i]),
			location:  size[i] >> 8,
		}

		start := entry.location
		for p := uint32(0); p < uint32(entry.size); p++ {
			pos := start + p
			if pos > uint32(maxSection) {
				return nil, fmt.Errorf("anvil/file: invalid chunk data location")
			}
			if used.Test(uint(pos)) {
				return nil, fmt.Errorf("anvil/file: invalid chunk size/location")
			}

			used.Set(uint(pos))
		}
		header[i] = entry
	}
	return &Reader{header: header, used: used, reader: r}, nil
}

func readHeaderSection(f io.ReaderAt, buf *section, offset int) error {
	if n, err := f.ReadAt(buf[:], int64(offset)); err != nil {
		return errors.Wrap("anvil/file: unable to read file header", err)
	} else if n != sectionSize {
		return fmt.Errorf("anvil/file: Incorrect number of bytes read")
	}
	return nil
}
