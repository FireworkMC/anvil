package anvil

import (
	"io"
	"os"
	"sync"

	"github.com/bits-and-blooms/bitset"
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
	"github.com/yehan2002/fastbytes/v2"
)

// File is a single anvil region file.
type File struct {
	mux    sync.RWMutex
	header *Header
	used   *bitset.BitSet

	pos   Region
	anvil *Anvil

	size  int64
	write Writer
	read  Reader

	closed bool

	c  compressor
	cm CompressMethod
}

// OpenFile opens the given anvil file.
// If readonly is set any attempts to modify the file will return an error.
// If any data is stored in external files, any attempt to read it will return ErrExternal.
// If an attempt is made to write a data that is over 1MB after compression, ErrExternal will be returned.
// To allow reading and writing to external files use `Open` instead.
func OpenFile(path string, readonly bool) (f *File, err error) {
	var r Reader
	var size int64
	if r, size, err = openFile(fs, path); err == nil {
		f, err = ReadFile(Region{0, 0}, r, readonly, size)
	}
	return
}

func openFile(fs afero.Fs, path string) (r Reader, size int64, err error) {
	var fileSize int64
	if info, err := fs.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, 0, err
		}
	} else {
		fileSize = info.Size()
	}

	var f afero.File
	if f, err = fs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return nil, 0, errors.Wrap("anvil: unable to open file", err)
	}
	return f, fileSize, nil
}

// ReadFile reads an anvil file from the given ReadAtCloser.
// This has the same limitations as `OpenFile`.
// If fileSize is 0, no attempt is made to read any headers.
func ReadFile(rg Region, r Reader, readonly bool, fileSize int64) (f *File, err error) {

	// check if the file size is 0 or a multiple of 4096
	if fileSize&sectionSizeMask != 0 || (fileSize != 0 && fileSize < SectionSize*2) {
		return nil, ErrSize
	}

	header := headerPool.Get().(*Header)
	f = &File{header: header, pos: rg, read: r, size: fileSize}

	if !readonly {
		var canWrite bool
		f.write, canWrite = r.(Writer)
		if !canWrite {
			return nil, errors.Error("anvil: ReadFile: `r` must implement anvil.Writer to be opened in write mode")
		}
	}

	if fileSize == 0 { // fast path for empty files
		header.clear()
		f.used = bitset.New(Entries)
		return f, nil
	}

	maxSection := fileSize / SectionSize
	f.used = bitset.New(uint(maxSection))

	var size, timestamps [Entries]uint32
	if err = f.readHeader(size[:], timestamps[:]); err != nil {
		return nil, err
	}

	for i := 0; i < Entries; i++ {
		c := Entry{Timestamp: int32(timestamps[i]), Size: uint8(size[i]), Offset: size[i] >> 8}

		start := c.Offset
		for p := uint32(0); p < uint32(c.Size); p++ {
			pos := start + p
			if pos > uint32(maxSection) {
				return nil, errors.CauseStr(ErrCorrupted, "invalid chunk data location")
			}
			if f.used.Test(uint(pos)) {
				return nil, errors.CauseStr(ErrCorrupted, "invalid chunk size/location")
			}

			f.used.Set(uint(pos))
		}

		header[i] = c
	}

	return f, nil
}

// readHeader reads the region file header.
func (f *File) readHeader(size, timestamps []uint32) (err error) {
	if err = f.readUint32Section(size[:], 0); err == nil {
		err = f.readUint32Section(timestamps[:], SectionSize)
	}
	return err
}

// readUint32Section reads a 4096 byte section at the given offset into the given uint32 slice.
func (f *File) readUint32Section(dst []uint32, offset int) error {
	tmp := sectionPool.Get().(*section)
	defer tmp.Free()

	if n, err := f.read.ReadAt(tmp[:], int64(offset)); err != nil {
		return errors.Wrap("anvil: unable to read file header", err)
	} else if n != SectionSize {
		return errors.Wrap("anvil: Incorrect number of bytes read", io.EOF)
	}

	fastbytes.BigEndian.ToU32(tmp[:], dst)
	return nil
}
