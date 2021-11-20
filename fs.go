package anvil

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

var fs afero.Fs = &afero.OsFs{}

// reader an interface that implements io.ReadAt and io.Closer
type reader interface {
	io.ReaderAt
	io.Closer
}

// writer a writer to modify an anvil file.
// The value returned by Fs.Open should implement this interface if the anvil file is modifiable
type writer interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
}

var _ writer = afero.File(nil)

// NewFs creates an Fs from the given afero.Fs.
func NewFs(f afero.Fs) *Fs { return &Fs{fs: f, RegionFmt: "r.%d.%d.mca", ChunkFmt: "r.%d.%d.mcc"} }

// Fs handles opening anvil files.
type Fs struct {
	fs                  afero.Fs
	RegionFmt, ChunkFmt string
}

// open opens the given region file
func (d *Fs) open(anvilX, anvilZ int32, readonly bool) (r reader, size int64, err error) {
	if r, size, err = openFile(d.fs, fmt.Sprintf(d.RegionFmt, anvilX, anvilZ), readonly); err == nil {
		return r, size, nil
	}
	return nil, 0, err
}

// readExternal reads an external .mcc file
func (d *Fs) readExternal(entryX, entryZ int32) (r io.ReadCloser, err error) {
	var f afero.File
	if f, err = fs.Open(fmt.Sprintf(d.ChunkFmt, entryX, entryZ)); err != nil {
		return nil, errors.Wrap("anvil: unable to open external file", err)
	}
	return f, nil
}

// writeExternal writes to an external .mcc file
func (d *Fs) writeExternal(entryX, entryZ int32, b *buffer) (err error) {
	var f afero.File
	if f, err = fs.Create(fmt.Sprintf(d.ChunkFmt, entryX, entryZ)); err != nil {
		return errors.Wrap("anvil: unable to create external file", err)
	}
	return errors.Wrap("anvil: unable to write external file", b.WriteTo(f, false))
}

func openFile(fs afero.Fs, path string, readonly bool) (r reader, size int64, err error) {
	var fileSize int64
	if info, err := fs.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, 0, err
		}
	} else {
		fileSize = info.Size()
	}

	var fileFlags = os.O_RDWR | os.O_CREATE
	if readonly {
		if fileSize == 0 {
			return
		}
		fileFlags = os.O_RDONLY
	}

	var f afero.File
	if f, err = fs.OpenFile(path, fileFlags, 0666); err != nil {
		return nil, 0, errors.Wrap("anvil: unable to open file", err)
	}
	return f, fileSize, nil
}
