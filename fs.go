package anvil

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

var osFs afero.Fs = &afero.OsFs{}

// Reader an interface that implements io.ReadAt and io.Closer
type Reader interface {
	io.ReaderAt
	io.Closer
}

// Writer a writer to modify an anvil file.
// The value returned by Fs.Open should implement this interface if the anvil file is modifiable
type Writer interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
}

var _ Writer = afero.File(nil)
var _ Writer = &os.File{}

// NewFs creates an Fs from the given afero.Fs.
func NewFs(f afero.Fs) *Fs { return &Fs{fs: f} }

// Fs handles opening anvil files.
type Fs struct{ fs afero.Fs }

// Open opens the given region file
func (d *Fs) Open(anvilX, anvilZ int32) (r Reader, size int64, err error) {
	if r, size, err = openFile(d.fs, fmt.Sprintf("r.%d.%d.mca", anvilX, anvilZ)); err == nil {
		return r, size, nil
	}
	return nil, 0, err
}

// ReadExternal reads an external .mcc file
func (d *Fs) ReadExternal(entryX, entryZ int32) (r io.ReadCloser, err error) {
	var f afero.File
	if f, err = osFs.Open(fmt.Sprintf("r.%d.%d.mcc", entryX, entryZ)); err != nil {
		return nil, errors.Wrap("anvil: unable to open external file", err)
	}
	return f, nil
}

// WriteExternal writes to an external .mcc file
func (d *Fs) WriteExternal(entryX, entryZ int32, b *Buffer) (err error) {
	var f afero.File
	if f, err = osFs.Create(fmt.Sprintf("r.%d.%d.mcc", entryX, entryZ)); err != nil {
		return errors.Wrap("anvil: unable to create external file", err)
	}
	return errors.Wrap("anvil: unable to write external file", b.WriteTo(f, false))
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
