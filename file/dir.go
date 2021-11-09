package file

import (
	"fmt"
	"io"

	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

// Fs handles opening anvil files.
type Fs interface {
	Open(rg Region) (r ReadAtCloser, size int64, readonly bool, err error)
	ReadExternal(c Chunk) (r io.ReadCloser, err error)
	WriteExternal(c Chunk, b *Buffer) (err error)
}

type dir struct{ fs afero.Fs }

// Open opens the given region file
func (d *dir) Open(x, z int) (r ReadAtCloser, size int64, readonly bool, err error) {
	if r, size, err = openFile(d.fs, fmt.Sprintf("r.%d.%d.mca", x>>5, z>>5)); err == nil {
		return r, size, false, nil
	}
	return nil, 0, false, err
}

// ReadExternal reads an external .mcc file
func (d *dir) ReadExternal(x, z int) (r io.ReadCloser, err error) {
	var f afero.File
	if f, err = fs.Open(fmt.Sprintf("r.%d.%d.mcc", x, z)); err != nil {
		return nil, errors.Wrap("anvil/file: unable to open external file", err)
	}
	return f, nil
}

// WriteExternal writes to an external .mcc file
func (d *dir) WriteExternal(x, z int, b *Buffer) (err error) {
	var f afero.File
	if f, err = fs.Create(fmt.Sprintf("r.%d.%d.mcc", x, z)); err != nil {
		return errors.Wrap("anvil/file: unable to create external file", err)
	}
	return errors.Wrap("anvil/file: unable to write external file", b.WriteTo(f, false))
}
