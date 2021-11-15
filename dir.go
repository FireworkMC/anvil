package anvil

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

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

// Fs handles opening anvil files.
type Fs interface {
	Open(rg Region) (r Reader, size int64, readonly bool, err error)
	ReadExternal(c Chunk) (r io.ReadCloser, err error)
	WriteExternal(c Chunk, b *Buffer) (err error)
}

var _ Writer = afero.File(nil)
var _ Writer = &os.File{}

type dir struct{ fs afero.Fs }

// Open opens the given region file
func (d *dir) Open(x, z int) (r Reader, size int64, readonly bool, err error) {
	if r, size, err = openFile(d.fs, fmt.Sprintf("r.%d.%d.mca", x>>5, z>>5)); err == nil {
		return r, size, false, nil
	}
	return nil, 0, false, err
}

// ReadExternal reads an external .mcc file
func (d *dir) ReadExternal(x, z int) (r io.ReadCloser, err error) {
	var f afero.File
	if f, err = fs.Open(fmt.Sprintf("r.%d.%d.mcc", x, z)); err != nil {
		return nil, errors.Wrap("anvil: unable to open external file", err)
	}
	return f, nil
}

// WriteExternal writes to an external .mcc file
func (d *dir) WriteExternal(x, z int, b *Buffer) (err error) {
	var f afero.File
	if f, err = fs.Create(fmt.Sprintf("r.%d.%d.mcc", x, z)); err != nil {
		return errors.Wrap("anvil: unable to create external file", err)
	}
	return errors.Wrap("anvil: unable to write external file", b.WriteTo(f, false))
}
