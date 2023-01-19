package anvil

import (
	"io"
	"os"

	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

// reader an interface that implements io.ReadAt and io.Closer
type reader interface {
	io.ReaderAt
	io.Closer
}

type noopReadAtCloser struct{ io.ReaderAt }

func (r *noopReadAtCloser) Close() error { return nil }

// writer a writer to modify an anvil file.
type writer interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
}

var _ writer = afero.File(nil)

func openFile(path string, settings Settings) (r reader, size int64, err error) {
	var fileFlags int

	if settings.ReadOnly {
		fileFlags = os.O_RDONLY
	} else {
		fileFlags = os.O_RDWR | os.O_CREATE
	}

	if settings.Sync {
		fileFlags |= os.O_SYNC
	}

	var f afero.File
	if f, err = settings.fs.OpenFile(path, fileFlags, 0666); err != nil {
		return nil, 0, errors.Wrap("anvil: unable to open file", err)
	}

	var fileSize int64
	if info, err := f.Stat(); err != nil {
		return nil, 0, errors.Wrap("anvil: unable to stat file", err)
	} else {
		fileSize = info.Size()
	}

	return f, fileSize, nil
}
