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

type readAtCloser struct{ io.ReaderAt }

func (r *readAtCloser) Close() error { return nil }

// writer a writer to modify an anvil file.
type writer interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
}

var _ writer = afero.File(nil)

func openFile(path string, settings Settings) (r reader, size int64, err error) {
	var fileSize int64
	if info, err := settings.fs.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, 0, err
		}
	} else {
		fileSize = info.Size()
	}

	var fileFlags = os.O_CREATE

	if settings.Sync {
		fileFlags |= os.O_SYNC
	}

	if settings.ReadOnly {
		if fileSize == 0 {
			return
		}
		fileFlags = os.O_RDONLY
	} else {
		fileFlags |= os.O_RDWR
	}

	var f afero.File
	if f, err = settings.fs.OpenFile(path, fileFlags, 0666); err != nil {
		return nil, 0, errors.Wrap("anvil: unable to open file", err)
	}
	return f, fileSize, nil
}
