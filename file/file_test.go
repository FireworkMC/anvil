package file

import (
	"bytes"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/yehan2002/is/v2"
)

func init() {
	fs = afero.NewCopyOnWriteFs(afero.NewBasePathFs(&afero.OsFs{}, "../testdata"), &afero.MemMapFs{})
}

func TestWriteNew(t *testing.T) {
	is := is.New(t)
	sections := [16][]byte{}
	for i := range sections {
		sections[i] = bytes.Repeat([]byte{byte(i + 1)}, (i+1)*sectionSize)
	}
	f, err := Open("write-test-new.mca")
	is(err == nil, "unexpected error occurred while creating anvil file: %s", err)
	for i := range sections {
		f.Write(i, i, sections[i])

		n, err := f.f.Seek(0, io.SeekEnd)
		is(err == nil, "unexpected error")
		is(n&sectionSizeMask == 0, "file size is not a multiple of `sectionSize`: %d", n)

		r, err := f.Read(i, i)
		is(err == nil, "failed to read data: %s", err)
		buf, err := io.ReadAll(r)
		is(err == nil, "failed to read data")
		is.Equal(buf, sections[i], "incorrect value read")
	}
}
