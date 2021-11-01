package file

import (
	"bytes"
	"crypto/rand"
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
	sections := [1024][]byte{}
	for i := range sections {
		sections[i] = bytes.Repeat([]byte{byte(i + 1)}, (i+1)*128)
	}
	f, err := Open("write-test-new.mca")
	is(err == nil, "unexpected error occurred while creating anvil file: %s", err)
	for i := range sections {
		f.Write(i&0x1f, i>>5, sections[i])

		n, err := f.f.Seek(0, io.SeekEnd)
		is(err == nil, "unexpected error")
		is(n&sectionSizeMask == 0, "file size is not a multiple of `sectionSize`: %d", n)

		r, err := f.Read(i&0x1f, i>>5)
		is(err == nil, "failed to read data: %s", err)
		buf, err := io.ReadAll(r)
		r.Close()
		is(err == nil, "failed to read data")
		is.Equal(buf, sections[i], "incorrect value read")
	}
}

func TestWriteLarge(t *testing.T) {
	is := is.New(t)
	sections := [16][]byte{}
	for i := range sections {
		buf := make([]byte, sectionSize*16)
		rand.Read(buf)
		sections[i] = buf
	}
	f, err := Open("write-test-new.mca")
	is(err == nil, "unexpected error occurred while creating anvil file: %s", err)
	for i := range sections {
		f.Write(i&0x1f, i>>5, sections[i])

		n, err := f.f.Seek(0, io.SeekEnd)
		is(err == nil, "unexpected error")
		is(n&sectionSizeMask == 0, "file size is not a multiple of `sectionSize`: %d", n)

		r, err := f.Read(i&0x1f, i>>5)
		is(err == nil, "failed to read data: %s", err)
		buf, err := io.ReadAll(r)
		r.Close()
		is(err == nil, "failed to read data")
		is.Equal(buf, sections[i], "incorrect value read")
	}
}
