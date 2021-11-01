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
	sections := [1024][]byte{}
	for i := range sections {
		sections[i] = bytes.Repeat([]byte{byte(i + 1)}, (i+1)*128)
	}
	testRoundtrip(is.New(t), "write-test-new.mca", sections[:])
}

func TestWriteNewLarge(t *testing.T) {
	sections := [16][]byte{}
	for i := range sections {
		buf := make([]byte, sectionSize*16)
		rand.Read(buf)
		sections[i] = buf
	}
	testRoundtrip(is.New(t), "write-test-new-large.mca", sections[:])
}

func testRoundtrip(is is.Is, name string, sections [][]byte) {
	f, err := Open(name)
	is(err == nil, "unexpected error occurred while creating anvil file: %s", err)

	for i, buf := range sections {
		f.Write(i&0x1f, i>>5, buf)

		n, err := f.f.Seek(0, io.SeekEnd)
		is(err == nil, "unexpected error")
		is(n&sectionSizeMask == 0, "file size is not a multiple of `sectionSize`: %d", n)

		r, err := f.Read(i&0x1f, i>>5)
		is(err == nil, "failed to read data: %s", err)
		data, err := io.ReadAll(r)
		_ = r.Close()
		is(err == nil, "failed to read data")
		is.Equal(buf, data, "incorrect value read")
	}
	f.f.Close()

	f, err = Open(name)
	is(err == nil, "unexpected error occurred while opening anvil file: %s", err)
	for i, buf := range sections {
		r, err := f.Read(i&0x1f, i>>5)
		is(err == nil, "failed to read data: %s", err)
		data, err := io.ReadAll(r)
		_ = r.Close()
		is(err == nil, "failed to read data")
		is.Equal(buf, data, "incorrect value read")
	}
}
