package file

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/yehan2002/is/v2"
)

func init() {
	fs = afero.NewCopyOnWriteFs(afero.NewBasePathFs(&afero.OsFs{}, "../testdata"), &afero.MemMapFs{})
}

var compressionMethods = []CompressMethod{CompressionGzip, CompressionZlib, CompressionNone}

func TestWriteNew(t *testing.T) {
	sections := [1024][]byte{}
	for i := range sections {
		sections[i] = bytes.Repeat([]byte{byte(i + 1)}, (i+1)*128)
	}
	for _, method := range compressionMethods {
		testRoundtrip(is.New(t), method, "write-test-new", sections[:])
	}
}

func TestWriteNewLarge(t *testing.T) {
	sections := [16][]byte{}
	for i := range sections {
		buf := make([]byte, SectionSize*16)
		rand.Read(buf)
		sections[i] = buf
	}
	for _, method := range compressionMethods {
		testRoundtrip(is.New(t), method, "write-test-new-large", sections[:])
	}
}

func testRoundtrip(is is.Is, cm CompressMethod, name string, sections [][]byte) {
	name = fmt.Sprintf("%s-%s.mca", name, cm.String())
	f, err := Open(name)
	is(err == nil, "unexpected error occurred while creating anvil file: %s", err)

	f.CompressionMethod(cm)

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
		is(bytes.Equal(buf, data), "incorrect value read")
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
		is(bytes.Equal(buf, data), "incorrect value read")
	}

}
