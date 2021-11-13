package anvil

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
	fs = afero.NewCopyOnWriteFs(afero.NewBasePathFs(&afero.OsFs{}, "./testdata"), &afero.MemMapFs{})
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
	f, err := OpenFile(name, false)
	is(err == nil, "unexpected error occurred while creating anvil file: %s", err)

	f.CompressionMethod(cm)

	var bb bytes.Buffer

	for i, buf := range sections {
		f.Write(uint8(i&0x1f), uint8(i>>5), buf)

		n, err := f.write.(io.Seeker).Seek(0, io.SeekEnd)
		is(err == nil, "unexpected error")
		is(n&sectionSizeMask == 0, "file size is not a multiple of `sectionSize`: %d", n)

		n, err = f.Read(uint8(i&0x1f), uint8(i>>5), &bb)
		is(err == nil, "failed to read data: %s", err)
		is(err == nil, "failed to read data: %s", err)
		is(bytes.Equal(buf, bb.Bytes()), "incorrect value read")
		bb.Reset()
	}
	f.Close()
	f, err = OpenFile(name, false)
	is(err == nil, "unexpected error occurred while opening anvil file: %s", err)
	for i, buf := range sections {
		_, err = f.Read(uint8(i&0x1f), uint8(i>>5), &bb)
		is(err == nil, "failed to read data: %s", err)
		is(err == nil, "failed to read data")
		is(bytes.Equal(buf, bb.Bytes()), "incorrect value read")
		bb.Reset()
	}

}
