package anvil

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/spf13/afero/mem"
	"github.com/yehan2002/is/v2"
)

func init() {
	filesystem = afero.NewCopyOnWriteFs(afero.NewBasePathFs(&afero.OsFs{}, "./testdata"), &afero.MemMapFs{})
}

var compressionMethods = []CompressMethod{CompressionGzip, CompressionZlib, CompressionNone}

func TestWriteNew(t *testing.T) {
	sections := [512][]byte{}
	for i := range sections {
		sections[i] = bytes.Repeat([]byte{byte(i + 1)}, (i+1)*256)
	}
	for _, method := range compressionMethods {
		testRoundtrip(is.New(t), method, "write-test-new", sections[:])
	}
}

func TestWriteNewLarge(t *testing.T) {
	sections := [16][]byte{}
	for i := range sections {
		buf := make([]byte, SectionSize*16)
		_, err := rand.Read(buf)
		if err != nil {
			t.Fatal(err)
		}

		sections[i] = buf
	}
	for _, method := range compressionMethods {
		testRoundtrip(is.New(t), method, "write-test-new-large", sections[:])
	}
}

func TestWriteExternal(t *testing.T) {
	err := filesystem.MkdirAll("test-write-external", 0o777)
	if err != nil {
		t.Fatal("unable to make test directory.", err)
	}

	fs := afero.NewBasePathFs(filesystem, "test-write-external")
	c, err := OpenFs(fs)
	if err != nil {
		t.Fatal(err)
	}
	var sections [Entries][]byte
	var bufferLengths = [5]int{100, 4096, 4097, 10240, 4096 * 256}
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			if (x+z)%2 == 1 {
				continue
			}
			length := bufferLengths[((x+z)%10)/2]
			buf := make([]byte, length)
			buf[0] = byte(x)
			buf[1] = byte(z)
			buf[length-2] = byte(x)
			buf[length-1] = byte(z)
			sections[x|z<<5] = buf
		}
	}

	is := is.New(t)
	var bb bytes.Buffer

	for i, buf := range sections {
		err := c.Write(int32(i&0x1f), int32(i>>5), buf)
		is(err == nil, "failed to write data: %s", err)

		// n, err := c.write.(io.Seeker).Seek(0, io.SeekEnd)
		is(err == nil, "unexpected error")
		// is(n&sectionSizeMask == 0, "file size is not a multiple of `sectionSize`: %d", n)

		_, err = c.ReadTo(int32(i&0x1f), int32(i>>5), &bb)
		if (int32(i&0x1f)+int32(i>>5))%2 == 1 {
			is.Err(err, ErrNotExist, "unexpected state")
			continue
		}

		is(err == nil, "failed to read data: (%d,%d) %s", int32(i&0x1f), int32(i>>5), err)
		is(bytes.Equal(buf, bb.Bytes()), "incorrect value read")
		bb.Reset()
	}

	c, err = OpenFs(fs)
	if err != nil {
		t.Fatal(err)
	}

	for i, buf := range sections {
		_, err = c.ReadTo(int32(i&0x1f), int32(i>>5), &bb)
		if (int32(i&0x1f)+int32(i>>5))%2 == 1 {
			is.Err(err, ErrNotExist, "unexpected state")
			continue
		}

		is(err == nil, "failed to read data: (%d,%d) %s", int32(i&0x1f), int32(i>>5), err)
		is(bytes.Equal(buf, bb.Bytes()), "incorrect value read")
		bb.Reset()
	}
}

func testRoundtrip(is is.Is, cm CompressMethod, name string, sections [][]byte) {
	name = fmt.Sprintf("%s-%s.mca", name, cm.String())
	memFile := mem.NewFileHandle(mem.CreateFile(name))

	f, err := ReadAnvil(0, 0, memFile, 0, nil, Settings{})
	is(err == nil, "unexpected error occurred while creating anvil file: %s", err)

	err = f.CompressionMethod(cm)
	is(err == nil, "unable to set compression method")

	var bb bytes.Buffer

	for i, buf := range sections {
		err = f.Write(uint8(i&0x1f), uint8(i>>5), buf)
		is(err == nil, "unexpected error")

		n, err := f.(*file).writer.(io.Seeker).Seek(0, io.SeekEnd)
		is(err == nil, "unexpected error")
		is(n&sectionSizeMask == 0, "file size is not a multiple of `sectionSize`: %d", n)

		readFnTest(is, f, uint8(i&0x1f), uint8(i>>5), &bb)
		is(bytes.Equal(buf, bb.Bytes()), "incorrect value read")
		bb.Reset()
	}

	f, err = ReadAnvil(0, 0, memFile, memFile.Info().Size(), nil, Settings{})
	is(err == nil, "unexpected error occurred while opening anvil file: %s", err)
	for i, buf := range sections {
		readFnTest(is, f, uint8(i&0x1f), uint8(i>>5), &bb)
		is(bytes.Equal(buf, bb.Bytes()), "incorrect value read")
		bb.Reset()
	}

}

// tests if all 3 read functions return the same result
func readFnTest(is is.Is, f File, x, z uint8, buf *bytes.Buffer) {
	data, err := f.Read(x, z)
	is(err == nil, "Read: failed to read data: %s", err)

	var tmpBuf bytes.Buffer
	tmpBuf.Grow(len(data))

	_, err = f.ReadTo(x, z, &tmpBuf)
	is(err == nil, "ReadTo: failed to read data: %s", err)
	is(bytes.Equal(tmpBuf.Bytes(), data), "Read and ReadTo returned different results")

	err = f.ReadWith(x, z, func(r io.Reader) error {
		tmpBuf.Reset()
		_, e := tmpBuf.ReadFrom(r)
		return e
	})

	is(err == nil, "ReadWith: failed to read data: %s", err)
	is(bytes.Equal(tmpBuf.Bytes(), data), "Read and ReadWith returned different results")

	buf.Reset()
	buf.Write(data)
}
