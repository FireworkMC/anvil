package anvil

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/afero"
	"github.com/spf13/afero/tarfs"
	"github.com/yehan2002/is/v2"
)

type fileTest struct{}

func TestFile(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	is.Suite(t, &fileTest{})
}

func (f *fileTest) TestSimple(is is.Is) {
	test := f.testFile(is, "test_simple.tzst")
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			test(x, z)
		}
	}
}

func (f *fileTest) TestMultpleFiles(is is.Is) {
	test := f.testFile(is, "test_multiple_files.tzst")
	for x := -31; x < 64; x++ {
		for z := -31; z < 64; z++ {
			test(x, z)
		}
	}
}

func (f *fileTest) testFile(is is.Is, name string) func(x, z int) {
	tfs := readTestData(is, name)
	cache, err := OpenFs(NewFs(tfs), true, 5)
	is(err == nil, "unexpected error while opening")

	return func(x, z int) {
		anvilBuffer := bytes.Buffer{}
		_, cacheErr := cache.Read(int32(x), int32(z), &anvilBuffer)
		buf, fileErr := afero.ReadFile(tfs, fmt.Sprintf("chunks/%d.%d.nbt", x, z))

		if errors.Is(cacheErr, os.ErrNotExist) && errors.Is(cacheErr, ErrNotGenerated) {
			return
		}

		is(cacheErr == nil, "unexpected error while reading test data: c: %s, f: %s ", cacheErr, fileErr)
		is(fileErr == nil, "unexpected error while reading test data: f: %s, c: %s,", fileErr, cacheErr)

		is(bytes.Equal(buf, anvilBuffer.Bytes()), "incorrect data read at %x, %x", x, z)
	}

}

func readTestData(is is.Is, name string) afero.Fs {
	f, err := fs.Open(name)
	is(err == nil, "unexpected error while reading test data: %s", err)
	defer f.Close()

	dec, err := zstd.NewReader(f)
	is(err == nil, "unexpected error while decompressing test data: %s", err)
	buf := bytes.Buffer{}
	_, err = io.Copy(&buf, dec)
	is(err == nil, "unexpected error while decompressing test data: %s", err)

	return tarfs.New(tar.NewReader(&buf))
}
