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
	tfs := readTestData(is, "test_simple.tzst")
	test := f.testFile(is, tfs)
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			test(x, z)
		}
	}
}

func (f *fileTest) TestExternal(is is.Is) {
	tfs := readTestData(is, "test_external.tzst")
	test := f.testFile(is, tfs)
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			test(x, z)
		}
	}
}

func (f *fileTest) TestMultpleFiles(is is.Is) {
	tfs := readTestData(is, "test_multiple_files.tzst")
	test := f.testFile(is, tfs)
	for x := -31; x < 64; x++ {
		for z := -31; z < 64; z++ {
			test(x, z)
		}
	}
}

func (f *fileTest) testFile(is is.Is, tfs afero.Fs) func(x, z int) {
	cache, err := OpenFs(tfs, Settings{ReadOnly: true, CacheSize: 5})
	is(err == nil, "unexpected error while opening")

	return func(x, z int) {
		anvilBuffer := bytes.Buffer{}
		_, cacheErr := cache.ReadTo(int32(x), int32(z), &anvilBuffer)
		buf, fileErr := afero.ReadFile(tfs, fmt.Sprintf("chunks/%d.%d.nbt", x, z))

		f, err := cache.get(int32(x>>5), int32(z>>5))
		is(err == nil, "unexpected error")
		defer func() { is(cache.free(f) == nil, "unable to free file") }()

		if errors.Is(fileErr, os.ErrNotExist) && errors.Is(cacheErr, ErrNotExist) {
			return
		}

		if cacheErr != nil || fileErr != nil {
			is.Log("header at (%d,%d): %#v", x, z, f.header.Get(uint8(x&31), uint8(z&31)))
			is(cacheErr == nil, "unexpected error while reading test data: x: %d z:%d c: %s, f: %s ", x, z, cacheErr, fileErr)
			is(fileErr == nil, "unexpected error while reading test data: x: %d z:%d f: %s, c: %s,", x, z, fileErr, cacheErr)
		}

		is(bytes.Equal(buf, anvilBuffer.Bytes()), "incorrect data read at %x, %x", x, z)
	}

}

func readTestData(is is.Is, name string) afero.Fs {
	f, err := filesystem.Open(name)
	is(err == nil, "unexpected error while reading test data: %s", err)
	defer f.Close()

	dec, err := zstd.NewReader(f)
	is(err == nil, "unexpected error while decompressing test data: %s", err)
	buf := bytes.Buffer{}
	_, err = io.Copy(&buf, dec)
	is(err == nil, "unexpected error while decompressing test data: %s", err)

	return tarfs.New(tar.NewReader(&buf))
}
