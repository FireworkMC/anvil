package anvil

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/afero"
	"github.com/spf13/afero/tarfs"
	"github.com/yehan2002/is/v2"
)

type fileTest struct{}

func TestFile(t *testing.T) { is.SuiteP(t, &fileTest{}) }

func (f *fileTest) TestHeader(is is.Is) {
	tfs := f.readTestData(is)
	cache, err := OpenFs(NewFs(tfs), true, 10)
	is(err == nil, "unexpected error while opening")
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			anvilBuffer := bytes.Buffer{}
			_, err = cache.Read(int32(x), int32(z), &anvilBuffer)
			is(err == nil, "unexpected error while reading test data: %s", err)
			buf, err := afero.ReadFile(tfs, fmt.Sprintf("chunks/%d.%d.nbt", x, z))
			is(err == nil, "unexpected error while reading test data: %s", err)
			is.Equal(buf, anvilBuffer.Bytes(), "incorrect data read at %x, %x", x, z)
		}
	}
}

func (*fileTest) readTestData(is is.Is) afero.Fs {
	f, err := fs.Open("testdata.tzst")
	defer f.Close()
	is(err == nil, "unexpected error while reading test data: %s", err)
	dec, err := zstd.NewReader(f)
	is(err == nil, "unexpected error while decompressing test data: %s", err)
	out, err := fs.Create("testdata.tar")
	_, err = io.Copy(out, dec)
	is(err == nil, "unexpected error while decompressing test data: %s", err)
	err = out.Close()
	is(err == nil, "unexpected error while decompressing test data: %s", err)

	tarFile, err := fs.Open("testdata.tar")
	is(err == nil, "unexpected error while reading test data: %s", err)

	return tarfs.New(tar.NewReader(tarFile))
}
