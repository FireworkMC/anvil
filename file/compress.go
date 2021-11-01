package file

import (
	"io"
	"sync"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zlib"
)

var (
	zlibPool = sync.Pool{}
	gzipPool = sync.Pool{}
)

type resetable interface{ Reset(r io.Reader) error }

func newZlibReader(src io.ReadCloser) (r io.ReadCloser, err error) {
	if reader := zlibPool.Get(); reader != nil {
		r = reader.(io.ReadCloser)
		err = r.(zlib.Resetter).Reset(src, nil)
	} else {
		r, err = zlib.NewReader(src)
	}

	return &compressor{r: r, src: src, pool: &zlibPool}, err
}

func newGzipReader(src io.ReadCloser) (r io.ReadCloser, err error) {
	if reader := gzipPool.Get(); reader != nil {
		r = reader.(io.ReadCloser)
		err = r.(resetable).Reset(src)
	} else {
		r, err = gzip.NewReader(src)
	}
	return &compressor{r: r, src: src, pool: &gzipPool}, err
}

type compressor struct {
	src  io.ReadCloser
	r    io.ReadCloser
	pool *sync.Pool
}

func (z *compressor) Read(p []byte) (n int, err error) { return z.r.Read(p) }

func (z *compressor) Close() (err error) {
	if z.r != nil {
		z.src.Close()
		z.pool.Put(z.r)
		*z = compressor{}
	}
	return
}
