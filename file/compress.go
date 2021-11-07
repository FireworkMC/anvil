package file

import (
	"io"
	"sync"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zlib"
	"github.com/yehan2002/errors"
)

// CompressMethod the compression method used for compressing region data
type CompressMethod byte

// DefaultCompression the default compression method to be used
var DefaultCompression = CompressionZlib

// supported methods
const (
	CompressionGzip = 1 + iota
	CompressionZlib
	CompressionNone

	externalMask = 0x80
)

var (
	gzipDecompressPool = decompressorPool{new: func(src io.ReadCloser) (readCloseResetter, error) {
		return gzip.NewReader(src)
	}}
	zlibDecompressPool = decompressorPool{new: func(src io.ReadCloser) (readCloseResetter, error) {
		t, err := zlib.NewReader(src)
		return &zlibReadResetWrapper{t.(zlibReader)}, err
	}}
)

type decompressorPool struct {
	sync.Pool
	new func(io.ReadCloser) (readCloseResetter, error)
}

func (d *decompressorPool) Get(src io.ReadCloser) (dec *decompressor, err error) {
	var r readCloseResetter
	if reader := d.Pool.Get(); reader != nil {
		t := reader.(readCloseResetter)
		r, err = t, t.Reset(src)
	} else {
		r, err = d.new(src)
	}
	return &decompressor{src: src, r: r, pool: &d.Pool}, err
}

func (c CompressMethod) decompressor(src io.ReadCloser) (reader io.ReadCloser, err error) {
	switch c {
	case CompressionGzip:
		reader, err = gzipDecompressPool.Get(src)
	case CompressionZlib:
		reader, err = zlibDecompressPool.Get(src)
	case CompressionNone:
		reader = io.NopCloser(src)
	default:
		err = errors.Error("unsupported compression method")
	}
	return reader, errors.Wrap("anvil/file: unable to decompress", err)
}

func (c CompressMethod) compressor() (compressor, error) {
	switch c {
	case CompressionGzip:
		return gzip.NewWriter(io.Discard), nil
	case CompressionZlib:
		return zlib.NewWriter(io.Discard), nil
	case CompressionNone:
		return &noopCompressor{}, nil
	default:
		return nil, errors.Error("anvil/file: unsupported compression method")
	}
}

type compressor interface {
	io.WriteCloser
	Reset(io.Writer)
}

type zlibReader interface {
	io.ReadCloser
	zlib.Resetter
}

type zlibReadResetWrapper struct{ zlibReader }

func (z *zlibReadResetWrapper) Reset(r io.Reader) error { return z.zlibReader.Reset(r, nil) }

type readCloseResetter interface {
	io.Reader
	io.Closer
	Reset(io.Reader) error
}

var _ readCloseResetter = &zlibReadResetWrapper{}
var _ readCloseResetter = &gzip.Reader{}

type decompressor struct {
	src  io.ReadCloser
	r    io.ReadCloser
	pool *sync.Pool
}

func (z *decompressor) Read(p []byte) (n int, err error) { return z.r.Read(p) }

func (z *decompressor) Close() (err error) {
	if z.r != nil {
		z.src.Close()
		z.pool.Put(z.r)
		*z = decompressor{}
	}
	return
}

type noopCompressor struct{ dst io.Writer }

var _ compressor = &noopCompressor{}

func (n *noopCompressor) Write(p []byte) (int, error) { return n.dst.Write(p) }
func (n *noopCompressor) Close() error                { return nil }
func (n *noopCompressor) Reset(w io.Writer)           { n.dst = w }
