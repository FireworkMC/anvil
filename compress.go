package anvil

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
const DefaultCompression = CompressionZlib

// supported methods
const (
	CompressionGzip CompressMethod = 1 + iota
	CompressionZlib
	CompressionNone

	externalMask = 0x80
)

func (c CompressMethod) String() string {
	switch c {
	case CompressionGzip:
		return "gzip"
	case CompressionZlib:
		return "zlib"
	case CompressionNone:
		return "none"
	default:
		return "unsupported"
	}
}

var (
	gzipDecompressPool = decompressorPool{new: func(src io.ReadCloser) (readCloseResetter, error) {
		return gzip.NewReader(src)
	}}
	zlibDecompressPool = decompressorPool{new: func(src io.ReadCloser) (readCloseResetter, error) {
		t, err := zlib.NewReader(src)
		if err != nil {
			return nil, err
		}
		return &zlibReadResetWrapper{t.(zlibReader)}, err
	}}
)

// decompressorPool a pool of readCloseResetters that can be used to decompress data
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
	return &decompressor{src: src, decompress: r, pool: &d.Pool}, err
}

// decompressor returns a decompressor for the compression method.
// Callers must close the returned reader after use for it to be reused.
// Trying to use the reader after calling Close will cause a panic.
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
	return reader, errors.Wrap("anvil: unable to decompress", err)
}

// compressor returns a compressor for the compression method.
// Callers should reuse the returned compressor and should only
// create a new one when the compression method changes.
func (c CompressMethod) compressor() (compressor, error) {
	switch c {
	case CompressionGzip:
		return gzip.NewWriter(io.Discard), nil
	case CompressionZlib:
		return zlib.NewWriter(io.Discard), nil
	case CompressionNone:
		return &noopCompressor{}, nil
	default:
		return nil, errors.Error("anvil: unsupported compression method")
	}
}

type compressor interface {
	io.WriteCloser
	Reset(io.Writer)
}

type decompressor struct {
	src        io.ReadCloser
	decompress readCloseResetter
	pool       *sync.Pool
}

var _ io.ReadCloser = &decompressor{}

func (z *decompressor) Read(p []byte) (n int, err error) { return z.decompress.Read(p) }

func (z *decompressor) Close() (err error) {
	if z.decompress != nil {
		z.src.Close()
		z.pool.Put(z.decompress)
		*z = decompressor{}
	}
	return
}

type zlibReader interface {
	io.ReadCloser
	zlib.Resetter
}

// zlibReadResetWrapper a wrapper around zlib.Reader to make it implement the readResetCloser interface.
type zlibReadResetWrapper struct{ zlibReader }

func (z *zlibReadResetWrapper) Reset(r io.Reader) error { return z.zlibReader.Reset(r, nil) }

type readCloseResetter interface {
	io.Reader
	io.Closer
	Reset(io.Reader) error
}

var _ readCloseResetter = &zlibReadResetWrapper{}
var _ readCloseResetter = &gzip.Reader{}

// noopCompressor a compressor that does nothing.
type noopCompressor struct{ dst io.Writer }

var _ compressor = &noopCompressor{}

func (n *noopCompressor) Write(p []byte) (int, error) { return n.dst.Write(p) }
func (n *noopCompressor) Close() error                { return nil }
func (n *noopCompressor) Reset(w io.Writer)           { n.dst = w }

type muxReader struct {
	io.ReadCloser
	mux *sync.RWMutex
}

func (m *muxReader) Close() (err error) {
	if m.mux != nil {
		m.mux.RUnlock()
		err = m.ReadCloser.Close()
		*m = muxReader{}
	}
	return
}
