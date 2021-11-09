package file

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/yehan2002/is/v2"
)

type bufferTest struct{}

func TestBuffer(t *testing.T) { is.Suite(t, &bufferTest{}) }

func (b *bufferTest) TestBufferWrite(is is.Is) {
	buf := Buffer{}
	defer buf.Free()

	data := []byte{1, 2, 3, 4}
	expected := append(zeroHeader[:], data...)
	n, _ := buf.Write(data)
	is(n == len(data), "Write returned an incorrect number of bytes")
	is.Equal(buf.buf[0][:buf.length], expected, "incorrect internal state")

	expected = append(expected, data...)
	n, _ = buf.Write(data)
	is(n == len(data), "Write returned an incorrect number of bytes")
	is.Equal(buf.buf[0][:buf.length], expected, "incorrect internal state")
}

func (b *bufferTest) TestBufferWriteLarge(is is.Is) {
	buf := Buffer{}
	defer buf.Free()
	byteBuffer := bytes.Buffer{}

	data := section{}
	b.setAllSection(&data, 1)

	expected := append([]byte(nil), data[:]...)
	n, _ := buf.Write(data[:])
	is(n == len(data), "Write returned an incorrect number of bytes")
	buf.WriteTo(&byteBuffer, false)
	is.Equal(byteBuffer.Bytes(), expected[:], "incorrect bytes written")

	b.setAllSection(&data, 2)
	byteBuffer.Reset()
	expected = append(expected, data[:]...)
	n, _ = buf.Write(data[:])
	is(n == len(data), "Write returned an incorrect number of bytes")
	buf.WriteTo(&byteBuffer, false)
	is.Equal(byteBuffer.Bytes(), expected[:], "incorrect bytes written")
}

func (b *bufferTest) TestHeader(is is.Is) {
	var u32 = binary.BigEndian.Uint32

	buf := Buffer{}
	testData := []byte{0}
	bytes := bytes.Buffer{}

	buf.Write(testData)
	buf.WriteTo(&bytes, true)
	written := bytes.Bytes()
	is(u32(written) == uint32(len(testData))+1, "incorrect length written")
	is(written[4] == byte(DefaultCompression), "incorrect compression method written")

	buf.Free()
	bytes.Reset()

	buf.Write(testData)
	buf.CompressMethod(CompressionGzip)
	buf.WriteTo(&bytes, true)
	written = bytes.Bytes()
	is.Equal(u32(written), uint32(len(testData))+1, "incorrect length written")
	is.Equal(written[4], byte(CompressionGzip), "incorrect compression method written")

}

func (b *bufferTest) setAllSection(s *section, v byte) {
	for i := range s {
		s[i] = v
	}
}

func (b *bufferTest) TestBufferLength(is is.Is) {
	buf := Buffer{}
	defer buf.Free()

	is(buf.Len() == 0, "buffer returned incorrect length")
	buf.Write([]byte{})
	is(buf.Len() == 0, "buffer returned incorrect length")
	buf.Write([]byte{1})
	is(buf.Len() == 6, "buffer returned incorrect length")
}
