package anvil

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/yehan2002/is/v2"
)

type bufferTest struct{}

func TestBuffer(t *testing.T) { is.SuiteP(t, &bufferTest{}) }

func (b *bufferTest) TestBufferWrite(is is.Is) {
	buf := buffer{}
	defer buf.Reset()

	data := []byte{1, 2, 3, 4}
	expected := data
	n := buf.AppendBytes(data)
	is(n == len(data), "Write returned an incorrect number of bytes")
	is.Equal(buf.buf[0][5:buf.length], expected, "incorrect internal state")

	expected = append(data, data...)
	n = buf.AppendBytes(data)
	is(n == len(data), "Write returned an incorrect number of bytes")
	is.Equal(buf.buf[0][5:buf.length], expected, "incorrect internal state")
}

func (b *bufferTest) TestBufferWriteLarge(is is.Is) {
	buf := buffer{}
	defer buf.Reset()
	byteBuffer := bytes.Buffer{}

	data := section{}
	b.setAllSection(&data, 1)

	expected := append([]byte(nil), data[:]...)
	n := buf.AppendBytes(data[:])
	is(n == len(data), "Write returned an incorrect number of bytes")
	err := buf.WriteTo(&byteBuffer, false)
	is(err == nil, "unexpected error: %s", err)
	is.Equal(byteBuffer.Bytes(), expected[:], "incorrect bytes written")

	b.setAllSection(&data, 2)
	byteBuffer.Reset()
	expected = append(expected, data[:]...)
	n = buf.AppendBytes(data[:])
	is(n == len(data), "Write returned an incorrect number of bytes")
	err = buf.WriteTo(&byteBuffer, false)
	is(err == nil, "unexpected error: %s", err)
	is.Equal(byteBuffer.Bytes(), expected[:], "incorrect bytes written")
}

func (b *bufferTest) TestHeader(is is.Is) {
	var u32 = binary.BigEndian.Uint32

	buf := buffer{}
	testData := []byte{0}
	bytes := bytes.Buffer{}

	buf.AppendBytes(testData)
	err := buf.WriteTo(&bytes, true)
	is(err == nil, "unexpected error: %s", err)
	written := bytes.Bytes()
	is(u32(written) == uint32(len(testData))+1, "incorrect length written")
	is(written[4] == byte(DefaultCompression), "incorrect compression method written")

	buf.Reset()
	bytes.Reset()

	buf.AppendBytes(testData)
	is(err == nil, "unexpected error: %s", err)
	buf.CompressMethod(CompressionGzip)
	err = buf.WriteTo(&bytes, true)
	is(err == nil, "unexpected error: %s", err)
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
	buf := buffer{}
	defer buf.Reset()

	is(buf.Len() == 0, "buffer returned incorrect length")
	_, err := buf.Write([]byte{})
	is(err == nil, "unexpected error: %s", err)
	is(buf.Len() == 0, "buffer returned incorrect length")
	_, err = buf.Write([]byte{1})
	is(err == nil, "unexpected error: %s", err)
	is(buf.Len() == 6, "buffer returned incorrect length")
}
