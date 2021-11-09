package file

import (
	"testing"

	"github.com/yehan2002/is/v2"
)

type fileTest struct{}

func TestFile(t *testing.T) { is.SuiteP(t, &fileTest{}) }

func (f *fileTest) TestHeader(is is.Is) {
	var actual [Entries]Entry

	header := headerPool.Get().(*Header)
	header.clear()
	defer header.Free()

	is.Equal(header[:], actual[:], "incorrect header clear")

	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			v := uint32(x)<<16 | uint32(z)
			header.Get(uint8(x), uint8(z)).Offset = v
			actual[z*32+x] = Entry{Offset: v}
		}
	}
	is.Equal(header[:], actual[:], "incorrect header modification")
	is.Panic(func() { header.Get(32, 0) }, "header did not panic for invalid coords")
	is.Panic(func() { header.Get(0, 32) }, "header did not panic for invalid coords")
}
