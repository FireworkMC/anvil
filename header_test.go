package anvil

import (
	"testing"

	"github.com/bits-and-blooms/bitset"
	"github.com/yehan2002/is/v2"
)

func TestHeader(t *testing.T) { is.SuiteP(t, &headerTest{}) }

type headerTest struct{}

func makeHeader() *Header {
	h := newHeader()
	h.used = bitset.New(Entries)
	h.used.Set(0)
	h.used.Set(1)
	return h
}

func (*headerTest) TestSetRemove(is is.Is) {
	h := makeHeader()
	var used uint32 = 2
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			size := uint8(x&0xF<<4 + z&0xF)
			err := h.Set(uint8(x), uint8(z), Entry{offset: used, size: uint8(size)})
			is(err == nil, "unexpected error returned by Header.Set")
			used += uint32(size)
			is(h.used.Count() == uint(used), "incorrect number of sections used")
		}
	}

	var offset uint32 = 2
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			size := uint8(x&0xF<<4 + z&0xF)
			err := h.Remove(uint8(x), uint8(z))
			is(err == nil, "unexpected error returned by Header.Remove")
			is(h.used.Count() == uint(used-uint32(size)), "incorrect number of sections used")
			err = h.Set(uint8(x), uint8(z), Entry{offset: offset, size: size})
			is(err == nil, "unexpected error returned by Header.Set")
			offset += uint32(size)
			is(h.used.Count() == uint(used), "incorrect number of sections used")
		}
	}

	offset = 2
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			size := uint8(x&0xF<<4 + z&0xF)
			err := h.Set(uint8(x), uint8(z), Entry{offset: offset, size: size})
			is(err == nil, "unexpected error returned by Header.Set")
			is(h.used.Count() == uint(used), "incorrect number of sections used")
			offset += uint32(size)
		}
	}
}

func (*headerTest) TestGet(is is.Is) {
	var actual [Entries]Entry

	header := newHeader()
	header.clear()
	defer header.Free()

	is.Equal(header.entries[:], actual[:], "incorrect header clear")

	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			v := uint32(x)<<16 | uint32(z)
			header.Get(uint8(x), uint8(z)).offset = v
			actual[z*32+x] = Entry{offset: v}
		}
	}
	is.Equal(header.entries[:], actual[:], "incorrect header modification")
	is.Panic(func() { header.Get(32, 0) }, "header did not panic for invalid coords")
	is.Panic(func() { header.Get(0, 32) }, "header did not panic for invalid coords")
}
