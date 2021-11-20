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
	h.used = bitset.New(entries)
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
			h.Set(uint8(x), uint8(z), Entry{Offset: used, Size: uint8(size)})
			used += uint32(size)
			is(h.used.Count() == uint(used), "incorrect number of sections used")
		}
	}

	var offset uint32 = 2
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			size := uint8(x&0xF<<4 + z&0xF)
			h.Remove(uint8(x), uint8(z))
			is(h.used.Count() == uint(used-uint32(size)), "incorrect number of sections used")
			h.Set(uint8(x), uint8(z), Entry{Offset: offset, Size: size})
			offset += uint32(size)
			is(h.used.Count() == uint(used), "incorrect number of sections used")
		}
	}

	offset = 2
	for x := 0; x < 32; x++ {
		for z := 0; z < 32; z++ {
			size := uint8(x&0xF<<4 + z&0xF)
			h.Set(uint8(x), uint8(z), Entry{Offset: offset, Size: size})
			is(h.used.Count() == uint(used), "incorrect number of sections used")
			offset += uint32(size)
		}
	}
}
