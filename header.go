package anvil

import (
	"sync"

	"github.com/bits-and-blooms/bitset"
)

var headerPool = sync.Pool{New: func() interface{} { return &[entries]Entry{} }}

// Region the position of a Region file
type Region struct{ x, z int32 }

// Chunk gets the chunk position for the given postion
func (r *Region) Chunk(x, z uint8) (int32, int32) { return r.x<<5 | int32(x), r.z<<5 | int32(z) }

// sections returns the minimum number of sections to store the given number of bytes
func sections(v uint) uint { return (v + sectionSizeMask) / sectionSize }

// Entry an entry in the region file
type Entry struct {
	// Size the number of sections used by this entry
	// If this is zero the data has not been generated yet and is not stored in this file.
	Size uint8
	// Offset the offset of the chunk in the region file (in sections).
	// The maximum offset is (2<<24)-1 sections.
	Offset uint32
	// Timestamp the Timestamp when the chunk was last modified.
	// This is stored as the number of seconds since January 1, 1970 UTC.
	Timestamp int32
}

// Generated returns if the entry is stored in this file.
func (e *Entry) Generated() bool { return e.Offset != 0 && e.Size != 0 }

// OffsetBytes returns the offset in bytes
func (e *Entry) OffsetBytes() int64 { return int64(e.Offset) * sectionSize }

// Header the header of the region file.
type Header struct {
	entries *[entries]Entry
	used    *bitset.BitSet
}

// Get gets the entry at the given x,z coords.
// If the given x,z values are not between 0 and 31 (inclusive) this panics.
func (h *Header) Get(x, z uint8) *Entry {
	if x > 31 || z > 31 {
		panic("invalid position")
	}
	return &h.entries[uint16(x&0x1f)|(uint16(z&0x1f)<<5)]
}

func (h *Header) clear() { *h.entries = [entries]Entry{} }

// Set updates the entry at x,z and the given marks the
// space used by the given entry in the `used` bitset as used.
func (h *Header) Set(x, z uint8, c Entry) {
	old := h.Get(x, z)
	if old.Generated() {
		h.freeSpace(old)
	}
	h.markSpace(c)
	*old = c
}

// Remove removes the given entry from the header and marks the space used
// by the given entry in the `used` bitset as unused.
func (h *Header) Remove(x, z uint8) {
	e := h.Get(x, z)
	h.freeSpace(e)
	*e = Entry{}
}

// markSpace marks the space used by the given entry as used.
// This panics if the entry overflows into used an area.
func (h *Header) markSpace(c Entry) {
	end := uint(c.Offset) + uint(c.Size)
	for i := uint(c.Offset); i < end; i++ {

		if h.used.Test(i) {
			panic("anvil: Header: entry overflows into used space")
		}

		h.used.Set(i)
	}
}

// freeSpace marks the space used by the entry as unused.
// This panics if any area used by the entry is not marked as used.
func (h *Header) freeSpace(c *Entry) {
	if c.Offset == 0 || c.Size == 0 {
		return
	}
	end := uint(c.Offset) + uint(c.Size)
	for i := uint(c.Offset); i < end; i++ {
		if !h.used.Test(i) {
			panic("anvil: Header: inconsistent usage of space")
		}
		h.used.Clear(i)
	}
}

// FindSpace finds the next free space large enough to store `size` sections
func (h *Header) FindSpace(size uint) (offset uint, found bool) {
	// ignore the first two section since they are used for the header
	offset = 2

	var hasSpace = true
	for hasSpace {
		var next uint

		offset, hasSpace = h.used.NextClear(offset)
		if !hasSpace {
			break
		}

		next, hasSpace = h.used.NextSet(offset)
		if hasSpace && next-offset >= size {
			return offset, true
		}

		offset = next
	}

	return 0, false
}

// Free frees the header and puts it into the pool.
// Callers must not use the header after calling this.
func (h *Header) Free() { headerPool.Put(h.entries) }

func newHeader() *Header { return &Header{entries: headerPool.Get().(*[entries]Entry)} }
