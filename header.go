package anvil

import (
	"sync"
)

var headerPool = sync.Pool{New: func() interface{} { return &Header{} }}

// Header the header of the region file.
type Header [Entries]Entry

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

// Get gets the entry at the given x,z coords.
// If the given x,z values are not between 0 and 31 (inclusive) this panics.
func (h *Header) Get(x, z uint8) *Entry {
	if x > 31 || z > 31 {
		panic("invalid position")
	}
	return &h[uint16(x&0x1f)|(uint16(z&0x1f)<<5)]
}

func (h *Header) clear() { *h = Header{} }

// Free frees the header and puts it into the pool.
// Callers must not use the header after calling this.
func (h *Header) Free() { headerPool.Put(h) }

// Generated returns if the entry is stored in this file.
func (e *Entry) Generated() bool { return e.Offset != 0 && e.Size != 0 }

// OffsetBytes returns the offset in bytes
func (e *Entry) OffsetBytes() int64 { return int64(e.Offset) * SectionSize }
