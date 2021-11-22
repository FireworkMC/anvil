package anvil

import (
	"fmt"
	"io"
	"sync"

	"github.com/bits-and-blooms/bitset"
	"github.com/yehan2002/errors"
	"github.com/yehan2002/fastbytes/v2"
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
		panic(fmt.Errorf("anvil/Header: Get: invalid position (%d,%d)", x, z))
	}
	return &h.entries[uint16(x&0x1f)|(uint16(z&0x1f)<<5)]
}

func (h *Header) clear() { *h.entries = [entries]Entry{}; h.used.ClearAll() }

// Set updates the entry at x,z and the given marks the
// space used by the given entry in the `used` bitset as used.
func (h *Header) Set(x, z uint8, c Entry) {
	if c.Offset < 2 || c.Offset+uint32(c.Size) > maxFileSections {
		if c.Offset == 0 && c.Size == 0 {
			h.Remove(x, z)
			return
		}
		panic("invalid offset")
	}

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
	for i := uint(0); i < uint(c.Size); i++ {
		pos := uint(c.Offset) + i

		if h.used.Test(pos) {
			panic("anvil: Header: entry overflows into used space")
		}

		h.used.Set(pos)
	}
}

// freeSpace marks the space used by the entry as unused.
// This panics if any area used by the entry is not marked as used.
func (h *Header) freeSpace(c *Entry) {
	if c.Offset == 0 || c.Size == 0 {
		return
	}

	for i := uint(0); i < uint(c.Size); i++ {
		pos := uint(c.Offset) + i
		if !h.used.Test(pos) {
			panic("anvil: Header: inconsistent usage of space")
		}
		h.used.Clear(pos)
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

// Read reads the header from the given arrays.
// `size` should contains the size and position of entries, with the least significant byte
// being the number of sections used by the entry and the rest containing the
// offset where the entry starts.
// `timestamps` should be an array of timestamps when the entries were last modified
// as the number of seconds since January 1, 1970 UTC.
// This function expects `size`, `timestamps` to be in the hosts byte order.
// `fileSections` is the max amount of sections that can be used by the entries.
// If `fileSections` is 0, `maxFileSections` is used instead.
func (h *Header) Read(size, timestamps *[entries]uint32, fileSections uint32) (err error) {
	if fileSections == 0 {
		fileSections = maxFileSections
	}

	for i := 0; i < entries; i++ {

		size, offset := size[i]&0xFF, size[i]>>8

		for p := uint32(0); p < size; p++ {
			pos := offset + p

			// check if the postion is within the file
			if pos > fileSections {
				return errors.CauseStr(ErrCorrupted, "entry is outside the file")
			}

			// check if the position overlaps with another entry
			if h.used.Test(uint(pos)) {
				return errors.CauseStr(ErrCorrupted, "entry overlaps with another entry")
			}

			h.used.Set(uint(pos))
		}

		h.entries[i] = Entry{Timestamp: int32(timestamps[i]), Size: uint8(size), Offset: offset}
	}
	return
}

// Write writes the header to the given arrays.
// See comment on `Header.Read`.
func (h *Header) Write(size, timestamps *[entries]uint32) {
	for i := 0; i < entries; i++ {
		entry := h.entries[i]
		size[i] = entry.Offset<<8 | uint32(entry.Size)
		timestamps[i] = uint32(entry.Timestamp)
	}
}

// ReadHeader reads a header from the given reader.
// The given reader must read at least 2*4096 bytes.
// If `maxSections` != 0, this returns an error if the header references more than
// `maxSections` sections.
func ReadHeader(r io.ReaderAt, maxSection uint) (h *Header, err error) {
	h = newHeader()

	if maxSection == 0 {
		h.used = bitset.New(entries)
	} else {
		h.used = bitset.New(maxSection)
	}

	// read the file header
	var size, timestamps [entries]uint32
	if err = h.readUint32Section(r, size[:], 0); err == nil {
		if err = h.readUint32Section(r, timestamps[:], sectionSize); err == nil {
			if err = h.Read(&size, &timestamps, uint32(maxSection)); err == nil {
				return h, nil
			}
		}
	}
	return nil, err
}

func newHeader() *Header { return &Header{entries: headerPool.Get().(*[entries]Entry)} }

// readUint32Section reads a 4096 byte section at the given offset into the given uint32 slice.
func (h *Header) readUint32Section(read io.ReaderAt, dst []uint32, offset int) error {
	tmp := sectionPool.Get().(*section)
	defer tmp.Free()

	if n, err := read.ReadAt(tmp[:], int64(offset)); err != nil {
		return errors.Wrap("anvil: unable to read file header", err)
	} else if n != sectionSize {
		return errors.Wrap("anvil: Incorrect number of bytes read", io.EOF)
	}

	fastbytes.BigEndian.ToU32(tmp[:], dst)
	return nil
}
