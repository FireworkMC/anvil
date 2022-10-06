package anvil

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/yehan2002/errors"
	"github.com/yehan2002/fastbytes/v2"
)

var headerPool = sync.Pool{New: func() interface{} { return &[Entries]Entry{} }}

// pos the position of a anvil file.
// Normally the x and z values are the x and z values in the filename of the anvil file.
type pos struct{ x, z int32 }

// External gets the x and z for an entry that is stored in a separate file.
func (r *pos) External(x, z uint8) (int32, int32) { return r.x<<5 | int32(x), r.z<<5 | int32(z) }

// sections returns the minimum number of sections to store the given number of bytes
func sections(v uint) uint { return (v + SectionSize - 1) / SectionSize }

// Entry an entry in the anvil file
type Entry struct {
	size      uint8
	offset    uint32
	timestamp int32
}

// Exists returns if the entry is stored in this file.
func (e *Entry) Exists() bool { return e.offset != 0 && e.size != 0 }

// Modified returns when the entry was last modified.
func (e *Entry) Modified() time.Time { return time.Unix(int64(e.timestamp), 0) }

// CompressedSize the number of sections used by this entry (in sections).
// To get the size in bytes, multiply this value by [SectionSize].
// If this is zero the data does not exist in this file.
// If the entry is stored in an external file, this will return 1.
func (e *Entry) CompressedSize() int64 { return int64(e.size) }

// Offset is the offset of the entry in the anvil file (in sections).
// The maximum offset is (2<<24)-1 sections.
// To get the size in bytes, multiply this value by [SectionSize].
// If this is zero the data does not exist in this file.
func (e *Entry) Offset() int64 { return int64(e.offset) }

// Header the header of the anvil file.
type Header struct {
	entries *[Entries]Entry
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

func (h *Header) clear() { *h.entries = [Entries]Entry{}; h.used.ClearAll() }

// Set updates the entry at x,z and the given marks the
// space used by the given entry in the `used` bitset as used.
func (h *Header) Set(x, z uint8, c Entry) error {
	if c.offset < 2 || c.offset+uint32(c.size) > MaxFileSections {
		if c.offset == 0 && c.size == 0 {
			return h.Remove(x, z)
		}
		panic(fmt.Errorf("anvil/Header: invalid position (%d,%d)", x, z))
	}

	old := h.Get(x, z)
	if old.Exists() {
		h.freeSpace(old)
	}

	if err := h.markSpace(c); err != nil {
		return err
	}

	*old = c
	return nil
}

// Remove removes the given entry from the header and marks the space used
// by the given entry in the `used` bitset as unused.
func (h *Header) Remove(x, z uint8) error {
	e := h.Get(x, z)

	if err := h.freeSpace(e); err != nil {
		return err
	}

	*e = Entry{}

	return nil
}

// markSpace marks the space used by the given entry as used.
// This panics if the entry overflows into used an area.
func (h *Header) markSpace(c Entry) error {
	for i := uint(0); i < uint(c.size); i++ {
		pos := uint(c.offset) + i

		if h.used.Test(pos) {
			return fmt.Errorf("anvil: Header: entry overflows into used space")
		}

		h.used.Set(pos)
	}
	return nil
}

// freeSpace marks the space used by the entry as unused.
// This panics if any area used by the entry is not marked as used.
func (h *Header) freeSpace(c *Entry) error {
	if c.offset == 0 || c.size == 0 {
		return nil
	}

	for i := uint(0); i < uint(c.size); i++ {
		pos := uint(c.offset) + i
		if !h.used.Test(pos) {
			return fmt.Errorf("anvil: Header: inconsistent usage of space")
		}
		h.used.Clear(pos)
	}
	return nil
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

// load reads the header from the given arrays.
// See comment on [Header.LoadHeader].
func (h *Header) load(size, timestamps *[Entries]uint32, fileSections uint32) (err error) {
	if fileSections == 0 {
		fileSections = MaxFileSections
	}

	for i := 0; i < Entries; i++ {

		size, offset := size[i]&0xFF, size[i]>>8

		for p := uint32(0); p < size; p++ {
			pos := offset + p

			// check if the position is within the file
			if pos > fileSections {
				return errors.CauseStr(ErrCorrupted, "entry is outside the file")
			}

			// check if the position overlaps with another entry
			if h.used.Test(uint(pos)) {
				return errors.CauseStr(ErrCorrupted, "entry overlaps with another entry")
			}

			h.used.Set(uint(pos))
		}

		h.entries[i] = Entry{timestamp: int32(timestamps[i]), size: uint8(size), offset: offset}
	}
	return
}

// Write writes the header to the given arrays.
func (h *Header) Write(size, timestamps *[Entries]uint32) {
	for i := 0; i < Entries; i++ {
		entry := h.entries[i]
		size[i] = entry.offset<<8 | uint32(entry.size)
		timestamps[i] = uint32(entry.timestamp)
	}
}

// ReadHeader reads a header from the given reader.
// The given reader must read at least 2*4096 bytes.
// If `maxSections` != 0, this returns an error if the header references more than
// `maxSections` sections.
func ReadHeader(r io.ReaderAt, maxSection uint) (h *Header, err error) {
	// read the file header
	var size, timestamps [Entries]uint32
	if err = readUint32Section(r, size[:], 0); err == nil {
		if err = readUint32Section(r, timestamps[:], SectionSize); err == nil {
			return LoadHeader(&size, &timestamps, maxSection)
		}
	}
	return nil, err
}

// LoadHeader reads the header from the given arrays.
// `size` should contains the size and position of entries, with the least significant byte
// being the number of sections used by the entry and the rest containing the
// offset where the entry starts.
// `timestamps` should be an array of timestamps when the entries were last modified
// as the number of seconds since January 1, 1970 UTC.
// This function expects `size`, `timestamps` to be in the hosts byte order.
// `fileSections` is the max amount of sections that can be used by the entries.
// If `fileSections` is 0, `maxFileSections` is used instead.
func LoadHeader(size, timestamps *[Entries]uint32, fileSections uint) (h *Header, err error) {
	h = newHeader()

	if fileSections == 0 {
		h.used = bitset.New(Entries)
	} else {
		h.used = bitset.New(fileSections)
	}

	if err = h.load(size, timestamps, uint32(fileSections)); err == nil {
		return h, nil
	}
	return nil, err
}

func newHeader() *Header { return &Header{entries: headerPool.Get().(*[Entries]Entry)} }

// readUint32Section reads a 4096 byte section at the given offset into the given uint32 slice.
func readUint32Section(read io.ReaderAt, dst []uint32, offset int) error {
	tmp := sectionPool.Get().(*section)
	defer tmp.Free()

	if n, err := read.ReadAt(tmp[:], int64(offset)); err != nil {
		return errors.Wrap("anvil: unable to read file header", err)
	} else if n != SectionSize {
		return errors.Wrap("anvil: Incorrect number of bytes read", io.EOF)
	}

	fastbytes.BigEndian.ToU32(tmp[:], dst)
	return nil
}
