package anvil

import (
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

const (
	// ErrExternal returned if the chunk is in an external file.
	// This error is only returned if the region file was opened as a single file.
	ErrExternal = errors.Error("anvil: chunk is in separate file")
	// ErrNotGenerated returned if the chunk has not been generated yet.
	ErrNotGenerated = errors.Error("anvil: chunk has not been generated")
	// ErrSize returned if the size of the anvil file is not a multiple of 4096.
	ErrSize = errors.Error("anvil: invalid file size")
	// ErrCorrupted the given file contains invalid/corrupted data
	ErrCorrupted = errors.Error("anvil: corrupted file")
	// ErrClosed the given file has already been closed
	ErrClosed = errors.Error("anvil: file closed")
)

const (
	// Entries the number of entries in a region file
	Entries = 32 * 32
	// SectionSize the size of a section
	SectionSize     = 1 << sectionShift
	sectionSizeMask = SectionSize - 1
	sectionShift    = 12
)

var fs afero.Fs = &afero.OsFs{}

// sections returns the minimum number of sections to store the given number of bytes
func sections(v uint) uint {
	return (v + sectionSizeMask) / SectionSize
}

// Chunk the position of a chunk
type Chunk struct{ X, Z int32 }

// Region the position of a region file
type Region struct{ X, Z int32 }

// Region gets the region file position for the chunk
func (c *Chunk) Region() Region { return Region{c.X >> 5, c.Z >> 5} }

// Chunk gets the chunk position for the given postion
func (r *Region) Chunk(x, z uint8) Chunk { return Chunk{r.X<<5 | int32(x), r.Z<<5 | int32(z)} }

// Anvil todo
type Anvil struct{ fs Fs }

// File gets the anvil file for the given coords
func (a *Anvil) File(rg Region) (f *File, err error) {
	r, size, readonly, err := a.fs.Open(rg)
	if err != nil {
		return
	}
	if f, err = ReadFile(rg, r, readonly, size); err == nil {
		f.anvil = a
	}
	return
}
