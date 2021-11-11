package anvil

import (
	"io"
)

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
	var fd *file
	if fd, err = a.file(rg); err == nil {
		f = &File{file: fd}
	}
	return
}

func (a *Anvil) file(rg Region) (f *file, err error) {
	r, size, readonly, err := a.fs.Open(rg)
	if err != nil {
		return
	}
	return open(rg, r, readonly, size)
}

// CachedAnvil a cached version of anvil.
type CachedAnvil struct {
	*Anvil
	regions map[Region]*file
}

// File gets the anvil file for the given coords
func (a *CachedAnvil) File(rg Region) (f *File, err error) {
	var ok bool
	var fd *file
	if fd, ok = a.regions[rg]; !ok {
		if fd, err = a.file(rg); err != nil {
			return
		}
		a.regions[rg] = fd
	}

	f = &File{file: fd}

	return
}

// Read reads the chunk data for the given location
func (a *CachedAnvil) Read(c Chunk, read io.ReaderFrom) (n int64, err error) {
	var f *File
	if f, err = a.File(c.Region()); err == nil {
		n, err = f.Read(uint8(c.X&0x1f), uint8(c.Z&0x1f), read)
	}
	return
}

// Write writes the chunk data for the given location
func (a *CachedAnvil) Write(c Chunk, p []byte) (err error) {
	var f *File
	if f, err = a.File(c.Region()); err == nil {
		err = f.Write(uint8(c.X&0x1f), uint8(c.Z&0x1f), p)
	}
	return
}
