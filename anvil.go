package anvil

import (
	"io"

	"github.com/bluele/gcache"
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
type Anvil struct {
	fs      Fs
	regions map[Region]*File
}

// GetFile gets the anvil file for the given coords
func (a *Anvil) GetFile(rg Region) (f *File, err error) {
	var ok bool

	if f, ok = a.regions[rg]; ok {
		r, size, readonly, err := a.fs.Open(rg)
		if err != nil {
			return nil, err
		}
		if f, err = open(rg, r, readonly, size); err == nil {
			a.regions[rg] = f
		}
	}
	return
}

// Read reads the chunk data for the given location
func (a *Anvil) Read(c Chunk, read io.ReaderFrom) (n int64, err error) {
	var f *File
	if f, err = a.GetFile(c.Region()); err == nil {
		n, err = f.Read(uint8(c.X&0x1f), uint8(c.Z&0x1f), read)
	}
	return
}

// Write writes the chunk data for the given location
func (a *Anvil) Write(c Chunk, p []byte) (err error) {
	var f *File
	if f, err = a.GetFile(c.Region()); err == nil {
		err = f.Write(uint8(c.X&0x1f), uint8(c.Z&0x1f), p)
	}
	return
}

func (a *Anvil) cacheLoader(key interface{}) (value interface{}, err error) {

	return
}

func newArcCache(size int, evict gcache.EvictedFunc) *gcache.ARC {
	return gcache.New(size).ARC().EvictedFunc(evict).LoaderFunc(nil).Build().(*gcache.ARC)
}
