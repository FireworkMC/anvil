package anvil

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
	return open(rg, r, readonly, size)
}
