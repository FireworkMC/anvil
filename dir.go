package anvil

import (
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/spf13/afero"
)

// Dir handles an entire directory containing anvil files.
// This makes no attempt to synchronize file access or cache reads.
// Most user should use `Cache` instead.
type Dir struct {
	fs       *Fs
	readonly bool
}

// File gets the anvil file for the given coords
func (a *Dir) File(rg Region) (f *Anvil, err error) {
	r, size, err := a.fs.open(rg.x, rg.z)
	if err != nil {
		return
	}
	if f, err = NewAnvil(rg, r, a.readonly, size); err == nil {
		f.dir = a
	}
	return
}

// Open opens the given directory.
func Open(path string, readonly bool) (*Dir, error) {
	if _, err := fs.Stat(path); err != nil {
		return nil, err
	}
	dir := &Fs{afero.NewBasePathFs(fs, path)}
	return &Dir{fs: dir, readonly: readonly}, nil
}

// Cached returns a cached version of `Dir`.
func Cached(d *Dir, size int) (*Cache, error) {
	lru, err := simplelru.NewLRU(size, nil)
	if err == nil {
		return &Cache{dir: d, inUse: map[Region]*cachedAnvil{}, lru: lru, lruSize: size}, nil
	}
	return nil, err
}
