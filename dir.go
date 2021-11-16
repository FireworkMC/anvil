package anvil

import (
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/spf13/afero"
)

// Open opens the given directory.
func Open(path string, readonly bool, size int) (c *Cache, err error) {
	if _, err = fs.Stat(path); err == nil {
		return OpenFs(NewFs(afero.NewBasePathFs(fs, path)), readonly, size)
	}
	return
}

// OpenFs opens the given directory.
func OpenFs(fs *Fs, readonly bool, size int) (c *Cache, err error) {
	cache := Cache{fs: fs, inUse: map[Region]*cachedAnvil{}, lruSize: size}
	if cache.lru, err = simplelru.NewLRU(size, nil); err == nil {
		return &cache, nil
	}
	return
}
