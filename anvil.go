package anvil

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

const (
	// ErrExternal returned if the chunk is in an external file.
	// This error is only returned if the anvil file was opened as a single file.
	ErrExternal = errors.Const("anvil: chunk is in separate file")
	// ErrNotGenerated returned if the chunk has not been generated yet.
	ErrNotGenerated = errors.Const("anvil: chunk has not been generated")
	// ErrSize returned if the size of the anvil file is not a multiple of 4096.
	ErrSize = errors.Const("anvil: invalid file size")
	// ErrCorrupted the given file contains invalid/corrupted data
	ErrCorrupted = errors.Const("anvil: corrupted file")
	// ErrClosed the given file has already been closed
	ErrClosed = errors.Const("anvil: file closed")
)

const (
	sectionSizeMask = SectionSize - 1
	sectionShift    = 12
	entryHeaderSize = 5

	// Entries the number of Entries in a anvil file
	Entries = 32 * 32
	// SectionSize the size of a section
	SectionSize = 1 << sectionShift
	// MaxFileSections the maximum number of sections a file can contain
	MaxFileSections = 255 * Entries
)

// Anvil a anvil file cache.
type Anvil struct {
	fs    *Fs
	inUse map[Pos]*cachedFile

	lru     *simplelru.LRU
	lruSize int

	readonly bool

	mux sync.RWMutex
}

// Read reads the chunk data for the given location
func (a *Anvil) Read(entryX, entryZ int32, read io.ReaderFrom) (n int64, err error) {
	var f *cachedFile
	if f, err = a.get(entryX>>5, entryZ>>5); err == nil {
		defer a.free(f)
		n, err = f.Read(uint8(entryX&0x1f), uint8(entryZ&0x1f), read)
	}
	return
}

// Write writes the chunk data for the given location
func (a *Anvil) Write(entryX, entryZ int32, p []byte) (err error) {
	var f *cachedFile
	if f, err = a.get(entryX>>5, entryZ>>5); err == nil {
		defer a.free(f)
		err = f.Write(uint8(entryX&0x1f), uint8(entryZ&0x1f), p)
	}
	return
}

// File opens the anvil file at rgX, rgZ.
// Callers must close the returned file for it to be removed from the cache.
func (a *Anvil) File(rgX, rgZ int32) (f *CachedFile, err error) {
	c, err := a.get(rgX, rgZ)
	if err != nil {
		return nil, err
	}
	return &CachedFile{cachedFile: c}, nil
}

// get gets the anvil get for the given coords
func (a *Anvil) get(rgX, rgZ int32) (f *cachedFile, err error) {
	rg := Pos{rgX, rgZ}
	a.mux.RLock()
	f, ok := a.getFile(rg)
	a.mux.RUnlock()

	if !ok {
		a.mux.Lock()
		defer a.mux.Unlock()
		// check if the file was opened while we were waiting for the mux
		if f, ok = a.getFile(rg); !ok {
			var file *File
			if v, ok := a.lru.Get(rg); ok { // check if the file is in the lru cache
				a.lru.Remove(rg)
				file = v.(*File)
			} else { // read file from the disk
				var r reader
				var size int64
				if r, size, err = a.fs.open(rg.x, rg.z, a.readonly); err == nil {
					file, err = NewAnvil(rg, a.fs, r, a.readonly, size)
				}
			}

			if err == nil {
				f = &cachedFile{File: file, cache: a}
				f.useCount.Add(1)
				a.inUse[rg] = f
			}
		}
	}

	return
}

func (a *Anvil) free(f *cachedFile) (err error) {
	a.mux.RLock()
	newCount := f.useCount.Add(-1)
	a.mux.RUnlock()

	if newCount == 0 {
		a.mux.Lock()
		defer a.mux.Unlock()
		if newCount = f.useCount.Load(); newCount == 0 {

			// evict the oldest file from the lru if adding a new element will cause a element to be evicted
			// We do this to insure the file gets closed properly and to free all associated resources
			if a.lru.Len() == a.lruSize {
				if _, old, ok := a.lru.RemoveOldest(); ok {
					if err = old.(*File).Close(); err != nil {
						err = errors.Wrap("anvil.Cache: error occurred while evicting file", err)
					}
				}
			}

			evicted := a.lru.Add(f.pos, f.File)
			if evicted {
				// This should never happen since we manually evicted the oldest element
				panic("anvil.Cache: File was incorrectly evicted")
			}

			delete(a.inUse, f.pos)
		}
	}
	return
}

func (a *Anvil) getFile(rg Pos) (f *cachedFile, ok bool) {
	f, ok = a.inUse[rg]
	if ok {
		f.useCount.Add(1)
	}
	return
}

// Open opens the given directory.
func Open(path string, readonly bool, cacheSize int) (c *Anvil, err error) {
	if path, err = filepath.Abs(path); err == nil {
		var info os.FileInfo
		if info, err = fs.Stat(path); err == nil {
			if !info.IsDir() {
				return nil, errors.New("anvil: Open: " + path + " is not a directory")
			}
			return OpenFs(NewFs(afero.NewBasePathFs(fs, path)), readonly, cacheSize)
		}
	}
	return
}

// OpenFs opens the given directory.
func OpenFs(fs *Fs, readonly bool, cacheSize int) (c *Anvil, err error) {
	cache := Anvil{fs: fs, inUse: map[Pos]*cachedFile{}, lruSize: cacheSize, readonly: readonly}
	if cache.lru, err = simplelru.NewLRU(cacheSize, nil); err == nil {
		return &cache, nil
	}
	return
}
