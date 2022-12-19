package anvil

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	lru "github.com/hashicorp/golang-lru/simplelru"
	"github.com/spf13/afero"
	"github.com/yehan2002/errors"
)

const (
	// ErrExternal returned if the is in an external file.
	// This error is only returned if the entry anvil file was opened as a single file.
	ErrExternal = errors.Const("anvil: entry is in separate file")
	// ErrNotExist returned if the entry does not exist.
	ErrNotExist = errors.Const("anvil: entry does not exist")
	// ErrSize returned if the size of the anvil file is not a multiple of [SectionSize].
	ErrSize = errors.Const("anvil: invalid file size")
	// ErrCorrupted the given file contains invalid/corrupted data
	ErrCorrupted = errors.Const("anvil: corrupted file")
	// ErrClosed the given file has already been closed
	ErrClosed = errors.Const("anvil: file closed")
	// ErrReadOnly the file was opened in readonly mode.
	ErrReadOnly = errors.Const("anvil: file is opened in read-only mode")
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

// Settings settings
type Settings struct {
	// Readonly if the file should be opened in readonly mode.
	// If this is set, all write operation will return [ErrReadOnly].
	// Default: false
	ReadOnly bool
	// Sync if the file should be opened for synchronous I/O.
	// Default: false
	Sync bool

	// The cache size for [Anvil].
	// If this value is -1 the cache will be disabled.
	// Default: 20
	CacheSize int

	// The formatting string to be used to generate the file name for an anvil file
	AnvilFmt string
	// The formatting string to be used to generate the file name for a chunk that is stored
	// separately from and anvil file.
	ChunkFmt string

	fs afero.Fs
}

var filesystem afero.Fs = &afero.OsFs{}

var defaultSettings = Settings{
	CacheSize: 20,
	AnvilFmt:  "r.%d.%d.mca",
	ChunkFmt:  "c.%d.%d.mcc",
	fs:        filesystem,
}

// Anvil a anvil file cache.
type Anvil struct {
	inUse map[pos]*file

	lru *lru.LRU

	settings Settings

	mux sync.RWMutex
}

// Read reads the chunk data for the given location.
func (a *Anvil) Read(entryX, entryZ int32, read io.ReaderFrom) (n int64, err error) {
	var f *file
	if f, err = a.get(entryX>>5, entryZ>>5); err == nil {
		defer a.free(f)
		n, err = f.Read(uint8(entryX&0x1f), uint8(entryZ&0x1f), read)
	}
	return
}

// Write writes the chunk data for the given location
func (a *Anvil) Write(entryX, entryZ int32, p []byte) (err error) {
	var f *file
	if f, err = a.get(entryX>>5, entryZ>>5); err == nil {
		defer a.free(f)
		err = f.Write(uint8(entryX&0x1f), uint8(entryZ&0x1f), p)
	}
	return
}

// Info gets information stored in the anvil header for the given entry.
func (a *Anvil) Info(entryX, entryZ int32) (entry Entry, exists bool, err error) {
	var f *file
	if f, err = a.get(entryX>>5, entryZ>>5); err == nil {
		defer a.free(f)
		entry, exists = f.Info(uint8(entryX&0x1f), uint8(entryZ&0x1f))
	}
	return
}

// File opens the anvil file at rgX, rgZ.
// Callers must close the returned file for it to be removed from the cache.
func (a *Anvil) File(rgX, rgZ int32) (f File, err error) {
	c, err := a.get(rgX, rgZ)
	if err != nil {
		return nil, err
	}
	return &cachedFile{file: c}, nil
}

// get gets the anvil get for the given coords
func (a *Anvil) get(rgX, rgZ int32) (f *file, err error) {
	rg := pos{rgX, rgZ}
	a.mux.RLock()
	f, ok := a.getFile(rg)
	a.mux.RUnlock()

	if !ok {
		a.mux.Lock()
		defer a.mux.Unlock()
		// check if the file was opened while we were waiting for the mux
		if f, ok = a.getFile(rg); !ok {

			if a.lru != nil {
				// check if the file is in the lru cache
				if v, ok := a.lru.Get(rg); ok {
					a.lru.Remove(rg)
					f = v.(*file)
				}
			}

			// file wasn't in the cache. read file from the disk
			if f == nil {
				var r reader
				var size int64
				filename := fmt.Sprintf(a.settings.AnvilFmt, rg.x, rg.z)
				if r, size, err = openFile(filename, a.settings); err == nil {
					f, err = newAnvil(rg.x, rg.z, r, size, a.settings)
					f.cache = a
				}
			}

			if err == nil {
				f.useCount.Add(1)
				a.inUse[rg] = f
			}
		}
	}

	return
}

func (a *Anvil) free(f *file) (err error) {
	a.mux.RLock()
	newCount := f.useCount.Add(-1)
	a.mux.RUnlock()

	if newCount == 0 {
		a.mux.Lock()
		defer a.mux.Unlock()
		if newCount = f.useCount.Load(); newCount == 0 {

			if a.lru == nil {
				// cache is disabled. close the file
				delete(a.inUse, f.pos)
				return f.Close()
			}

			// evict the oldest file from the lru if adding a new element will cause a element to be evicted
			// We do this to insure the file gets closed properly and to free all associated resources.
			// We cannot use EvictCallback since there is no way to handle error that occur while closing the file.
			if a.lru.Len() == a.settings.CacheSize {
				if _, old, ok := a.lru.RemoveOldest(); ok {
					if err = old.(*file).Close(); err != nil {
						err = errors.Wrap("anvil.Cache: error occurred while evicting file", err)
					}
				}
			}

			evicted := a.lru.Add(f.pos, f)
			if evicted {
				// This should never happen since we manually evicted the oldest element
				panic("anvil.Cache: File was incorrectly evicted")
			}

			delete(a.inUse, f.pos)
		}
	}
	return
}

func (a *Anvil) getFile(rg pos) (f *file, ok bool) {
	f, ok = a.inUse[rg]
	if ok {
		f.useCount.Add(1)
	}
	return
}

// Open opens the given directory.
func Open(path string, opt ...Settings) (c *Anvil, err error) {
	if path, err = filepath.Abs(path); err == nil {
		var info os.FileInfo
		if info, err = filesystem.Stat(path); err == nil {
			if !info.IsDir() {
				return nil, errors.New("anvil: Open: " + path + " is not a directory")
			}
			return OpenFs(afero.NewBasePathFs(filesystem, path), opt...)
		}
	}
	return
}

// OpenFs opens the given directory.
func OpenFs(fs afero.Fs, opt ...Settings) (c *Anvil, err error) {
	settings := getSettings(opt, fs)

	cache := Anvil{inUse: map[pos]*file{}, settings: settings}

	if settings.CacheSize > 0 {
		if cache.lru, err = lru.NewLRU(settings.CacheSize, nil); err != nil {
			return nil, err
		}
	}

	return &cache, nil
}

func getSettings(s []Settings, fs afero.Fs) Settings {
	var settings = defaultSettings

	if len(s) == 1 {
		settings = s[0]

		if settings.CacheSize == 0 {
			settings.CacheSize = defaultSettings.CacheSize
		}

		if settings.AnvilFmt == "" {
			settings.AnvilFmt = defaultSettings.AnvilFmt
		}

		if settings.ChunkFmt == "" {
			settings.ChunkFmt = defaultSettings.ChunkFmt
		}
	}

	settings.fs = fs

	return settings
}
