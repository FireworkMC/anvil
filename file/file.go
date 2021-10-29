package file

import (
	"os"

	"github.com/natefinch/atomic"
)

const regionFileChunks = 32 * 32

// File is a single anvil region file.
type File struct {
	r *os.File
}

type header struct {
	chunks [regionFileChunks]chunkEntry
}

type chunkEntry struct {
	size      uint8
	location  uint32
	timestamp uint32
}

func _() {
	_ = atomic.WriteFile
}
