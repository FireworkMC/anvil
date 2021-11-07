package file

import "io"

// Dir TODO
type Dir struct{}

func (d *Dir) readExternal(x, z int) (io.ReadCloser, error) { return nil, ErrExternal }

func (d *Dir) writeExternal(x, z int, b *buffer) error { return ErrExternal }
