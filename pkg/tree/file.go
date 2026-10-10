package tree

import (
	"bytes"
	"io"
	"io/fs"
	"slices"
)

type file struct {
	node   *node
	name   string
	reader bytes.Reader
	offset int
	closed bool
}

func (f *file) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, &fs.PathError{Op: "stat", Path: f.name, Err: fs.ErrClosed}
	}
	return f.node, nil
}

func (f *file) Read(p []byte) (int, error) {
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if f.node.IsDir() {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrInvalid}
	}
	return f.reader.Read(p)
}

func (f *file) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.closed {
		return nil, &fs.PathError{Op: "readdir", Path: f.name, Err: fs.ErrClosed}
	}
	if !f.node.IsDir() {
		return nil, &fs.PathError{Op: "readdir", Path: f.name, Err: fs.ErrInvalid}
	}
	entries := f.node.children[f.offset:]
	if n > 0 && len(entries) == 0 {
		return nil, io.EOF
	}
	if n > 0 {
		entries = entries[:min(n, len(entries))]
	}
	f.offset += len(entries)
	return slices.Clone(entries), nil
}

func (f *file) Close() error {
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	return nil
}
