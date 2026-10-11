package source

import (
	"io"
	"io/fs"
)

// FileFS names the content file exposed by a cache Blob reader. Close the initial
// reader after construction; this filesystem opens independent pinned readers.
func FileFS(name string, r io.ReadCloser) fs.FS { return fileFS{name: name, input: r.(fs.FS)} }

type fileFS struct {
	name  string
	input fs.FS
}

func (f fileFS) Open(name string) (fs.File, error) {
	path := "content"
	if name == "." {
		path = "."
	} else if name != f.name {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	file, err := f.input.Open(path)
	if err != nil {
		return nil, err
	}
	return &namedFile{File: file, name: name, contentName: f.name}, nil
}

type fileEntry struct {
	fs.FileInfo
	name string
}

func (e fileEntry) Name() string               { return e.name }
func (e fileEntry) Type() fs.FileMode          { return e.Mode().Type() }
func (e fileEntry) Info() (fs.FileInfo, error) { return e, nil }

type namedFile struct {
	fs.File
	name        string
	contentName string
}

func (f *namedFile) Stat() (fs.FileInfo, error) {
	info, err := f.File.Stat()
	return fileEntry{name: f.name, FileInfo: info}, err
}

func (f *namedFile) ReadDir(n int) ([]fs.DirEntry, error) {
	dir, ok := f.File.(fs.ReadDirFile)
	if !ok {
		return nil, fs.ErrInvalid
	}
	entries, err := dir.ReadDir(n)
	if err != nil {
		return nil, err
	}
	for i, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		entries[i] = fileEntry{name: f.contentName, FileInfo: info}
	}
	return entries, nil
}
