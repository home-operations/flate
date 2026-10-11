package cache

import (
	"archive/tar"
	"bytes"
	"io"
	"io/fs"
)

// FileFS names a Blob generation. The reader MUST have come from Blob;
// close it after construction. Each filesystem Open pins an independent reader.
func FileFS(name string, r io.ReadCloser) fs.FS {
	input := r.(*blobHandle).input
	input.name = name
	return input
}

type blobHandle struct {
	io.ReadCloser
	input fileFS
}

type fileFS struct {
	name string
	size int64
	open func() (io.ReadCloser, error)
}

func (f fileFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	node := &tarNode{FileInfo: (&tar.Header{Name: f.name, Mode: 0o600, Size: f.size}).FileInfo()}
	if name == "." {
		root := &tarNode{FileInfo: (&tar.Header{Name: ".", Mode: 0o750, Typeflag: tar.TypeDir}).FileInfo(), children: []fs.DirEntry{node}}
		return &tarFile{tarNode: root, Reader: bytes.NewReader(nil)}, nil
	}
	if name != f.name {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	r, err := f.open()
	if err != nil {
		return nil, err
	}
	return &blobFile{ReadCloser: r, tarNode: node}, nil
}

type blobFile struct {
	io.ReadCloser
	*tarNode
}

func (f *blobFile) Stat() (fs.FileInfo, error) { return f.tarNode, nil }
