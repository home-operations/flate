package cache

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"io/fs"

	"github.com/home-operations/flate/pkg/manifest"
)

// NameBlob gives an acquired Blob generation a filename and shaping identity.
func NameBlob(name string, generation fs.FS) fs.FS {
	input := generation.(fileFS)
	input.name = name
	input.identity = manifest.Fingerprint([2]string{input.identity, name})
	return input
}

// FileName returns the acquired payload path, or empty for a directory view.
func FileName(input fs.FS) string {
	file, _ := input.(fileFS)
	return file.name
}

type fileFS struct {
	backingPath string
	name        string
	identity    string
	size        int64
	open        func() (io.ReadCloser, error)
}

func (f fileFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	if name == "." {
		node := &tarNode{FileInfo: (&tar.Header{Name: f.name, Mode: 0o600, Size: f.size}).FileInfo()}
		root := &tarNode{FileInfo: (&tar.Header{Name: ".", Mode: 0o750, Typeflag: tar.TypeDir}).FileInfo(), children: []fs.DirEntry{node}}
		return &tarFile{tarNode: root, Reader: bytes.NewReader(nil)}, nil
	}
	if name != f.name {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	r, err := f.open()
	if err != nil {
		return nil, &CacheError{err}
	}
	return &blobFile{ReadCloser: r, FileInfo: (&tar.Header{Name: f.name, Mode: 0o600, Size: f.size}).FileInfo()}, nil
}

func (f fileFS) ReadFile(name string) ([]byte, error) {
	var r io.ReadCloser
	var err error
	if name == f.name && fs.ValidPath(name) {
		r, err = f.open()
	} else {
		r, err = f.Open(name)
	}
	if err != nil {
		return nil, &CacheError{err}
	}
	var data bytes.Buffer
	data.Grow(int(f.size) + bytes.MinRead)
	_, err = data.ReadFrom(r)
	return data.Bytes(), errors.Join(err, r.Close())
}

type blobFile struct {
	io.ReadCloser
	fs.FileInfo
}

func (f *blobFile) Stat() (fs.FileInfo, error) { return f.FileInfo, nil }

// AcquisitionIdentity reports the held generation and shaping of cache-owned filesystems.
// Anonymous inputs and wrappers have no acquisition authority.
func AcquisitionIdentity(input any) string {
	switch input := input.(type) {
	case *tarFS:
		return input.identity
	case fileFS:
		return input.identity
	default:
		return ""
	}
}
