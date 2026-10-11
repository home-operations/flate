package cache

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/tree"
)

var maxExtractedBytes int64 = 1 << 30

type tarFS struct {
	blob  string
	data  []byte
	nodes map[string]*tarNode
}
type tarNode struct {
	fs.FileInfo
	offset   int64
	header   int64
	sparse   bool
	children []fs.DirEntry
}

func (n *tarNode) Type() fs.FileMode          { return n.Mode().Type() }
func (n *tarNode) Info() (fs.FileInfo, error) { return n, nil }

type tarFile struct {
	*tarNode
	io.Reader
	file   *os.File
	cursor int
}

func (f *tarFile) Stat() (fs.FileInfo, error) { return f.tarNode, nil }
func (f *tarFile) Close() error {
	if f.file != nil {
		return f.file.Close()
	}
	return nil
}
func (f *tarFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if !f.IsDir() {
		return nil, fs.ErrInvalid
	}
	if n > 0 && f.cursor == len(f.children) {
		return nil, io.EOF
	}
	end := len(f.children)
	if n > 0 {
		end = min(end, f.cursor+n)
	}
	entries := slices.Clone(f.children[f.cursor:end])
	f.cursor = end
	return entries, nil
}
func (t *tarFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	node := t.nodes[name]
	if node == nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	f := &tarFile{tarNode: node, Reader: io.NewSectionReader(bytes.NewReader(t.data), node.offset, node.Size())}
	if !node.IsDir() && t.blob != "" {
		var err error
		f.file, err = os.Open(t.blob)
		if err != nil {
			return nil, err
		}
		f.Reader = io.NewSectionReader(f.file, node.offset, node.Size())
		if node.sparse {
			tr := tar.NewReader(io.NewSectionReader(f.file, node.header, 1<<63-1-node.header))
			if _, err := tr.Next(); err != nil {
				_ = f.file.Close()
				return nil, err
			}
			f.Reader = tr
		}
	}
	return f, nil
}

type positionReader struct {
	io.Reader
	offset int64
}

func (r *positionReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.offset += int64(n)
	return n, err
}
func (r *positionReader) Seek(offset int64, whence int) (int64, error) {
	s, ok := r.Reader.(io.Seeker)
	if !ok {
		return r.offset, errors.ErrUnsupported
	}
	n, err := s.Seek(offset, whence)
	if err == nil {
		r.offset = n
	}
	return n, err
}

func readArchive(r io.Reader, key ArchiveKey, blob string) (fs.FS, error) {
	memory := blob == ""
	pos := &positionReader{Reader: r}
	tr := tar.NewReader(pos)
	index := &tarFS{blob: blob, nodes: map[string]*tarNode{".": {FileInfo: (&tar.Header{Name: ".", Mode: 0o750, Typeflag: tar.TypeDir}).FileInfo()}}}
	var builder *tree.Builder
	if memory {
		builder = tree.NewBuilder(nil)
	}
	var total, nextHeader int64
	for {
		header := nextHeader
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar: %w", err)
		}
		safe, err := safepath.SafeJoin(filepath.Join(string(filepath.Separator), "flate-archive"), h.Name, true)
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(filepath.ToSlash(safe), "/flate-archive/")
		if filepath.ToSlash(safe) == "/flate-archive" {
			name = "."
		}
		nextHeader = (pos.offset + h.Size + 511) / 512 * 512
		if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir {
			if _, err := io.Copy(io.Discard, tr); err != nil { //nolint:gosec // tar.Reader bounds each declared entry; regular payloads are capped below.
				return nil, err
			}
			nextHeader = (pos.offset + 511) / 512 * 512
			continue
		}
		sparse := false
		for k := range h.PAXRecords {
			sparse = sparse || strings.HasPrefix(k, "GNU.sparse.")
		}

		old := index.nodes[name]
		if old != nil && old.IsDir() != (h.Typeflag == tar.TypeDir) {
			return nil, tree.ErrConflict
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if n := index.nodes[parent]; n != nil && !n.IsDir() {
				return nil, tree.ErrConflict
			}
			if index.nodes[parent] == nil {
				index.nodes[parent] = &tarNode{FileInfo: (&tar.Header{Name: path.Base(parent), Mode: 0o750, Typeflag: tar.TypeDir}).FileInfo()}
			}
		}
		copyHeader := *h
		copyHeader.Name = path.Base(name)
		copyHeader.Mode = 0o600
		if h.Typeflag == tar.TypeDir {
			copyHeader.Mode = 0o750
		}
		copyHeader.ModTime = time.Time{}
		index.nodes[name] = &tarNode{FileInfo: copyHeader.FileInfo(), offset: pos.offset, header: header, sparse: sparse}
		if h.Typeflag == tar.TypeDir {
			continue
		}
		if h.Size > maxExtractedBytes-total {
			return nil, fmt.Errorf("tar entry %q exceeds %d-byte extract limit", h.Name, maxExtractedBytes)
		}
		total += h.Size
		if sparse && builder == nil {
			if _, err := io.Copy(io.Discard, tr); err != nil { //nolint:gosec // The expanded-byte cap covers this regular sparse entry.
				return nil, err
			}
			nextHeader = (pos.offset + 511) / 512 * 512
		}
		if builder != nil {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			nextHeader = (pos.offset + 511) / 512 * 512
			if err := builder.AddFile(name, data, 0o600); err != nil {
				return nil, err
			}
		}
	}
	index.list()
	var raw fs.FS = index
	if memory {
		raw = builder
	}
	matcher, err := sourceignore.NewFS(raw, key.Ignore, key.WithDefaults)
	if err != nil {
		return nil, err
	}
	kept := map[string]bool{".": true}
	for name, node := range index.nodes {
		if node.IsDir() || matcher.Match(filepath.FromSlash(name), false) {
			continue
		}
		kept[name] = true
		for p := path.Dir(name); p != "."; p = path.Dir(p) {
			kept[p] = true
		}
	}
	names := make([]string, 0, len(index.nodes))
	for name := range index.nodes {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if kept[name] {
			continue
		}
		delete(index.nodes, name)
		if memory {
			if err := builder.RemoveAll(name); err != nil {
				return nil, err
			}
		}
	}
	if memory {
		return builder.Build()
	}
	index.list()
	return index, nil
}
func (t *tarFS) list() {
	for _, node := range t.nodes {
		node.children = nil
	}
	for name, node := range t.nodes {
		if name != "." {
			parent := t.nodes[path.Dir(name)]
			parent.children = append(parent.children, node)
		}
	}
	for _, node := range t.nodes {
		slices.SortFunc(node.children, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	}
}

// Blob readers expose the pinned generation as an fs.FS with one content file.
type blobHandle struct {
	io.ReadCloser
	fs.FS
}
