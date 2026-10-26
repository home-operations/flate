package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/tree"
	"github.com/opencontainers/go-digest"
)

type tarFS struct {
	root     *os.Root
	memory   *tree.Tree
	identity string
	digest   digest.Digest
	nodes    map[string]*tarNode
	denied   map[string]bool
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

func (t *tarFS) identify(content digest.Digest, key ArchiveKey) {
	t.identity = manifest.Fingerprint(struct {
		Digest  digest.Digest
		Ignore  string
		Policy  string
		Version int
	}{content, key.Ignore, key.Policy, 1})
}

func (t *tarFS) Open(name string) (fs.File, error) {
	if (t.memory == nil || t.nodes != nil || len(t.denied) != 0) && !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	for parent := name; len(t.denied) > 0 && parent != "."; parent = path.Dir(parent) {
		if t.denied[parent] {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
		}
	}
	if t.memory != nil && t.nodes == nil {
		return t.memory.Open(name)
	}
	node := t.nodes[name]
	if node == nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	f := &tarFile{tarNode: node, Reader: bytes.NewReader(nil)}
	if !node.IsDir() {
		if t.memory != nil {
			return t.memory.Open(name)
		}
		var err error
		f.file, err = t.root.Open(blobPath(t.digest))
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

func alignNext(offset int64) int64 { return (offset + 511) / 512 * 512 }

func readArchive(r io.Reader, key ArchiveKey, index *tarFS) (*tarFS, error) {
	pos := &positionReader{Reader: r}
	tr := tar.NewReader(pos)
	var builder *tree.Builder
	if index.root == nil {
		builder = tree.NewBuilder(nil)
	}
	index.nodes = map[string]*tarNode{".": {FileInfo: (&tar.Header{Name: ".", Mode: 0o750, Typeflag: tar.TypeDir}).FileInfo()}}
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
		name, err := safepath.ArchiveName(h.Name)
		if err != nil {
			return nil, err
		}
		if key.Policy == "extract" {
			const limit = 100 << 20
			if h.Size > limit-total {
				return nil, fmt.Errorf("tar entry %q exceeds %d-byte extract limit", h.Name, limit)
			}
			total += h.Size
		}
		nextHeader = alignNext(pos.offset + h.Size)
		link := h.Typeflag == tar.TypeSymlink || h.Typeflag == tar.TypeLink
		if key.Policy == "extract" && h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir {
			return nil, fmt.Errorf("unsupported tar entry %q type %d", h.Name, h.Typeflag)
		}
		if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir && !link {
			if _, err := io.Copy(io.Discard, tr); err != nil { //nolint:gosec // Flux source archives permit uncapped traversal; member paths are validated.
				return nil, err
			}
			nextHeader = alignNext(pos.offset)
			continue
		}
		sparse := false
		for k := range h.PAXRecords {
			sparse = sparse || strings.HasPrefix(k, "GNU.sparse.")
		}

		old := index.nodes[name]
		if old != nil && (old.IsDir() != (h.Typeflag == tar.TypeDir) || (old.Mode()&fs.ModeSymlink != 0) != link) {
			return nil, tree.ErrConflict
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if n := index.nodes[parent]; n != nil && !n.IsDir() {
				return nil, tree.ErrConflict
			}
			if index.nodes[parent] == nil {
				mode := int64(0o750)
				if key.Policy == "extract" {
					mode = 0o755
				}
				index.nodes[parent] = &tarNode{FileInfo: (&tar.Header{Name: path.Base(parent), Mode: mode, Typeflag: tar.TypeDir}).FileInfo()}
			}
		}
		if link {
			index.nodes[name] = &tarNode{FileInfo: (&tar.Header{Name: path.Base(name), Typeflag: tar.TypeSymlink}).FileInfo()}
			if index.denied == nil {
				index.denied = make(map[string]bool)
			}
			index.denied[name] = true
			continue
		}
		copyHeader := *h
		copyHeader.Name = path.Base(name)
		copyHeader.Mode = 0o600
		if h.Typeflag == tar.TypeDir {
			copyHeader.Mode = 0o750
		}
		if key.Policy == "extract" {
			copyHeader.Mode = h.Mode & 0o777
			if old != nil {
				copyHeader.Mode = int64(old.Mode().Perm())
			}
		}
		if key.Policy == "fixed" && h.Typeflag != tar.TypeDir && h.Mode&0o111 != 0 {
			copyHeader.Mode = 0o700
		}
		copyHeader.ModTime = time.Time{}
		if key.Policy == "fixed" {
			copyHeader.Uid, copyHeader.Gid, copyHeader.Uname, copyHeader.Gname = 0, 0, "", ""
			copyHeader.AccessTime, copyHeader.ChangeTime = time.Time{}, time.Time{}
		}
		index.nodes[name] = &tarNode{FileInfo: copyHeader.FileInfo(), offset: pos.offset, header: header, sparse: sparse}
		if h.Typeflag == tar.TypeDir {
			if builder != nil && (key.Policy == "fixed" || key.Policy == "extract") {
				if _, err := fs.Stat(builder, name); errors.Is(err, fs.ErrNotExist) {
					empty, _ := tree.NewBuilder(nil).Build()
					if err := builder.Mount(name, empty); err != nil {
						return nil, err
					}
				} else if err != nil {
					return nil, err
				}
			}
			continue
		}
		if sparse && builder == nil {
			if _, err := io.Copy(io.Discard, tr); err != nil { //nolint:gosec // Flux source archives permit sparse expansion; reads stay confined to this member.
				return nil, err
			}
			nextHeader = alignNext(pos.offset)
		}
		if builder != nil {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			nextHeader = alignNext(pos.offset)
			if err := builder.AddFile(name, data, copyHeader.FileInfo().Mode()); err != nil {
				return nil, err
			}
		}
	}
	if builder == nil {
		index.list()
		if index.digest != "" {
			if err := pruneArchive(index, index.nodes, key); err != nil {
				return nil, err
			}
			index.list()
		}
		return index, nil
	}
	if err := pruneArchive(builder, index.nodes, key); err != nil {
		return nil, err
	}
	memory, err := builder.Build()
	if err != nil {
		return nil, err
	}
	index.memory = memory
	if key.Policy != "fixed" {
		index.nodes = nil
	} else {
		index.list()
	}
	return index, nil
}

func pruneArchive(raw fs.FS, nodes map[string]*tarNode, key ArchiveKey) error {
	if key.Policy == "extract" {
		return nil
	}
	var files []string
	if key.Policy == "fixed" {
		files = []string{}
	}
	matcher, err := sourceignore.NewFS(raw, files, key.Ignore, key.Policy == "source" || key.Policy == "fixed")
	if err != nil {
		return err
	}
	kept := map[string]bool{".": true}
	for name, node := range nodes {
		if name == "." || !node.Mode().IsRegular() && (key.Policy != "fixed" || !node.IsDir()) || matcher.Match(filepath.FromSlash(name), node.IsDir()) {
			continue
		}
		kept[name] = true
		for p := path.Dir(name); p != "."; p = path.Dir(p) {
			kept[p] = true
		}
	}
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if kept[name] {
			continue
		}
		delete(nodes, name)
		if builder, ok := raw.(*tree.Builder); ok {
			if err := builder.RemoveAll(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *tarFS) list() {
	for _, node := range t.nodes {
		node.children = nil
	}
	for name, node := range t.nodes {
		if name != "." && node.Mode()&fs.ModeSymlink == 0 {
			parent := t.nodes[path.Dir(name)]
			parent.children = append(parent.children, node)
		}
	}
	for _, node := range t.nodes {
		slices.SortFunc(node.children, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	}
}

func (t *tarFS) treeIdentity(ctx context.Context) (string, error) {
	type record struct {
		Path    string
		Mode    fs.FileMode
		Size    int64
		Content string
	}
	records := make([]record, 0, len(t.nodes))
	for _, name := range slices.Sorted(maps.Keys(t.nodes)) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if name == "." {
			continue
		}
		node := t.nodes[name]
		r := record{Path: name, Mode: node.Mode(), Size: node.Size()}
		if node.Mode().IsRegular() {
			data, err := fs.ReadFile(t, name)
			if err != nil {
				return "", err
			}
			r.Content = manifest.Fingerprint(data)
			if r.Content == "" {
				return "", fmt.Errorf("hash file %q: %w", name, manifest.ErrInput)
			}
		}
		records = append(records, r)
	}
	fingerprint := manifest.Fingerprint(records)
	if fingerprint == "" {
		return "", fmt.Errorf("hash tree: %w", manifest.ErrInput)
	}
	return fingerprint, nil
}
