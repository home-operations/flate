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

	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/tree"
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
	explicit bool
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
		return f.file.Close() //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
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
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrInvalid}
	}
	for parent := name; len(t.denied) > 0 && parent != "."; parent = path.Dir(parent) {
		if t.denied[parent] {
			return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrPermission}
		}
	}
	if t.memory != nil && t.nodes == nil {
		return t.memory.Open(name)
	}
	node := t.nodes[name]
	if node == nil {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	f := &tarFile{tarNode: node, Reader: bytes.NewReader(nil)}
	if node.IsDir() {
		return f, nil
	}
	if t.memory != nil {
		return t.memory.Open(name)
	}
	var err error
	f.file, err = t.root.Open(blobPath(t.digest))
	if err != nil {
		return nil, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	f.Reader = io.NewSectionReader(f.file, node.offset, node.Size())
	if node.sparse {
		tr := tar.NewReader(io.NewSectionReader(f.file, node.header, 1<<63-1-node.header))
		if _, err := tr.Next(); err != nil {
			_ = f.file.Close()
			return nil, err //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
		}
		f.Reader = tr
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
	return n, err //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
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
	return n, err //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
}

func alignNext(offset int64) int64 { return (offset + tarBlockSize - 1) / tarBlockSize * tarBlockSize }

type archiveReader struct {
	position          *positionReader
	reader            *tar.Reader
	key               ArchiveKey
	index             *tarFS
	builder           *tree.Builder
	total, nextHeader int64
}

func readArchive(r io.Reader, key ArchiveKey, index *tarFS) (*tarFS, error) {
	pos := &positionReader{Reader: r}
	a := archiveReader{position: pos, reader: tar.NewReader(pos), key: key, index: index}
	if index.root == nil {
		a.builder = tree.NewBuilder(nil)
	}
	index.nodes = map[string]*tarNode{
		".": {FileInfo: (&tar.Header{Name: ".", Mode: cacheDirectoryMode, Typeflag: tar.TypeDir}).FileInfo()},
	}
	for {
		header := a.nextHeader
		h, err := a.reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar: %w", err)
		}
		if err := a.readEntry(h, header); err != nil {
			return nil, err
		}
	}
	return a.finish()
}

func (a *archiveReader) readEntry(h *tar.Header, header int64) error {
	name, err := safepath.ArchiveName(h.Name)
	if err != nil {
		return err
	}
	if a.key.Policy == extractPolicy {
		const limit = 100 << 20
		if h.Size > limit-a.total {
			return fmt.Errorf("tar entry %q exceeds %d-byte extract limit", h.Name, limit)
		}
		a.total += h.Size
	}
	a.nextHeader = alignNext(a.position.offset + h.Size)
	link := h.Typeflag == tar.TypeSymlink || h.Typeflag == tar.TypeLink
	if a.key.Policy == extractPolicy && h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir {
		return fmt.Errorf("unsupported tar entry %q type %d", h.Name, h.Typeflag)
	}
	if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir && !link {
		return a.discardBody()
	}
	old := a.index.nodes[name]
	if admitParentsErr := a.admitParents(name, h.Typeflag == tar.TypeDir, link); admitParentsErr != nil {
		return admitParentsErr
	}
	if link {
		a.index.nodes[name] = &tarNode{
			FileInfo: (&tar.Header{Name: path.Base(name), Typeflag: tar.TypeSymlink}).FileInfo(),
		}
		if a.index.denied == nil {
			a.index.denied = make(map[string]bool)
		}
		a.index.denied[name] = true
		return nil
	}
	sparse := false
	for k := range h.PAXRecords {
		sparse = sparse || strings.HasPrefix(k, "GNU.sparse.")
	}
	copyHeader := a.copyHeader(name, h, old)
	a.index.nodes[name] = &tarNode{
		FileInfo: copyHeader.FileInfo(),
		offset:   a.position.offset,
		header:   header,
		sparse:   sparse,
		explicit: h.Typeflag == tar.TypeDir,
	}
	if h.Typeflag == tar.TypeDir {
		return a.mountDirectory(name)
	}
	if sparse && a.builder == nil {
		return a.discardBody()
	}
	if a.builder == nil {
		return nil
	}
	data, err := io.ReadAll(a.reader)
	if err != nil {
		return fmt.Errorf("read tar body: %w", err)
	}
	a.nextHeader = alignNext(a.position.offset)
	return a.builder.AddFile(name, data, copyHeader.FileInfo().Mode())
}

func (a *archiveReader) discardBody() error {
	// Flux accepts source archives without an extract size limit.
	if _, err := io.Copy(
		io.Discard,
		a.reader,
	); err != nil {
		return fmt.Errorf("read tar body: %w", err)
	}
	a.nextHeader = alignNext(a.position.offset)
	return nil
}

func (a *archiveReader) admitParents(name string, directory, link bool) error {
	old := a.index.nodes[name]
	if old != nil && (old.IsDir() != directory || (old.Mode()&fs.ModeSymlink != 0) != link) {
		return tree.ErrConflict
	}
	for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
		if n := a.index.nodes[parent]; n != nil && !n.IsDir() {
			return tree.ErrConflict
		}
		if a.index.nodes[parent] != nil {
			continue
		}
		mode := int64(cacheDirectoryMode)
		if a.key.Policy == extractPolicy {
			mode = 0o755
		}
		a.index.nodes[parent] = &tarNode{
			FileInfo: (&tar.Header{Name: path.Base(parent), Mode: mode, Typeflag: tar.TypeDir}).FileInfo(),
		}
	}
	return nil
}

func (a *archiveReader) copyHeader(name string, h *tar.Header, old *tarNode) tar.Header {
	copyHeader := *h
	copyHeader.Name = path.Base(name)
	copyHeader.Mode = privateFileMode
	if h.Typeflag == tar.TypeDir {
		copyHeader.Mode = cacheDirectoryMode
	}
	if a.key.Policy == extractPolicy {
		copyHeader.Mode = h.Mode & unixPermissionMask
		if old != nil {
			copyHeader.Mode = int64(old.Mode().Perm())
		}
	}
	if a.key.Policy != extractPolicy && h.Typeflag != tar.TypeDir && h.Mode&0o111 != 0 {
		copyHeader.Mode = 0o700
	}
	copyHeader.ModTime = time.Time{}
	if a.key.Policy == fixedPolicy {
		copyHeader.Uid, copyHeader.Gid, copyHeader.Uname, copyHeader.Gname = 0, 0, "", ""
		copyHeader.AccessTime, copyHeader.ChangeTime = time.Time{}, time.Time{}
	}
	return copyHeader
}

func (a *archiveReader) mountDirectory(name string) error {
	if a.builder == nil {
		return nil
	}
	_, err := fs.Stat(a.builder, name)
	if errors.Is(err, fs.ErrNotExist) {
		empty, _ := tree.NewBuilder(nil).Build()
		return a.builder.Mount(name, empty)
	}
	if err != nil {
		return fmt.Errorf("stat tar directory: %w", err)
	}
	return nil
}

func (a *archiveReader) finish() (*tarFS, error) {
	if a.builder == nil {
		a.index.list()
		if a.index.digest != "" {
			if err := pruneArchive(a.index, a.index.nodes, a.key); err != nil {
				return nil, err
			}
			a.index.list()
		}
		return a.index, nil
	}
	if err := pruneArchive(a.builder, a.index.nodes, a.key); err != nil {
		return nil, err
	}
	memory, err := a.builder.Build()
	if err != nil {
		return nil, err
	}
	a.index.memory = memory
	a.index.list()
	return a.index, nil
}

func pruneArchive(raw fs.FS, nodes map[string]*tarNode, key ArchiveKey) error {
	if key.Policy == extractPolicy {
		return nil
	}
	var files []string
	if key.Policy == fixedPolicy {
		files = []string{}
	}
	matcher, err := sourceignore.NewFS(raw, files, key.Ignore, key.Policy == "source" || key.Policy == fixedPolicy)
	if err != nil {
		return err
	}
	kept := map[string]bool{".": true}
	for name, node := range nodes {
		retainDirectory := node.IsDir() && (key.Policy == fixedPolicy || node.explicit)
		if name == "." || !node.Mode().IsRegular() && !retainDirectory {
			continue
		}
		if matcher.Match(filepath.FromSlash(name), node.IsDir()) {
			continue
		}
		kept[name] = true
		for p := path.Dir(name); p != "."; p = path.Dir(p) {
			kept[p] = true
		}
	}
	return pruneNodes(raw, nodes, kept)
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
			return "", err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
		}
		if name == "." {
			continue
		}
		node := t.nodes[name]
		r := record{Path: name, Mode: node.Mode(), Size: node.Size()}
		if node.Mode().IsRegular() {
			data, err := fs.ReadFile(t, name)
			if err != nil {
				return "", fmt.Errorf("read cached tree content: %w", err)
			}
			r.Content = manifest.Fingerprint(data)
			if r.Content == "" {
				return "", manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("cannot hash file %q", name))
			}
		}
		records = append(records, r)
	}
	fingerprint := manifest.Fingerprint(records)
	if fingerprint == "" {
		return "", manifest.NewError(manifest.ErrInput, nil, "cannot hash tree")
	}
	return fingerprint, nil
}

func pruneNodes(raw fs.FS, nodes map[string]*tarNode, kept map[string]bool) error {
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
