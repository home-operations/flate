package gittree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/tree"
)

// ViewMode selects source artifact or remote-base membership policy.
type ViewMode uint8

const (
	// Source omits links and applies the Flux source-ignore policy.
	Source ViewMode = iota
	// RemoteBase follows confined file aliases and omits hidden directories.
	RemoteBase
)

// ViewOptions shapes a commit view without changing its object backing.
type ViewOptions struct {
	Mode     ViewMode
	Sparse   []string
	Ignore   string
	Worktree billy.Filesystem
}

// FS reads a commit lazily. Backing MUST remain immutable while readers exist.
// Pooled storers are exclusive to an operation and retain no expanded objects.
type FS struct {
	storers  sync.Pool
	worktree billy.Filesystem
	nodes    map[string]*gitNode
}

type gitNode struct {
	name     string
	hash     plumbing.Hash
	mode     fs.FileMode
	children []fs.DirEntry
	info     atomic.Pointer[gitInfo]
}

type gitEntry struct {
	input *FS
	name  string
}

func (e gitEntry) Name() string               { return path.Base(e.name) }
func (e gitEntry) Type() fs.FileMode          { return e.input.nodes[e.name].mode.Type() }
func (e gitEntry) IsDir() bool                { return e.Type().IsDir() }
func (e gitEntry) Info() (fs.FileInfo, error) { return e.input.Lstat(e.name) }

type gitInfo struct {
	*gitNode
	size int64
}

func (i gitInfo) Name() string       { return i.name }
func (i gitInfo) Mode() fs.FileMode  { return i.mode }
func (i gitInfo) Size() int64        { return i.size }
func (i gitInfo) IsDir() bool        { return i.mode.IsDir() }
func (i gitInfo) ModTime() time.Time { return time.Time{} }
func (i gitInfo) Sys() any           { return nil }

// New validates metadata and compiles visibility without reading regular blob bodies.
func New(ctx context.Context, backing billy.Filesystem, commit plumbing.Hash, opts ViewOptions) (*FS, error) {
	input := &FS{worktree: opts.Worktree, nodes: map[string]*gitNode{".": {name: ".", mode: fs.ModeDir | 0o755}}}
	input.storers.New = func() any {
		return filesystem.NewStorageWithOptions(backing, cache.NewObjectLRU(0), filesystem.Options{})
	}
	if err := input.walk(ctx, commit); err != nil {
		return nil, err
	}
	visible := func(name string) bool {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			base := path.Base(parent)
			if base == "node_modules" || strings.HasPrefix(base, ".") {
				return false
			}
		}
		return true
	}
	if opts.Mode == Source {
		matcher, err := sourceignore.NewFS(input, opts.Ignore, true)
		if err != nil {
			return nil, err
		}
		visible = func(name string) bool {
			return input.nodes[name].mode&fs.ModeSymlink == 0 &&
				(len(opts.Sparse) == 0 || slices.ContainsFunc(opts.Sparse, func(dir string) bool { return strings.HasPrefix(name, dir) })) &&
				!matcher.Match(filepath.FromSlash(name), false)
		}
	}
	names := slices.Sorted(maps.Keys(input.nodes))
	var links *tree.Builder
	if opts.Mode == RemoteBase {
		links = tree.NewBuilder(input)
		for _, name := range names {
			node := input.nodes[name]
			if node.mode&fs.ModeSymlink == 0 {
				continue
			}
			reader, err := input.openBlob(node.hash)
			if err != nil {
				return nil, err
			}
			data, readErr := io.ReadAll(reader)
			if err := errors.Join(readErr, reader.Close()); err != nil {
				return nil, err
			}
			if err := links.RemoveAll(name); err != nil {
				return nil, err
			}
			if err := links.AddSymlink(name, string(data)); err != nil {
				return nil, err
			}
		}
	}
	kept := map[string]bool{".": true}
	for _, name := range names {
		node := input.nodes[name]
		if node.mode.IsDir() || !visible(name) {
			continue
		}
		if node.mode&fs.ModeSymlink != 0 {
			resolved, err := links.Resolve(name)
			if errors.Is(err, tree.ErrEscape) || errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			target := input.nodes[resolved]
			if !target.mode.IsRegular() {
				continue
			}
			input.nodes[name] = &gitNode{name: path.Base(name), hash: target.hash, mode: target.mode}
		}
		kept[name] = true
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			kept[parent] = true
		}
	}
	for name := range input.nodes {
		if !kept[name] {
			delete(input.nodes, name)
		}
	}
	input.list()
	return input, nil
}

func (g *FS) walk(ctx context.Context, commit plumbing.Hash) error {
	if g.worktree != nil {
		return g.walkWorktree(ctx, ".")
	}
	storer := g.storers.Get().(*filesystem.Storage)
	defer g.storers.Put(storer)
	c, err := object.GetCommit(storer, commit)
	if err != nil {
		return err
	}
	return g.walkCommit(ctx, storer, c.TreeHash, ".")
}

func (g *FS) walkCommit(ctx context.Context, storer *filesystem.Storage, hash plumbing.Hash, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t, err := object.GetTree(storer, hash)
	if err != nil {
		return err
	}
	for _, entry := range t.Entries {
		if entry.Name == "" || entry.Name == "." || entry.Name == ".." || strings.ContainsAny(entry.Name, "/\x00") {
			return fmt.Errorf("malformed tree entry name %q", entry.Name)
		}
		name := path.Join(prefix, entry.Name)
		mode, err := entry.Mode.ToOSFileMode()
		if err != nil {
			return err
		}
		if entry.Mode == filemode.Submodule {
			slog.Warn("gittree: skipping submodule", "path", name)
			continue
		}
		if !mode.IsRegular() && !mode.IsDir() && mode&fs.ModeSymlink == 0 {
			continue
		}
		g.nodes[name] = &gitNode{name: entry.Name, hash: entry.Hash, mode: mode}
		g.nodes[prefix].children = append(g.nodes[prefix].children, gitEntry{g, name})
		if mode.IsDir() {
			if err := g.walkCommit(ctx, storer, entry.Hash, name); err != nil {
				return err
			}
		}
	}
	slices.SortFunc(g.nodes[prefix].children, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return nil
}

func (g *FS) walkWorktree(ctx context.Context, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := g.worktree.ReadDir(filepath.FromSlash(prefix))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == ".git" {
			continue
		}
		name := path.Join(prefix, entry.Name())
		if !fs.ValidPath(name) || strings.ContainsAny(entry.Name(), "/\x00") {
			return fs.ErrInvalid
		}
		mode := entry.Mode()
		if !mode.IsRegular() && !mode.IsDir() && mode&fs.ModeSymlink == 0 {
			continue
		}
		g.nodes[name] = &gitNode{name: entry.Name(), mode: mode}
		g.nodes[prefix].children = append(g.nodes[prefix].children, gitEntry{g, name})
		if mode.IsDir() {
			if err := g.walkWorktree(ctx, name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *FS) list() {
	for _, node := range g.nodes {
		node.children = nil
	}
	for name := range g.nodes {
		if name == "." {
			continue
		}
		parent := g.nodes[path.Dir(name)]
		parent.children = append(parent.children, gitEntry{g, name})
	}
	for _, node := range g.nodes {
		slices.SortFunc(node.children, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	}
}

// Lstat exposes only visible entries without following the final link.
func (g *FS) Lstat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrInvalid}
	}
	node := g.nodes[name]
	if node == nil {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrNotExist}
	}
	if node.mode.IsDir() {
		return gitInfo{gitNode: node}, nil
	}
	if g.worktree != nil {
		return g.worktree.Lstat(filepath.FromSlash(name))
	}
	if info := node.info.Load(); info != nil {
		return info, nil
	}
	storer := g.storers.Get().(*filesystem.Storage)
	defer g.storers.Put(storer)
	size, err := storer.EncodedObjectSize(node.hash)
	if err != nil {
		return nil, err
	}
	info := &gitInfo{node, size}
	node.info.Store(info)
	return info, nil
}

// Stat exposes visible source and remote-base entries.
func (g *FS) Stat(name string) (fs.FileInfo, error) { return g.Lstat(name) }

// ReadDir returns sorted caller-owned entries.
func (g *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	info, err := g.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	return slices.Clone(g.nodes[name].children), nil
}

// Open returns an independent cursor and retains backing through reader Close.
func (g *FS) Open(name string) (fs.File, error) {
	if node := g.nodes[name]; node != nil && node.mode&fs.ModeSymlink != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	info, err := g.Lstat(name)
	if err != nil {
		return nil, err
	}
	f := &gitFile{info: info, input: g}
	if info.IsDir() {
		f.entries = g.nodes[name].children
	} else if g.worktree != nil {
		f.reader, err = g.worktree.Open(filepath.FromSlash(name))
	} else {
		f.reader, err = g.openBlob(g.nodes[name].hash)
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (g *FS) openBlob(hash plumbing.Hash) (io.ReadCloser, error) {
	// Upstream's streaming delta reader does not reset its position on backward copies.
	storer := g.storers.Get().(*filesystem.Storage)
	defer g.storers.Put(storer)
	blob, err := object.GetBlob(storer, hash)
	if err != nil {
		return nil, err
	}
	return blob.Reader()
}

type gitFile struct {
	info    fs.FileInfo
	input   *FS
	reader  io.ReadCloser
	entries []fs.DirEntry
	cursor  int
}

func (f *gitFile) Stat() (fs.FileInfo, error) {
	if f.input == nil {
		return nil, fs.ErrClosed
	}
	return f.info, nil
}
func (f *gitFile) Close() error {
	if f.input == nil {
		return fs.ErrClosed
	}
	var err error
	if f.reader != nil {
		err = f.reader.Close()
	}
	f.input, f.reader, f.info, f.entries = nil, nil, nil, nil
	return err
}
func (f *gitFile) Read(p []byte) (int, error) {
	if f.input == nil {
		return 0, fs.ErrClosed
	}
	if f.reader == nil {
		return 0, fs.ErrInvalid
	}
	return f.reader.Read(p)
}
func (f *gitFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.input == nil {
		return nil, fs.ErrClosed
	}
	if !f.info.IsDir() {
		return nil, fs.ErrInvalid
	}
	if n > 0 && f.cursor == len(f.entries) {
		return nil, io.EOF
	}
	end := len(f.entries)
	if n > 0 {
		end = min(end, f.cursor+n)
	}
	entries := slices.Clone(f.entries[f.cursor:end])
	f.cursor = end
	return entries, nil
}
