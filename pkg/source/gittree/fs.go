package gittree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"path/filepath"
	"slices"
	"strings"
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

// ViewMode selects source artifact, remote-base or baseline membership policy.
type ViewMode uint8

const (
	// Source omits links and applies the Flux source-ignore policy.
	Source ViewMode = iota
	// RemoteBase follows confined file aliases and omits hidden directories.
	RemoteBase
	// Base leaves baseline membership and link policy to its caller.
	Base
)

// ViewOptions shapes a commit view without changing its object backing.
type ViewOptions struct {
	Mode     ViewMode
	Sparse   []string
	Ignore   string
	Worktree billy.Filesystem
}

// FS reads a commit lazily. Backing MUST remain immutable while readers exist.
// Each object operation owns a private storer with no retained expanded objects.
type FS struct {
	backing  billy.Filesystem
	worktree billy.Filesystem
	nodes    map[string]*gitNode
	matcher  *sourceignore.Matcher
	resolver *tree.Tree
}

type gitNode struct {
	name     string
	hash     plumbing.Hash
	mode     fs.FileMode
	children []fs.DirEntry
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
	input := &FS{backing: backing, worktree: opts.Worktree, nodes: map[string]*gitNode{".": {name: ".", mode: fs.ModeDir | 0o755}}}
	if opts.Worktree != nil {
		if err := input.walkWorktree(ctx, "."); err != nil {
			return nil, err
		}
	} else {
		storer := filesystem.NewStorageWithOptions(backing, cache.NewObjectLRU(0), filesystem.Options{LargeObjectThreshold: 1})
		c, err := object.GetCommit(storer, commit)
		if err == nil {
			err = input.walkCommit(ctx, storer, c.TreeHash, ".")
		}
		if err := errors.Join(err, storer.Close()); err != nil {
			return nil, err
		}
	}
	if opts.Mode == Base {
		input.list()
		resolver, err := tree.NewBuilder(input).Build()
		if err != nil {
			return nil, err
		}
		input.resolver = resolver
		return input, nil
	}
	var err error
	if opts.Mode == Source {
		for name, node := range input.nodes {
			if node.mode&fs.ModeSymlink != 0 {
				delete(input.nodes, name)
			}
		}
		input.list()
		input.matcher, err = sourceignore.NewFS(input, opts.Ignore, true)
		if err != nil {
			return nil, err
		}
	}
	kept := map[string]bool{".": true}
	var aliases map[string]*gitNode
	if opts.Mode == RemoteBase {
		input.list()
		aliases = make(map[string]*gitNode)
		resolver, err := tree.NewBuilder(input).Build()
		if err != nil {
			return nil, err
		}
		for name, node := range input.nodes {
			if node.mode&fs.ModeSymlink == 0 {
				continue
			}
			resolved, err := resolver.Resolve(name)
			if errors.Is(err, tree.ErrEscape) || errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			target := input.nodes[resolved]
			if target.mode.IsRegular() {
				alias := *target
				alias.name = path.Base(name)
				aliases[name] = &alias
			}
		}
	}
	for name, node := range input.nodes {
		if node.mode.IsDir() {
			continue
		}
		if node.mode&fs.ModeSymlink != 0 {
			if aliases[name] == nil {
				continue
			}
		}
		if !selected(name, opts.Sparse) {
			continue
		}
		if input.matcher != nil && input.matcher.Match(filepath.FromSlash(name), false) {
			continue
		}
		if opts.Mode == RemoteBase {
			hidden := false
			for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
				base := path.Base(parent)
				hidden = hidden || base == "node_modules" || strings.HasPrefix(base, ".")
			}
			if hidden {
				continue
			}
		}
		kept[name] = true
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			kept[parent] = true
		}
	}
	for name := range input.nodes {
		if !kept[name] {
			delete(input.nodes, name)
		} else if alias := aliases[name]; alias != nil {
			input.nodes[name] = alias
		}
	}
	input.list()
	return input, nil
}

func selected(name string, directories []string) bool {
	if len(directories) == 0 {
		return true
	}
	for _, directory := range directories {
		directory = strings.TrimSuffix(filepath.ToSlash(directory), "/")
		if name == directory || strings.HasPrefix(name, directory+"/") {
			return true
		}
	}
	return false
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
		if mode.IsDir() {
			if err := g.walkCommit(ctx, storer, entry.Hash, name); err != nil {
				return err
			}
		}
	}
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
		clear(node.children)
		node.children = node.children[:0]
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
	storer := filesystem.NewStorageWithOptions(g.backing, cache.NewObjectLRU(0), filesystem.Options{LargeObjectThreshold: 1})
	size, err := storer.EncodedObjectSize(node.hash)
	if err := errors.Join(err, storer.Close()); err != nil {
		return nil, err
	}
	return gitInfo{node, size}, nil
}

// Stat follows baseline links; source and remote-base views contain only regular entries.
func (g *FS) Stat(name string) (fs.FileInfo, error) {
	if g.resolver != nil {
		return g.resolver.Stat(name)
	}
	return g.Lstat(name)
}

// ReadDir returns sorted caller-owned entries.
func (g *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if g.resolver != nil {
		resolved, err := g.resolver.Resolve(name)
		if err != nil {
			return nil, err
		}
		name = resolved
	}
	info, err := g.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	return slices.Clone(g.nodes[name].children), nil
}

// ReadLink reads a lexical baseline link without following its target.
func (g *FS) ReadLink(name string) (string, error) {
	info, err := g.Lstat(name)
	if err != nil {
		return "", err
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	if g.worktree != nil {
		return g.worktree.Readlink(filepath.FromSlash(name))
	}
	reader, err := g.openBlob(g.nodes[name].hash)
	if err != nil {
		return "", err
	}
	data, readErr := io.ReadAll(reader)
	return string(data), errors.Join(readErr, reader.Close())
}

// Open returns an independent cursor and retains backing through reader Close.
func (g *FS) Open(name string) (fs.File, error) {
	if g.resolver != nil {
		resolved, err := g.resolver.Resolve(name)
		if err != nil {
			return nil, err
		}
		name = resolved
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

type blobReader struct {
	io.ReadCloser
	storer *filesystem.Storage
}

func (r *blobReader) Close() error {
	err := errors.Join(r.ReadCloser.Close(), r.storer.Close())
	r.ReadCloser, r.storer = nil, nil
	return err
}

func (g *FS) openBlob(hash plumbing.Hash) (io.ReadCloser, error) {
	storer := filesystem.NewStorageWithOptions(g.backing, cache.NewObjectLRU(0), filesystem.Options{LargeObjectThreshold: 1})
	blob, err := object.GetBlob(storer, hash)
	if err != nil {
		_ = storer.Close()
		return nil, err
	}
	reader, err := blob.Reader()
	if err != nil {
		_ = storer.Close()
		return nil, err
	}
	return &blobReader{reader, storer}, nil
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
