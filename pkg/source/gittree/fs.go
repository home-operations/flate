// Package gittree admits direct inputs and lazy Git tree views.
package gittree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
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
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/transactional"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/tree"
)

// ViewMode selects admitted input and source artifact membership.
type ViewMode uint8

const (
	// Source omits links and applies the Flux source-ignore policy.
	Source ViewMode = iota
	// RemoteBase follows confined file aliases and omits hidden directories.
	RemoteBase
	// Direct admits local files without source-ignore shaping and omits links.
	Direct
)

// ViewOptions shapes a commit view without changing its object backing.
type ViewOptions struct {
	// Content carries source coordinates without filesystem or routing state.
	Content  any
	Mode     ViewMode
	Sparse   []string
	Ignore   string
	Worktree billy.Filesystem
	// Current supplies admitted bytes; Git views also expose read-only local objects.
	Current fs.FS
	// Changes routes complete, unfiltered differences to commit bodies.
	Changes []string
}

// FS is an admitted filesystem. Its representation is selected once by New.
type FS struct{ view }

type view interface {
	fs.ReadFileFS
	fs.ReadDirFS
	fs.StatFS
	fs.ReadLinkFS
	// OpenRegular requires a regular file already admitted by Stat or ReadDir.
	OpenRegular(string) (fs.File, error)
}

// Backing MUST remain immutable while readers exist; pooled storers are exclusive
// to an operation and retain no expanded objects.
type gitView struct {
	storers  sync.Pool
	worktree billy.Filesystem
	nodes    map[string]*gitNode
	backing  billy.Filesystem
	current  fs.FS
	changes  map[string]bool
	refresh  *ViewOptions
	commit   plumbing.Hash
	identity string
	mode     ViewMode
}

type gitNode struct {
	name     string
	hash     plumbing.Hash
	mode     fs.FileMode
	children []fs.DirEntry
	info     atomic.Pointer[gitInfo]
}

type gitEntry struct {
	input *gitView
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

// New validates Git metadata and creates views without reading regular blob bodies.
// Direct with a zero commit retains Current admitted semantics. Mutable Source
// views are refreshed for one operation; pinned views retain their backing.
// Source without additional shaping retains an already shaped Current view.
// The returned identity pins committed selection; mutable views return an empty identity.
func New(ctx context.Context, backing billy.Filesystem, commit plumbing.Hash, opts ViewOptions) (*FS, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if current, ok := opts.Current.(*FS); ok {
		switch v := current.view.(type) {
		case *localView:
			opts.Current = newLocal(v.root)
			if opts.Mode == Direct && commit == plumbing.ZeroHash {
				return opts.Current.(*FS), "", nil
			}
		case *gitView:
			if v.refresh != nil {
				refreshed, _, err := New(ctx, v.backing, v.commit, *v.refresh)
				if err != nil {
					return nil, "", err
				}
				opts.Current = refreshed
				v = refreshed.view.(*gitView)
			}
			if commit == plumbing.ZeroHash && (opts.Mode == Direct || (opts.Mode == Source && v.mode == Source && opts.Ignore == "" && len(opts.Sparse) == 0)) {
				return opts.Current.(*FS), v.identity, nil
			}
		case *directView:
			if opts.Mode == Direct && commit == plumbing.ZeroHash {
				return current, "", nil
			}
		}
	}
	if opts.Mode == Direct && commit == plumbing.ZeroHash && opts.Current != nil {
		input := &struct {
			FS
			directView
		}{directView: directView{FS: opts.Current}}
		input.view = &input.directView
		return &input.FS, "", nil
	}
	if opts.Mode == Direct && opts.Current == nil && opts.Worktree != nil {
		root := opts.Worktree.Root()
		if _, err := os.Stat(root); err != nil {
			return nil, "", err
		}
		return newLocal(root), "", nil
	}
	input, err := newTree(ctx, backing, commit, opts)
	if err != nil {
		if opts.Mode == Source && errors.Is(err, fs.ErrPermission) {
			if current, ok := opts.Current.(*FS); ok {
				if mounted, ok := current.view.(*localView); ok {
					// Flux reads .sourceignore before listing each directory; retain its diagnostic.
					if _, ignoreErr := sourceignore.New(mounted.root, nil, true); ignoreErr != nil {
						return nil, "", ignoreErr
					}
				}
			}
			return nil, "", fmt.Errorf("sourceignore load: %w", err)
		}
		return nil, "", err
	}
	g := input.view.(*gitView)
	currentIdentity := ""
	if current := currentTree(opts.Current); commit == plumbing.ZeroHash && current != nil {
		currentIdentity = current.identity
	}
	committed := commit != plumbing.ZeroHash && (opts.Worktree == nil || opts.Content != nil)
	if committed || currentIdentity != "" {
		g.identity = manifest.Fingerprint(struct {
			Content any
			Commit  string
			Current string
			Mode    ViewMode
			Sparse  []string `json:",omitempty"`
			Ignore  string
		}{opts.Content, commit.String(), currentIdentity, opts.Mode, opts.Sparse, opts.Ignore})
		if g.identity == "" {
			return nil, "", fmt.Errorf("%w: git view content cannot be fingerprinted", manifest.ErrInput)
		}
	}
	if err := g.shape(opts); err != nil {
		return nil, "", err
	}
	return input, g.identity, nil
}

func currentTree(input fs.FS) *gitView {
	if current, ok := input.(*FS); ok {
		v, _ := current.view.(*gitView)
		return v
	}
	return nil
}

func newTree(ctx context.Context, backing billy.Filesystem, commit plumbing.Hash, opts ViewOptions) (*FS, error) {
	current := currentTree(opts.Current)
	native := current != nil
	combined := &struct {
		FS
		gitView
	}{commit: commit, mode: opts.Mode, worktree: opts.Worktree, backing: backing, current: opts.Current, nodes: map[string]*gitNode{".": {name: ".", mode: fs.ModeDir | 0o755}}}
	input := &combined.gitView
	combined.view = input
	input.storers.New = func() any {
		upper := filesystem.NewStorageWithOptions(backing, cache.NewObjectLRU(0), filesystem.Options{})
		if native && current.backing != nil {
			lower := filesystem.NewStorageWithOptions(current.backing, cache.NewObjectLRU(0), filesystem.Options{})
			return transactional.NewObjectStorage(lower, upper)
		}
		return storer.EncodedObjectStorer(upper)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.Current != nil {
		input.changes = make(map[string]bool, len(opts.Changes))
		for _, name := range opts.Changes {
			input.changes[name] = true
		}
	}
	if opts.Current != nil && commit == plumbing.ZeroHash {
		if native {
			input.storers.New = current.storers.New
			input.backing = current.backing
			input.worktree = current.worktree
		}
		if !native || current.backing == nil && current.worktree == nil {
			if err := input.walkWorktree(ctx, "."); err != nil {
				return nil, err
			}
		} else {
			for name, node := range current.nodes {
				input.nodes[name] = &gitNode{name: node.name, hash: node.hash, mode: node.mode, children: node.children}
			}
		}
	} else if err := input.walk(ctx, commit); err != nil {
		return nil, err
	}
	return &combined.FS, nil
}

func (g *gitView) shape(opts ViewOptions) error {
	visible := func(name string) bool {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			base := path.Base(parent)
			if base == "node_modules" || strings.HasPrefix(base, ".") {
				return false
			}
		}
		return true
	}
	if opts.Mode == Direct {
		visible = func(name string) bool { return g.nodes[name].mode&fs.ModeSymlink == 0 }
	}
	mutable := g.identity == "" && (opts.Current != nil || opts.Worktree != nil)
	routed := g.commit != plumbing.ZeroHash && opts.Current != nil
	if mutable || routed {
		refresh := opts
		refresh.Sparse, refresh.Changes = slices.Clone(opts.Sparse), slices.Clone(opts.Changes)
		g.refresh = &refresh
	}
	if opts.Mode == Source {
		matcher, err := sourceignore.NewFS(g, nil, opts.Ignore, true)
		if err != nil {
			return err
		}
		visible = func(name string) bool {
			return g.nodes[name].mode&fs.ModeSymlink == 0 &&
				(len(opts.Sparse) == 0 || slices.ContainsFunc(opts.Sparse, func(dir string) bool { return strings.HasPrefix(name, dir) })) &&
				!matcher.Match(filepath.FromSlash(name), false)
		}
	}
	names := slices.Sorted(maps.Keys(g.nodes))
	var links *tree.Builder
	if opts.Mode == RemoteBase {
		raw := &gitView{nodes: g.nodes, backing: g.backing, current: g.current, changes: g.changes}
		raw.storers.New = g.storers.New
		links = tree.NewBuilder(raw)
		for _, name := range names {
			node := g.nodes[name]
			if node.mode&fs.ModeSymlink == 0 {
				continue
			}
			target, err := g.ReadLink(name)
			if err != nil {
				return err
			}
			if err := links.RemoveAll(name); err != nil {
				return err
			}
			if err := links.AddSymlink(name, target); err != nil {
				return err
			}
		}
	}
	kept := map[string]bool{".": true}
	for _, name := range names {
		node := g.nodes[name]
		if node.mode.IsDir() || !visible(name) {
			continue
		}
		if node.mode&fs.ModeSymlink != 0 {
			resolved, err := links.Resolve(name)
			if errors.Is(err, tree.ErrEscape) || errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			target := g.nodes[resolved]
			if !target.mode.IsRegular() {
				continue
			}
			g.nodes[name] = &gitNode{name: path.Base(name), hash: target.hash, mode: target.mode}
		}
		kept[name] = true
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			kept[parent] = true
		}
	}
	for name := range g.nodes {
		if !kept[name] {
			delete(g.nodes, name)
		}
	}
	g.list()
	return nil
}

func (g *gitView) walk(ctx context.Context, commit plumbing.Hash) error {
	if g.worktree != nil && g.current == nil {
		return g.walkWorktree(ctx, ".")
	}
	storer := g.storers.Get().(storer.EncodedObjectStorer)
	defer g.storers.Put(storer)
	c, err := object.GetCommit(storer, commit)
	if err != nil {
		return err
	}
	return g.walkCommit(ctx, storer, c.TreeHash, ".")
}

func (g *gitView) walkCommit(ctx context.Context, storer storer.EncodedObjectStorer, hash plumbing.Hash, prefix string) error {
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

func (g *gitView) walkWorktree(ctx context.Context, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.worktree == nil {
		return fs.WalkDir(g.current, prefix, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.Name() == ".git" {
				if entry.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if !fs.ValidPath(name) || strings.ContainsAny(entry.Name(), "/\x00") {
				return fs.ErrInvalid
			}
			mode := entry.Type()
			if !mode.IsRegular() && !mode.IsDir() && mode&fs.ModeSymlink == 0 {
				return nil
			}
			g.nodes[name] = &gitNode{name: entry.Name(), mode: mode}
			if name != prefix {
				parent := g.nodes[path.Dir(name)]
				parent.children = append(parent.children, gitEntry{g, name})
			}
			return nil
		})
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

func (g *gitView) list() {
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
func (g *gitView) Lstat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrInvalid}
	}
	node := g.nodes[name]
	if node == nil {
		if g.backing == nil && g.current != nil {
			if _, err := fs.Lstat(g.current, name); err != nil {
				return nil, err
			}
		}
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	if g.backing == nil && g.current != nil {
		return fs.Lstat(g.current, name)
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
	storer := g.storers.Get().(storer.EncodedObjectStorer)
	defer g.storers.Put(storer)
	var size int64
	var err error
	if g.current != nil && !g.changes[name] && node.mode.IsRegular() {
		var current fs.FileInfo
		current, err = fs.Stat(g.current, name)
		if err == nil {
			size = current.Size()
		}
	} else {
		size, err = storer.EncodedObjectSize(node.hash)
	}
	if err != nil {
		return nil, err
	}
	info := &gitInfo{node, size}
	node.info.Store(info)
	return info, nil
}

// Stat exposes visible entries and preserves admitted Current semantics.
func (g *gitView) Stat(name string) (fs.FileInfo, error) {
	info, err := g.Lstat(name)
	if pathErr, ok := errors.AsType[*fs.PathError](err); ok {
		return info, &fs.PathError{Op: "stat", Path: pathErr.Path, Err: pathErr.Err}
	}
	return info, err
}

// ReadDir returns sorted caller-owned entries.
func (g *gitView) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
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

// Open returns an independent cursor and retains backing through reader Close.
func (g *gitView) Open(name string) (fs.File, error) {
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
	} else if g.current != nil && !g.changes[name] {
		f.reader, err = g.current.Open(name)
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

func (g *gitView) openBlob(hash plumbing.Hash) (io.ReadCloser, error) {
	// Upstream's streaming delta reader does not reset its position on backward copies.
	storer := g.storers.Get().(storer.EncodedObjectStorer)
	defer g.storers.Put(storer)
	blob, err := object.GetBlob(storer, hash)
	if err != nil {
		return nil, err
	}
	return blob.Reader()
}

type gitFile struct {
	info    fs.FileInfo
	input   fs.FS
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

// ReadFile reads admitted source members with plain filesystem reads. The local
// checkout MUST remain stable for the lifetime of its source view.
func (g *gitView) ReadFile(name string) ([]byte, error) {
	node := g.nodes[name]
	if node != nil && node.mode.IsRegular() && !g.changes[name] {
		if current, ok := g.current.(*FS); ok {
			if mounted, ok := current.view.(*localView); ok {
				abs, err := mounted.mounted(name)
				if err != nil {
					return nil, err
				}
				return os.ReadFile(abs) //nolint:gosec // Source membership admits regular files from the stable checkout.
			}
		}
	}
	f, err := g.Open(name)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	return data, errors.Join(err, f.Close())
}

// OpenRegular skips metadata checks for a regular file already admitted by Stat
// or ReadDir. Callers MUST retain the listing's immutable-backing assumption.
func (g *gitView) OpenRegular(name string) (fs.File, error) { return g.Open(name) }

// ReadLink exposes visible link metadata to the remote-base resolver.
func (g *gitView) ReadLink(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	if g.mode != Direct {
		node := g.nodes[name]
		if node == nil {
			return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrNotExist}
		}
		if node.mode&fs.ModeSymlink == 0 {
			return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
		}
	}
	if g.backing == nil && g.worktree == nil && g.current != nil {
		return fs.ReadLink(g.current, name)
	}
	var target string
	if g.worktree != nil {
		var err error
		target, err = g.worktree.Readlink(filepath.FromSlash(name))
		if err != nil {
			return "", err
		}
	} else {
		node := g.nodes[name]
		if node == nil {
			return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrNotExist}
		}
		if node.mode&fs.ModeSymlink == 0 {
			return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
		}
		r, err := g.openBlob(node.hash)
		if err != nil {
			return "", err
		}
		data, err := io.ReadAll(r)
		if err := errors.Join(err, r.Close()); err != nil {
			return "", err
		}
		target = string(data)
	}
	root := ""
	if current, ok := g.current.(*FS); ok {
		if mounted, ok := current.view.(*localView); ok {
			root = mounted.root
		}
	}
	if filepath.IsAbs(target) && root != "" {
		rel, err := filepath.Rel(root, target)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			rel, err = filepath.Rel(filepath.FromSlash(path.Dir(name)), rel)
			if err != nil {
				return "", err
			}
			target = filepath.ToSlash(rel)
		}
	}
	return target, nil
}
