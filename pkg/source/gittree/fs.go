// Package gittree admits direct inputs and lazy Git tree views.
package gittree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/sourceignore"
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
	policy *localPolicy
	// Content carries source coordinates without filesystem or routing state.
	Content  any
	Location *gitrepo.Location
	Mode     ViewMode
	Sparse   []string
	Ignore   string
	Worktree billy.Filesystem
	// Current supplies admitted bytes; Git views also expose read-only local objects.
	Current fs.FS
	// CurrentContent carries the acquisition owner's identity when Current wraps its admitted view.
	CurrentContent string
	// Changes routes complete, unfiltered differences to commit bodies.
	Changes []string
}

// NormalizeSparsePaths removes one leading ./ from each sparse selection.
// The result may share backing with paths and MUST remain immutable.
func NormalizeSparsePaths(paths []string) []string {
	for i, path := range paths {
		if !strings.HasPrefix(path, "./") {
			continue
		}
		paths = slices.Clone(paths)
		for j := i; j < len(paths); j++ {
			paths[j] = strings.TrimPrefix(paths[j], "./")
		}
		return paths
	}
	return paths
}

// FS is an admitted filesystem. Its representation is selected once by New.
type FS struct {
	view

	// Warnings is immutable admission evidence, nil when no advisory was observed.
	Warnings []manifest.Warning
	location *gitrepo.Location
	policy   *sourceignore.Matcher
	snapshot *localPolicy
	content  atomic.Pointer[contentFingerprint]
}

// Location returns the resolved disk identity retained by admission.
func (g *FS) Location() *gitrepo.Location { return g.location }

// OperationView creates private local observations while retaining admitted membership.
func (g *FS) OperationView() *FS {
	if local, ok := g.view.(*localView); ok {
		input := newLocal(local.root, true, g.snapshot)
		input.location = g.location
		input.Warnings = g.Warnings
		return input
	}
	return g
}

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
	denied   map[string]bool
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
// Mutable views refresh before reuse or construction; committed identities stay pinned.
// An error may return warning evidence without a usable filesystem.
//
//nolint:gocritic // The SDK accepts a value snapshot; a pointer parameter would change its call and aliasing contracts.
func New(ctx context.Context, backing billy.Filesystem, commit plumbing.Hash, opts ViewOptions) (*FS, string, error) {
	if err := ctx.Err(); err != nil {
		current, _ := opts.Current.(*FS)
		return warningOnly(current), "", err
	}
	direct, source := opts.Mode == Direct, opts.Mode == Source
	current, matcher, reuse, err := opts.refreshCurrent(ctx, commit)
	if err != nil {
		return warningOnly(current), "", err
	}
	identity := opts.CurrentContent
	if reuse && opts.Current != nil && (current != nil || identity == "") {
		if current != nil {
			return current, identity, nil
		}
		input := &struct {
			FS
			directView

			policyState localPolicy
		}{directView: directView{FS: opts.Current}}
		input.view = &input.directView
		input.location = opts.Location
		input.snapshot = &input.policyState
		return &input.FS, "", nil
	}
	if direct && opts.Current == nil && opts.Worktree != nil {
		root := opts.Worktree.Root()
		if _, statErr := os.Stat(root); statErr != nil {
			return nil, "", statErr //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
		}
		input := newLocal(root, false, &localPolicy{})
		input.location = opts.Location
		return input, "", nil
	}
	input, err := newTree(ctx, backing, commit, new(opts))
	if err != nil {
		if source && errors.Is(err, fs.ErrPermission) {
			return warningOnly(input), "", fmt.Errorf("sourceignore load: %w", err)
		}
		return warningOnly(input), "", err
	}
	input.policy = matcher
	return input.finishGitView(ctx, commit, &opts, identity)
}

func warningOnly(input *FS) *FS {
	if input == nil || len(input.Warnings) == 0 {
		return nil
	}
	return &FS{Warnings: input.Warnings}
}

func currentTree(input fs.FS) *gitView {
	if current, ok := input.(*FS); ok {
		v, _ := current.view.(*gitView)
		return v
	}
	return nil
}

func newTree(ctx context.Context, backing billy.Filesystem, commit plumbing.Hash, optsValue *ViewOptions) (*FS, error) {
	opts := *optsValue
	current := currentTree(opts.Current)
	native := current != nil
	combined := &struct {
		FS
		gitView

		policyState localPolicy
	}{commit: commit, mode: opts.Mode, worktree: opts.Worktree, backing: backing, current: opts.Current, nodes: map[string]*gitNode{".": {name: ".", mode: fs.ModeDir | gitDirectoryMode}}}
	input := &combined.gitView
	combined.snapshot = opts.policy
	if combined.snapshot == nil {
		combined.snapshot = &combined.policyState
	}
	combined.view = input
	combined.location = opts.Location
	input.storers.New = func() any {
		upper := filesystem.NewStorageWithOptions(backing, cache.NewObjectLRU(0), filesystem.Options{})
		if native && current.backing != nil {
			lower := filesystem.NewStorageWithOptions(current.backing, cache.NewObjectLRU(0), filesystem.Options{})
			return transactional.NewObjectStorage(lower, upper)
		}
		return storer.EncodedObjectStorer(upper)
	}
	if err := ctx.Err(); err != nil {
		return warningOnly(&combined.FS), err
	}
	if opts.Current != nil {
		input.changes = make(map[string]bool, len(opts.Changes))
		for _, name := range opts.Changes {
			input.changes[name] = true
		}
	}
	if opts.Current != nil && commit == plumbing.ZeroHash {
		if err := input.populateCurrent(ctx, current, native); err != nil {
			return warningOnly(&combined.FS), err
		}
	} else {
		warnings, err := input.walk(ctx, commit)
		combined.Warnings = warnings
		if err != nil {
			return warningOnly(&combined.FS), err
		}
	}
	return &combined.FS, nil
}

func (g *gitView) walk(ctx context.Context, commit plumbing.Hash) ([]manifest.Warning, error) {
	if g.worktree != nil && g.current == nil {
		return nil, g.walkWorktree(ctx, ".")
	}
	objectStore := g.storers.Get().(storer.EncodedObjectStorer) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	defer g.storers.Put(objectStore)
	c, err := object.GetCommit(objectStore, commit)
	if err != nil {
		return nil, err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
	}
	return g.walkCommit(ctx, objectStore, c.TreeHash, ".")
}

func (g *gitView) walkCommit(
	ctx context.Context,
	objectStore storer.EncodedObjectStorer,
	hash plumbing.Hash,
	prefix string,
) ([]manifest.Warning, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	t, err := object.GetTree(objectStore, hash)
	if err != nil {
		return nil, err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
	}
	var warnings []manifest.Warning
	for _, entry := range t.Entries {
		if entry.Name == "" || entry.Name == "." || entry.Name == ".." || strings.ContainsAny(entry.Name, "/\x00") {
			return warnings, fmt.Errorf("malformed tree entry name %q", entry.Name)
		}
		name := path.Join(prefix, entry.Name)
		mode, err := entry.Mode.ToOSFileMode()
		if err != nil {
			return warnings, err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
		}
		if entry.Mode == filemode.Submodule {
			warnings = append(warnings, manifest.Warning{
				Category: manifest.WarnSkippedSubmodule,
				Message:  "skipping submodule", Count: 1, Detail: []string{name, g.commit.String()},
			})
			continue
		}
		if !mode.IsRegular() && !mode.IsDir() && mode&fs.ModeSymlink == 0 {
			continue
		}
		g.nodes[name] = &gitNode{name: entry.Name, hash: entry.Hash, mode: mode}
		g.nodes[prefix].children = append(g.nodes[prefix].children, gitEntry{g, name})
		if mode.IsDir() {
			nested, err := g.walkCommit(ctx, objectStore, entry.Hash, name)
			warnings = append(warnings, nested...)
			if err != nil {
				return warnings, err
			}
		}
	}
	slices.SortFunc(g.nodes[prefix].children, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return warnings, nil
}

func (g *gitView) walkWorktree(ctx context.Context, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	if g.worktree == nil {
		return g.walkCurrent(ctx, prefix)
	}
	entries, err := g.worktree.ReadDir(filepath.FromSlash(prefix))
	if err != nil {
		return err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
	}
	for _, entry := range entries {
		if entry.Name() == gitDirectory {
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
		return g.missingInfo(name)
	}
	if g.backing == nil && g.current != nil {
		//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
		return fs.Lstat(
			g.current,
			name,
		)
	}
	if node.mode.IsDir() {
		return gitInfo{gitNode: node}, nil
	}
	if g.worktree != nil {
		//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
		return g.worktree.Lstat(
			filepath.FromSlash(name),
		)
	}
	if info := node.info.Load(); info != nil {
		return info, nil
	}
	objectStore := g.storers.Get().(storer.EncodedObjectStorer) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	defer g.storers.Put(objectStore)
	var size int64
	var err error
	if g.current != nil && !g.changes[name] && node.mode.IsRegular() {
		var current fs.FileInfo
		current, err = fs.Stat(g.current, name)
		if err == nil {
			size = current.Size()
		}
	} else {
		size, err = objectStore.EncodedObjectSize(node.hash)
	}
	if err != nil {
		return nil, err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
	}
	info := &gitInfo{node, size}
	node.info.Store(info)
	return info, nil
}

// Stat exposes visible entries and preserves admitted Current semantics.
func (g *gitView) Stat(name string) (fs.FileInfo, error) {
	info, err := g.Lstat(name)
	if pathErr, ok := errors.AsType[*fs.PathError](err); ok {
		return info, &fs.PathError{Op: statOperation, Path: pathErr.Path, Err: pathErr.Err}
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
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	info, err := g.Lstat(name)
	if err != nil {
		return nil, err
	}
	f := &gitFile{info: info, input: g}
	switch {
	case info.IsDir():
		f.entries = g.nodes[name].children
	case g.current != nil && !g.changes[name]:
		f.reader, err = g.current.Open(name)
	case g.worktree != nil:
		f.reader, err = g.worktree.Open(filepath.FromSlash(name))
	default:
		f.reader, err = g.openBlob(g.nodes[name].hash)
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (g *gitView) openBlob(hash plumbing.Hash) (io.ReadCloser, error) {
	// Upstream's streaming delta reader does not reset its position on backward copies.
	objectStore := g.storers.Get().(storer.EncodedObjectStorer) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	defer g.storers.Put(objectStore)
	blob, err := object.GetBlob(objectStore, hash)
	if err != nil {
		return nil, err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
	}
	return blob.Reader() //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
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
	return err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
}

func (f *gitFile) Read(p []byte) (int, error) {
	if f.input == nil {
		return 0, fs.ErrClosed
	}
	if f.reader == nil {
		return 0, fs.ErrInvalid
	}
	//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
	return f.reader.Read(
		p,
	)
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
		var mounted *localView
		var local bool
		if current, ok := g.current.(*FS); ok {
			mounted, local = current.view.(*localView)
		}
		if local {
			abs, err := mounted.mounted(name)
			if err != nil {
				return nil, err
			}
			//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
			return os.ReadFile(abs)
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
		return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrInvalid}
	}
	if g.nodes[name] == nil {
		if _, err := g.Lstat(name); err != nil {
			return "", err
		}
	}
	if g.mode != Direct {
		node := g.nodes[name]
		if node == nil {
			return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrNotExist}
		}
		if node.mode&fs.ModeSymlink == 0 {
			return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrInvalid}
		}
	}
	if g.backing == nil && g.worktree == nil && g.current != nil {
		//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
		return fs.ReadLink(
			g.current,
			name,
		)
	}
	var target string
	if g.worktree != nil {
		var err error
		target, err = g.worktree.Readlink(filepath.FromSlash(name))
		if err != nil {
			return "", err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
		}
	} else {
		var err error
		target, err = g.blobLink(name)
		if err != nil {
			return "", err
		}
	}
	return g.relativeLink(name, target)
}

const gitDirectoryMode = 0o755

func (o *ViewOptions) refreshCurrent(
	ctx context.Context,
	commit plumbing.Hash,
) (*FS, *sourceignore.Matcher, bool, error) {
	source := o.Mode == Source
	reuse := o.Mode == Direct && commit == plumbing.ZeroHash
	var matcher *sourceignore.Matcher
	current, _ := o.Current.(*FS)
	if current == nil {
		return nil, matcher, reuse, nil
	}

	if o.Location == nil {
		o.Location = current.location
	}
	switch v := current.view.(type) {
	case *localView:
		current = current.OperationView()
		current.location = o.Location
	case *gitView:
		if v.refresh != nil {
			refreshed, _, err := New(ctx, v.backing, v.commit, *v.refresh)
			if err != nil {
				return refreshed, nil, false, err
			}
			current, v = refreshed, refreshed.view.(*gitView) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		}
		o.CurrentContent = v.identity
		if commit == plumbing.ZeroHash {
			matcher = current.policy
		}
		if source && matcher != nil && o.Ignore != "" {
			matcher = matcher.WithExtra(o.Ignore)
		}
		reuse = reuse ||
			commit == plumbing.ZeroHash && source && v.mode == Source && o.Ignore == "" && len(o.Sparse) == 0
	}
	if source && commit == plumbing.ZeroHash && matcher == nil {
		var err error
		matcher, err = o.currentPolicy(ctx, current)
		if err != nil {
			return current, nil, false, err
		}
	}
	o.Current = current
	return current, matcher, reuse, nil
}

func (g *FS) finishGitView(
	ctx context.Context,
	commit plumbing.Hash,
	opts *ViewOptions,
	identity string,
) (*FS, string, error) {
	v := g.view.(*gitView) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	if commit != plumbing.ZeroHash {
		identity = ""
	}
	if committed := commit != plumbing.ZeroHash &&
		(opts.Worktree == nil || opts.Content != nil); committed ||
		identity != "" {
		v.identity = manifest.Fingerprint(struct {
			Content         any
			Commit, Current string
			Mode            ViewMode
			Sparse          []string `json:",omitempty"`
			Ignore          string
		}{opts.Content, commit.String(), identity, opts.Mode, opts.Sparse, opts.Ignore})
		if v.identity == "" {
			return warningOnly(
					g,
				), "", manifest.NewError(
					manifest.ErrInput,
					nil,
					"git view content cannot be fingerprinted",
				)
		}
	}
	if err := v.shape(ctx, new(*opts), g); err != nil {
		return warningOnly(g), "", err
	}
	return g, v.identity, nil
}

func (g *gitView) populateCurrent(ctx context.Context, current *gitView, native bool) error {
	if native {
		g.storers.New = current.storers.New
		g.backing = current.backing
		g.worktree = current.worktree
	}
	if !native || current.backing == nil && current.worktree == nil {
		if err := g.walkWorktree(ctx, "."); err != nil {
			return err
		}
	} else {
		g.denied = current.denied
		for name, node := range current.nodes {
			g.nodes[name] = &gitNode{name: node.name, hash: node.hash, mode: node.mode, children: node.children}
		}
	}
	return nil
}

func (g *gitView) walkCurrent(ctx context.Context, prefix string) error {
	//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
	return fs.WalkDir(g.current, prefix, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			//nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
			return err
		}
		if entry.Name() == gitDirectory {
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

func (g *gitView) missingInfo(name string) (fs.FileInfo, error) {
	for parent := name; len(g.denied) > 0 && parent != "."; parent = path.Dir(parent) {
		if g.denied[parent] {
			return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrPermission}
		}
	}
	if g.backing == nil && g.current != nil {
		if _, err := fs.Lstat(g.current, name); err != nil {
			return nil, err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
		}
	}
	return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
}

func (g *gitView) blobLink(name string) (string, error) {
	node := g.nodes[name]
	if node == nil {
		return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrNotExist}
	}
	if node.mode&fs.ModeSymlink == 0 {
		return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrInvalid}
	}
	r, err := g.openBlob(node.hash)
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(r)
	if joinErr := errors.Join(err, r.Close()); joinErr != nil {
		return "", joinErr
	}
	return string(data), nil
}

func (g *gitView) relativeLink(name, target string) (string, error) {
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
				return "", err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
			}
			target = filepath.ToSlash(rel)
		}
	}
	return target, nil
}

func (o *ViewOptions) currentPolicy(ctx context.Context, current *FS) (*sourceignore.Matcher, error) {
	matcher, err := current.SourcePolicy(ctx)
	if err != nil {
		return nil, err
	}
	if o.Ignore != "" {
		matcher = matcher.WithExtra(o.Ignore)
	}
	return matcher, nil
}
