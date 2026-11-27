// Package gittree admits direct inputs and lazy Git tree views.
package gittree

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"weak"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/transactional"

	"github.com/home-operations/flate/internal/engine/source/gitrepo"
	"github.com/home-operations/flate/internal/engine/source/sourceignore"
	"github.com/home-operations/flate/pkg/manifest"
)

var errRejectedLink = fmt.Errorf("%w", fs.ErrPermission)

// ViewMode selects admitted input and source artifact membership.
type ViewMode uint8

const (
	// Source omits links and applies the Flux source-ignore policy.
	Source ViewMode = iota
	// RemoteBase follows confined file aliases and omits hidden directories.
	RemoteBase
	// Direct admits local files without source-ignore shaping and omits links.
	Direct
	// Checkout defers source filtering and retains lexical links within admitted membership.
	Checkout
)

// ViewOptions shapes a commit view without changing its object backing.
type ViewOptions struct {
	policy     *localPolicy
	membership membership
	// Content carries source coordinates without filesystem or routing state.
	Content  any
	Location *gitrepo.Location
	Mode     ViewMode
	Sparse   []string
	Ignore   string
	Worktree billy.Filesystem
	// Current supplies admitted bytes; Git views also expose read-only local objects.
	Current fs.FS
	// CurrentObserved borrows observations from the same construction stage.
	CurrentObserved bool
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
	Warnings   []manifest.Warning
	location   *gitrepo.Location
	sameFiles  *FS
	policy     *sourceignore.Matcher
	policyKey  string
	snapshot   *localPolicy
	membership membership
	content    atomic.Pointer[contentFingerprint]
}

// Location returns the resolved disk identity retained by admission.
func (g *FS) Location() *gitrepo.Location { return g.location }

type view interface {
	fs.ReadFileFS
	fs.ReadDirFS
	fs.ReadLinkFS
	inspect(string) (fs.FileInfo, string, error)
	// OpenRegular requires a regular file already admitted by Stat or ReadDir.
	OpenRegular(string) (fs.File, error)
}

// Backing MUST remain immutable while readers exist. Borrowed storers are exclusive
// to an operation and retain no expanded objects.
type gitView struct {
	storer    atomic.Pointer[weak.Pointer[borrowedStorer]]
	newStorer func() storer.EncodedObjectStorer
	worktree  billy.Filesystem
	nodes     map[string]*gitNode
	denied    map[string]bool
	backing   billy.Filesystem
	current   fs.FS
	changes   map[string]bool
	refresh   *ViewOptions
	commit    plumbing.Hash
	identity  string
	mode      ViewMode
	pruned    bool
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
//nolint:gocritic // Construction keeps live filesystem diagnostics separate from pinned identities without another admission layer; the SDK accepts a value snapshot.
func New(ctx context.Context, backing billy.Filesystem, commit plumbing.Hash, opts ViewOptions) (*FS, string, error) {
	if err := ctx.Err(); err != nil {
		current, _ := opts.Current.(*FS)
		return warningOnly(current), "", err
	}
	liveWorktree := opts.Current == nil && opts.Worktree != nil && (commit == plumbing.ZeroHash || opts.Content == nil)
	if liveWorktree && opts.Location == nil {
		var err error
		opts.Location, err = gitrepo.Locate(ctx, opts.Worktree.Root(), opts.Worktree.Root())
		if err != nil {
			return nil, "", err
		}
	}
	if liveWorktree && !opts.membership.captured {
		directory, err := openDirectoryAt(ctx, opts.Location, opts.membership.excluded)
		if err != nil {
			return nil, "", err
		}
		captured := directory.membership
		_ = directory.Close()
		opts.membership = captured
	}
	if liveWorktree && opts.Mode == Direct {
		root := opts.Worktree.Root()
		if _, err := os.Stat(root); err != nil {
			return nil, "", err //nolint:wrapcheck // New forwards the current view refresh error with its existing filesystem operation and path.
		}
		input := newLocal(root, false, &localPolicy{})
		input.location, input.membership = opts.Location, opts.membership
		return input, "", nil
	}
	source := opts.Mode == Source
	previous, _ := opts.Current.(*FS)
	current, matcher, reuse, err := opts.refreshCurrent(ctx, commit)
	identity := opts.CurrentContent
	switch {
	case err != nil:
		return warningOnly(current), "", err
	case reuse && opts.Current != nil && current != nil:
		return current, identity, nil
	case reuse && opts.Current != nil && identity == "":
		input := &struct {
			FS
			directView

			policyState localPolicy
		}{directView: directView{FS: opts.Current}}
		input.view = &input.directView
		input.location = opts.Location
		input.membership = opts.membership
		input.snapshot = &input.policyState
		return &input.FS, "", nil
	}
	input, err := newTree(ctx, backing, commit, new(opts))
	switch {
	case err != nil && source && errors.Is(err, fs.ErrPermission):
		return warningOnly(input), "", fmt.Errorf("sourceignore load: %w", err)
	case err != nil:
		return warningOnly(input), "", err
	}
	input.policy = matcher
	input.sameFiles = sourceOrigin(previous, opts.Mode, commit)
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
	if current, ok := opts.Current.(*FS); ok && opts.Mode == Checkout {
		if local, ok := current.view.(*localView); ok {
			opts.Worktree = osfs.New(local.root, osfs.WithBoundOS())
			opts.Current = nil
		}
	}
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
	combined.membership = opts.membership
	if opts.Worktree != nil {
		input.worktree = combined.FS.memberFilesystem(opts.Worktree, opts.Worktree.Root())
	}
	input.newStorer = func() storer.EncodedObjectStorer {
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

type borrowedStorer struct {
	storage storer.EncodedObjectStorer
	idle    *weak.Pointer[borrowedStorer]
}

// Concurrent borrowers MUST own separate mutable indexes. Idle indexes are reclaimable.
func (g *gitView) borrowStorer() *borrowedStorer {
	if cached := g.storer.Swap(nil); cached != nil {
		if borrowed := cached.Value(); borrowed != nil {
			return borrowed
		}
	}
	borrowed := &borrowedStorer{storage: g.newStorer()}
	borrowed.idle = new(weak.Make(borrowed))
	return borrowed
}

func (g *gitView) releaseStorer(borrowed *borrowedStorer) {
	g.storer.CompareAndSwap(nil, borrowed.idle)
	runtime.KeepAlive(borrowed)
}

func (g *gitView) walk(ctx context.Context, commit plumbing.Hash) ([]manifest.Warning, error) {
	if g.worktree != nil && g.current == nil {
		return nil, g.walkWorktree(ctx, ".")
	}
	borrowed := g.borrowStorer()
	defer g.releaseStorer(borrowed)
	objectStore := borrowed.storage
	c, err := object.GetCommit(objectStore, commit)
	if err != nil {
		return nil, fmt.Errorf("read commit %s: %w", commit, err)
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
		return nil, err
	}
	t, err := object.GetTree(objectStore, hash)
	if err != nil {
		return nil, fmt.Errorf("read tree %s: %w", hash, err)
	}
	var warnings []manifest.Warning
	for _, entry := range t.Entries {
		if entry.Name == "" || entry.Name == "." || entry.Name == ".." || strings.ContainsAny(entry.Name, "/\x00") {
			return warnings, fmt.Errorf("malformed tree entry name %q", entry.Name)
		}
		name := path.Join(prefix, entry.Name)
		mode, err := entry.Mode.ToOSFileMode()
		if err != nil {
			return warnings, fmt.Errorf("convert tree entry mode %q: %w", name, err)
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
		return err
	}
	if g.current != nil {
		return g.walkCurrent(ctx, prefix)
	}
	entries, err := g.worktree.ReadDir(filepath.FromSlash(prefix))
	if err != nil {
		return err //nolint:wrapcheck // walkWorktree forwards ReadDir PathError fields from the admitted Billy worktree.
	}
	for _, entry := range entries {
		if entry.Name() == gitDirectory {
			continue
		}
		name := path.Join(prefix, entry.Name())
		if !fs.ValidPath(name) || strings.ContainsAny(entry.Name(), "/\x00") {
			return fs.ErrInvalid
		}
		info, err := entry.Info()
		if err != nil {
			return err //nolint:wrapcheck // walkWorktree forwards DirEntry.Info PathError fields for the admitted worktree entry.
		}
		mode := info.Mode()
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
		return nil, &fs.PathError{Op: lstatOperation, Path: name, Err: fs.ErrInvalid}
	}
	node := g.nodes[name]
	if node == nil {
		return g.missingInfo(name)
	}
	if g.backing == nil && g.current != nil {
		//nolint:wrapcheck // Lstat forwards the current filesystem PathError fields without resolving the final alias.
		return fs.Lstat(
			g.current,
			name,
		)
	}
	if node.mode.IsDir() {
		return gitInfo{gitNode: node}, nil
	}
	if g.worktree != nil {
		//nolint:wrapcheck // Lstat forwards the admitted Billy worktree PathError fields.
		return g.worktree.Lstat(
			filepath.FromSlash(name),
		)
	}
	if info := node.info.Load(); info != nil {
		return info, nil
	}
	borrowed := g.borrowStorer()
	defer g.releaseStorer(borrowed)
	objectStore := borrowed.storage
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
		return nil, fmt.Errorf("size %q: %w", name, err)
	}
	info := &gitInfo{node, size}
	node.info.Store(info)
	return info, nil
}

func (g *gitView) inspect(name string) (fs.FileInfo, string, error) {
	info, err := g.Lstat(name)
	if pathErr, ok := errors.AsType[*fs.PathError](err); ok {
		err = &fs.PathError{Op: statOperation, Path: pathErr.Path, Err: pathErr.Err}
	}
	return info, "", err
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
	borrowed := g.borrowStorer()
	defer g.releaseStorer(borrowed)
	objectStore := borrowed.storage
	blob, err := object.GetBlob(objectStore, hash)
	if err != nil {
		return nil, fmt.Errorf("read blob %s: %w", hash, err)
	}
	reader, err := blob.Reader()
	if err != nil {
		return reader, fmt.Errorf("open blob reader %s: %w", hash, err)
	}
	return reader, nil
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
	return err //nolint:wrapcheck // Close forwards the owned reader close error after releasing its view and cursor.
}

func (f *gitFile) Read(p []byte) (int, error) {
	if f.input == nil {
		return 0, fs.ErrClosed
	}
	if f.reader == nil {
		return 0, fs.ErrInvalid
	}
	//nolint:wrapcheck // Read MUST forward the reader's exact io.EOF sentinel and byte count.
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
			//nolint:wrapcheck // ReadFile forwards current filesystem bytes and PathError fields.
			return os.ReadFile(abs)
		}
	}
	open := g.Open
	if node != nil && node.mode.IsRegular() {
		open = g.OpenRegular
	}
	f, err := open(name)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	return data, errors.Join(err, f.Close())
}

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
		//nolint:wrapcheck // ReadLink forwards current filesystem targets and PathError fields.
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
			return "", err //nolint:wrapcheck // ReadLink forwards the admitted Billy Readlink operation and path.
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

	excluded := o.membership.excluded
	switch {
	case len(excluded) == 0:
		o.membership.excluded = current.membership.excluded
	case len(current.membership.excluded) > 1:
		o.membership.excluded = slices.Concat(
			excluded[:1], current.membership.excluded.tail(), excluded.tail(),
		).normalizeTail()
	}
	o.membership.ignored, o.membership.captured = current.membership.ignored, current.membership.captured
	if commit != plumbing.ZeroHash {
		o.membership.ignored = nil
	}
	o.Location = cmp.Or(o.Location, current.location)
	switch v := current.view.(type) {
	case *localView:
		current = o.localCurrent(current)
	case *gitView:
		if v.refresh != nil {
			refresh := *v.refresh
			refresh.membership = o.membership
			refreshed, _, err := New(ctx, v.backing, v.commit, refresh)
			if err != nil {
				return refreshed, nil, false, err
			}
			current, v = refreshed, refreshed.view.(*gitView) //nolint:errcheck // New publishes *gitView before refreshCurrent reads the refreshed representation; corruption MUST panic.
		}
		o.CurrentContent = v.identity
		if commit == plumbing.ZeroHash && !source {
			matcher = current.policy
		}
		reuse = reuse ||
			commit == plumbing.ZeroHash && source && v.mode == Source && o.Ignore == "" && len(o.Sparse) == 0
	}
	if source && commit == plumbing.ZeroHash {
		var err error
		matcher, err = current.sourcePolicy(ctx, o.Ignore, false)
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
	v := g.view.(*gitView) //nolint:errcheck // New constructs *gitView before finishGitView selects its nodes; corruption MUST panic.
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
			Excluded        string   `json:",omitempty"`
			ExcludedPaths   []string `json:",omitempty"`
		}{
			opts.Content, commit.String(), identity, opts.Mode, opts.Sparse, opts.Ignore,
			opts.membership.excluded.primary(), opts.membership.excluded.tail(),
		})
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
		g.newStorer = current.newStorer
		g.backing = current.backing
		g.worktree = current.worktree
	}
	if !native || current.backing == nil && current.worktree == nil {
		if err := g.walkWorktree(ctx, "."); err != nil {
			return err
		}
	} else {
		// Omitted links have no corresponding nodes to select again.
		g.denied = current.denied
		for name, node := range current.nodes {
			g.nodes[name] = &gitNode{name: node.name, hash: node.hash, mode: node.mode, children: node.children}
		}
	}
	return nil
}

func (g *gitView) walkCurrent(ctx context.Context, prefix string) error {
	//nolint:wrapcheck // walkCurrent forwards fs.WalkDir PathError fields and callback cancellation without changing traversal evidence.
	return fs.WalkDir(g.current, prefix, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Name() == gitDirectory {
			g.pruned = true
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
			g.pruned = true
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
				return "", fmt.Errorf("relate link target %q: %w", name, err)
			}
			target = filepath.ToSlash(rel)
		}
	}
	return target, nil
}
