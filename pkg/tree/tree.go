// Package tree provides immutable, confined in-memory filesystems and private
// construction overlays. It depends only on the standard library.
package tree

import (
	"bytes"
	"errors"
	"io/fs"
	"path"
	"slices"
	"strings"
	"time"
)

// ErrEscape identifies a path or link target outside the tree. All absolute
// link targets are escapes, including host paths that point inside an input root;
// input loaders must translate those targets before construction if permitted.
var ErrEscape = errors.New("path escapes tree")

// ErrLoop identifies resolution exceeding eight symlink traversals.
var ErrLoop = errors.New("too many symlinks")

// Tree is a sealed filesystem. An immutable lower permits concurrent readers
// without synchronization. Lower filesystems MUST remain read-only for the
// tree lifetime. Public slices are copies.
type Tree struct{ view }

type view struct {
	nodes   map[string]*node
	lower   *Tree
	removed map[string]struct{}
	input   *foreignInput
}

// An extra indirection keeps foreign I/O escape analysis off indexed maps.
// Only private builders memoize metadata; sealed reads never mutate it.
type foreignInput struct {
	lower  fs.FS
	memo   map[string]*node
	mounts map[string]fs.FS
}

type node struct {
	name     string
	mode     fs.FileMode
	data     []byte
	target   string
	children []fs.DirEntry
	foreign  *foreignNode
}

type foreignNode struct {
	source fs.FS
	path   string
	info   fs.FileInfo
	upper  bool
	mount  string
}

func (n *node) Name() string      { return n.name }
func (n *node) Mode() fs.FileMode { return n.mode }
func (n *node) Type() fs.FileMode { return n.mode.Type() }
func (n *node) IsDir() bool       { return n.mode.IsDir() }
func (n *node) ModTime() time.Time {
	if n.foreign != nil {
		return n.foreign.info.ModTime()
	}
	return time.Time{}
}
func (n *node) Sys() any                   { return nil }
func (n *node) Info() (fs.FileInfo, error) { return n, nil }
func (n *node) Size() int64 {
	if n.foreign != nil {
		return n.foreign.info.Size()
	}
	if n.mode&fs.ModeSymlink != 0 {
		return int64(len(n.target))
	}
	return int64(len(n.data))
}

func (t *view) lookup(name string) (*node, error) {
	if n := t.nodes[name]; n != nil {
		return n, nil
	}
	if t.lower == nil && t.input == nil {
		return nil, fs.ErrNotExist
	}
	if t.masked(name) {
		return nil, fs.ErrNotExist
	}
	if t.input != nil {
		prefix, input := t.mountedInput(name)
		if input != nil {
			rel := strings.TrimPrefix(name, prefix+"/")
			return readForeignNode(input, name, rel, prefix)
		}
	}

	if t.lower != nil {
		return t.lower.lookup(name)
	}
	if t.input == nil || t.input.lower == nil {
		return nil, fs.ErrNotExist
	}
	if n := t.input.memo[name]; n != nil {
		return n, nil
	}
	n, err := readForeignNode(t.input.lower, name, name, "")
	if err == nil && t.input.memo != nil {
		t.input.memo[name] = n
	}
	return n, err
}

func (t *view) masked(name string) bool {
	for p := name; ; p = path.Dir(p) {
		if _, ok := t.removed[p]; ok {
			return true
		}
		if p == "." {
			break
		}
	}
	return false
}

// Resolve follows links before subsequent parent components and returns a
// canonical slash path. Unlike the io/fs methods, it accepts unclean relative
// paths. Links stay confined to the tree.
func (t *view) Resolve(name string) (string, error) {
	p, _, err := t.resolve(name, true)
	if err != nil {
		return "", &fs.PathError{Op: "resolve", Path: name, Err: err}
	}
	return p, nil
}

func (t *view) resolve(name string, follow bool) (string, *node, error) {
	if strings.ContainsRune(name, 0) || name == "" {
		return "", nil, fs.ErrInvalid
	}
	if path.IsAbs(name) {
		return "", nil, ErrEscape
	}
	// Indexed memory nodes cannot have unresolved intermediate links.
	if fs.ValidPath(name) {
		if n := t.nodes[name]; n != nil && (!follow || n.mode&fs.ModeSymlink == 0) {
			return name, n, nil
		}
	}
	// Construction never creates descendants of links. An indexed node cannot
	// have an unresolved intermediate link, even in a merged overlay.
	if fs.ValidPath(name) && (t.lower != nil || t.input == nil) {
		if n, err := t.lookup(name); err == nil && (!follow || n.mode&fs.ModeSymlink == 0) {
			return name, n, nil
		} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", nil, err
		}
	}
	r := pathResolution{view: t, current: ".", remaining: name}
	return r.resolve(follow)
}

type pathResolution struct {
	view      *view
	current   string
	remaining string
	node      *node
	hops      int
}

func (r *pathResolution) resolve(follow bool) (string, *node, error) {
	var err error
	r.node, err = r.view.lookup(r.current)
	if err != nil {
		return "", nil, err
	}
	for r.remaining != "" {
		part, rest, slash := strings.Cut(r.remaining, "/")
		r.remaining = rest
		switch part {
		case "", ".":
			continue
		case "..":
			if r.current == "." {
				return "", nil, ErrEscape
			}
			r.current = path.Dir(r.current)
			r.node, err = r.view.lookup(r.current)
			if err != nil {
				return "", nil, err
			}
			continue
		}
		if advanceErr := r.advance(part, rest, slash, follow); advanceErr != nil {
			return "", nil, advanceErr
		}
	}
	if r.node == nil {
		return "", nil, fs.ErrNotExist
	}
	return r.current, r.node, nil
}

func (r *pathResolution) advance(part, rest string, slash, follow bool) error {
	next := part
	if r.current != "." {
		next = r.current + "/" + part
	}
	var err error
	r.node, err = r.view.lookup(next)
	if err != nil {
		return err
	}
	if r.node.mode&fs.ModeSymlink != 0 && (follow || slash) {
		return r.followLink(rest, slash)
	}
	if slash && !r.node.IsDir() {
		return fs.ErrInvalid
	}
	r.current = next
	return nil
}

func (r *pathResolution) followLink(rest string, slash bool) error {
	if path.IsAbs(r.node.target) {
		return ErrEscape
	}
	r.hops++
	if r.hops > maxSymbolicLinkDepth {
		return ErrLoop
	}
	r.remaining = r.node.target
	if slash {
		r.remaining += "/" + rest
	}
	if r.node.target == "" {
		return fs.ErrNotExist
	}
	var err error
	r.node, err = r.view.lookup(r.current)
	return err
}

func (t *view) readNode(op, name string, follow bool) (*node, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	p, n, err := t.resolve(name, follow)
	if err != nil {
		return nil, &fs.PathError{Op: op, Path: name, Err: err}
	}
	if p != name {
		info := *n
		info.name = path.Base(name)
		n = &info
	}
	return n, nil
}

// Open returns a read-only handle with an independent cursor.
func (t *view) Open(name string) (fs.File, error) {
	n, err := t.readNode("open", name, true)
	if err != nil {
		return nil, err
	}
	if n.foreign != nil {
		if !n.IsDir() {
			//nolint:wrapcheck // The io/fs adapter preserves upstream errors and their PathError structure.
			return n.foreign.source.Open(n.foreign.path)
		}
		entries, err := t.entries(n)
		if err != nil {
			return nil, err
		}
		opened := *n
		opened.children = entries
		opened.foreign = nil
		n = &opened
	}
	return &file{node: n, name: name, reader: *bytes.NewReader(n.data)}, nil
}

// ReadFile follows links and returns caller-owned bytes.
func (t *view) ReadFile(name string) ([]byte, error) {
	n, err := t.readNode("readfile", name, true)
	if err != nil {
		return nil, err
	}
	if n.IsDir() {
		return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrInvalid}
	}
	if n.foreign != nil {
		//nolint:wrapcheck // The io/fs adapter preserves upstream errors and their PathError structure.
		return fs.ReadFile(n.foreign.source, n.foreign.path)
	}
	return bytes.Clone(n.data), nil
}

// ReadDir returns caller-owned entries sorted by name, following a link root.
func (t *view) ReadDir(name string) ([]fs.DirEntry, error) {
	n, err := t.readNode(readDirOperation, name, true)
	if err != nil {
		return nil, err
	}
	if !n.IsDir() {
		return nil, &fs.PathError{Op: readDirOperation, Path: name, Err: fs.ErrInvalid}
	}
	entries, err := t.entries(n)
	return slices.Clone(entries), err
}

func (t *view) entries(n *node) ([]fs.DirEntry, error) {
	if n.foreign == nil || (!n.foreign.upper && n.children != nil) {
		return n.children, nil
	}
	if t.lower != nil && t.nodes[path.Join(n.foreign.mount, n.foreign.path)] == nil {
		return t.lower.entries(n)
	}
	var memo map[string]*node
	if t.input != nil && n.foreign.mount == "" {
		memo = t.input.memo
	}
	var entries []fs.DirEntry
	if cached := memo[n.foreign.path]; cached != nil {
		entries = cached.children
	}
	if entries == nil {
		var err error
		entries, err = fs.ReadDir(n.foreign.source, n.foreign.path)
		if err != nil {
			//nolint:wrapcheck // The io/fs adapter preserves upstream errors and their PathError structure.
			return nil, err
		}
		if cached := memo[n.foreign.path]; cached != nil {
			cached.children = entries
		}
	}
	if !n.foreign.upper {
		return entries, nil
	}
	merged := make([]fs.DirEntry, 0, len(entries)+len(n.children))
	for _, e := range entries {
		if len(t.removed) != 0 && t.masked(path.Join(n.foreign.mount, n.foreign.path, e.Name())) {
			continue
		}
		if _, exists := slices.BinarySearchFunc(n.children, e.Name(), compareEntry); !exists {
			merged = append(merged, e)
		}
	}
	merged = append(merged, n.children...)
	slices.SortFunc(merged, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return merged, nil
}

func compareEntry(e fs.DirEntry, name string) int { return strings.Compare(e.Name(), name) }

// Stat follows the final link.
func (t *view) Stat(name string) (fs.FileInfo, error) {
	n, err := t.readNode("stat", name, true)
	if err != nil {
		return nil, err
	}
	return n, nil
}

// Lstat follows intermediate links but leaves the final link visible.
func (t *view) Lstat(name string) (fs.FileInfo, error) {
	n, err := t.readNode("lstat", name, false)
	if err != nil {
		return nil, err
	}
	return n, nil
}

// ReadLink returns the final link's target without resolving it.
func (t *view) ReadLink(name string) (string, error) {
	n, err := t.readNode("readlink", name, false)
	if err != nil {
		return "", err
	}
	if n.mode&fs.ModeSymlink == 0 {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	return n.target, nil
}

// WalkRaw walks lexical nodes in order without following a link root or link
// entries. SkipDir prunes the single merged directory index.
func (t *view) WalkRaw(name string, fn fs.WalkDirFunc) error {
	n, err := t.readNode("walk", name, false)
	if err != nil {
		err = fn(name, nil, err)
	} else {
		err = t.walk(name, n, fn)
	}
	if errors.Is(err, fs.SkipDir) || errors.Is(err, fs.SkipAll) {
		return nil
	}
	return err
}

func (t *view) walk(name string, n *node, fn fs.WalkDirFunc) error {
	if err := fn(name, n, nil); err != nil || !n.IsDir() {
		if errors.Is(err, fs.SkipDir) && n.IsDir() {
			return nil
		}
		return err
	}
	entries := n.children
	if n.foreign != nil {
		var err error
		entries, err = t.entries(n)
		if err != nil {
			return fn(name, n, err)
		}
	}
	return t.walkChildren(name, entries, fn)
}

func (t *view) walkChildren(name string, entries []fs.DirEntry, fn fs.WalkDirFunc) error {
	for _, entry := range entries {
		p := entry.Name()
		if name != "." {
			p = name + "/" + p
		}
		child, ok := entry.(*node)
		if !ok {
			var err error
			child, err = t.lookup(p)
			if err != nil {
				if fnErr := fn(p, entry, err); fnErr != nil {
					return fnErr
				}
				continue
			}
		}
		if err := t.walk(p, child, fn); err != nil {
			if errors.Is(err, fs.SkipDir) {
				break
			}
			return err
		}
	}
	return nil
}

const (
	addFileOperation    = "addfile"
	addSymlinkOperation = "addsymlink"
	readDirOperation    = "readdir"
)

const (
	maxSymbolicLinkDepth = 8
)

func (t *view) mountedInput(name string) (string, fs.FS) {
	for prefix := path.Dir(name); ; prefix = path.Dir(prefix) {
		input := t.input.mounts[prefix]
		if input != nil {
			return prefix, input
		}
		if prefix == "." {
			return "", nil
		}
	}
}

func readForeignNode(input fs.FS, name, rel, mount string) (*node, error) {
	info, err := fs.Lstat(input, rel)
	if err != nil {
		//nolint:wrapcheck // The io/fs adapter preserves upstream errors and their PathError structure.
		return nil, err
	}
	n := &node{
		name:    path.Base(name),
		mode:    info.Mode(),
		foreign: &foreignNode{source: input, path: rel, info: info, mount: mount},
	}
	if n.mode&fs.ModeSymlink != 0 {
		n.target, err = fs.ReadLink(input, rel)
	}
	//nolint:wrapcheck // The io/fs adapter preserves upstream errors and their PathError structure.
	return n, err
}
