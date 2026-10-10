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

// Tree is a sealed filesystem. Its bytes and directory indexes are immutable,
// so readers may share it without synchronization. Public slices are copies.
type Tree struct{ view }

type view struct {
	nodes   map[string]*node
	lower   *Tree
	removed map[string]struct{}
}

type node struct {
	name     string
	mode     fs.FileMode
	data     []byte
	target   string
	children []fs.DirEntry
}

func (n *node) Name() string               { return n.name }
func (n *node) Mode() fs.FileMode          { return n.mode }
func (n *node) Type() fs.FileMode          { return n.mode.Type() }
func (n *node) IsDir() bool                { return n.mode.IsDir() }
func (n *node) ModTime() time.Time         { return time.Time{} }
func (n *node) Sys() any                   { return nil }
func (n *node) Info() (fs.FileInfo, error) { return n, nil }
func (n *node) Size() int64 {
	if n.mode&fs.ModeSymlink != 0 {
		return int64(len(n.target))
	}
	return int64(len(n.data))
}

func (t *view) lookup(name string) *node {
	if n := t.nodes[name]; n != nil {
		return n
	}
	if t.lower == nil {
		return nil
	}
	for p := name; ; p = path.Dir(p) {
		if _, ok := t.removed[p]; ok {
			return nil
		}
		if p == "." {
			break
		}
	}
	return t.lower.lookup(name)
}

// Resolve follows links before subsequent parent components and returns a
// canonical slash path. Unlike the io/fs methods, it accepts unclean relative
// paths. It never consults the host filesystem.
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
	// Construction never creates descendants of links. An indexed node cannot
	// have an unresolved intermediate link, even in a merged overlay.
	if fs.ValidPath(name) {
		if n := t.lookup(name); n != nil && (!follow || n.mode&fs.ModeSymlink == 0) {
			return name, n, nil
		}
	}
	current := "."
	n := t.lookup(current)
	hops := 0
	for remaining := name; remaining != ""; {
		part, rest, slash := strings.Cut(remaining, "/")
		remaining = rest
		switch part {
		case "", ".":
			continue
		case "..":
			if current == "." {
				return "", nil, ErrEscape
			}
			current = path.Dir(current)
			n = t.lookup(current)
			continue
		}
		next := part
		if current != "." {
			next = current + "/" + part
		}
		n = t.lookup(next)
		if n == nil {
			return "", nil, fs.ErrNotExist
		}
		if n.mode&fs.ModeSymlink != 0 && (follow || slash) {
			if path.IsAbs(n.target) {
				return "", nil, ErrEscape
			}
			hops++
			if hops > 8 {
				return "", nil, ErrLoop
			}
			remaining = n.target
			if slash {
				remaining += "/" + rest
			}
			if n.target == "" {
				return "", nil, fs.ErrNotExist
			}
			n = t.lookup(current)
			continue
		}
		if slash && !n.IsDir() {
			return "", nil, fs.ErrInvalid
		}
		current = next
	}
	if n == nil {
		return "", nil, fs.ErrNotExist
	}
	return current, n, nil
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
	return bytes.Clone(n.data), nil
}

// ReadDir returns caller-owned entries sorted by name, following a link root.
func (t *view) ReadDir(name string) ([]fs.DirEntry, error) {
	n, err := t.readNode("readdir", name, true)
	if err != nil {
		return nil, err
	}
	if !n.IsDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	return slices.Clone(n.children), nil
}

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

// Glob returns lexical matches in order, omitting escaping links. Dangling and
// looping links remain matches; opening them reports the resolution error.
func (t *view) Glob(pattern string) ([]string, error) {
	matches, err := fs.Glob(globFS{t: t}, pattern)
	if err != nil {
		return nil, &fs.PathError{Op: "glob", Path: pattern, Err: err}
	}
	return slices.DeleteFunc(matches, func(name string) bool {
		_, err := t.Resolve(name)
		return errors.Is(err, ErrEscape)
	}), nil
}

type globFS struct{ t *view }

func (g globFS) Open(name string) (fs.File, error)          { return g.t.Open(name) }
func (g globFS) Stat(name string) (fs.FileInfo, error)      { return g.t.Lstat(name) }
func (g globFS) ReadDir(name string) ([]fs.DirEntry, error) { return g.t.ReadDir(name) }

// WalkRaw walks lexical nodes in order without following a link root or link
// entries. SkipDir prunes the single merged directory index.
func (t *view) WalkRaw(name string, fn fs.WalkDirFunc) error {
	n, err := t.readNode("walk", name, false)
	if err != nil {
		err = fn(name, nil, err)
	} else {
		err = t.walk(name, n, fn)
	}
	if err == fs.SkipDir || err == fs.SkipAll {
		return nil
	}
	return err
}

func (t *view) walk(name string, n *node, fn fs.WalkDirFunc) error {
	if err := fn(name, n, nil); err != nil || !n.IsDir() {
		if err == fs.SkipDir && n.IsDir() {
			return nil
		}
		return err
	}
	for _, entry := range n.children {
		p := entry.Name()
		if name != "." {
			p = name + "/" + p
		}
		if err := t.walk(p, entry.(*node), fn); err != nil {
			if err == fs.SkipDir {
				break
			}
			return err
		}
	}
	return nil
}
