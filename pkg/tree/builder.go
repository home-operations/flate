package tree

import (
	"bytes"
	"errors"
	"io/fs"
	"path"
	"slices"
	"strings"
)

// ErrConflict identifies incompatible node kinds at a construction path.
var ErrConflict = errors.New("file/directory conflict")

// ErrSealed identifies an operation on a builder whose ownership was transferred.
var ErrSealed = errors.New("builder is sealed")

// Builder owns mutable construction state and must not be copied or shared
// between renders or source producers. Its reads use the sealed Tree's resolver.
type Builder struct {
	view
	sealed bool
}

// NewBuilder creates a private overlay over lower, or an empty source builder
// when lower is nil. Only upper directory indexes and inserted bytes are copied.
func NewBuilder(lower *Tree) *Builder {
	b := &Builder{
		nodes:   make(map[string]*node),
		lower:   lower,
		removed: make(map[string]struct{})}
	if lower == nil {
		b.nodes["."] = &node{name: ".", mode: fs.ModeDir | 0o755}
	}
	return b
}

func (b *Builder) normalize(op, name string) (string, error) {
	if b.sealed {
		return "", &fs.PathError{Op: op, Path: name, Err: ErrSealed}
	}
	p := path.Clean(name)
	if name == "" || strings.ContainsRune(name, 0) || !fs.ValidPath(p) {
		return "", &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return p, nil
}

func (b *Builder) directory(name string) (*node, error) {
	if n := b.nodes[name]; n != nil {
		if !n.IsDir() {
			return nil, ErrConflict
		}
		return n, nil
	}
	n := &node{name: path.Base(name), mode: fs.ModeDir | 0o755}
	if lower := b.lookup(name); lower != nil {
		if !lower.IsDir() {
			return nil, ErrConflict
		}
		*n = *lower
		n.children = slices.Clone(lower.children)
	}
	if err := b.put(name, n); err != nil {
		return nil, err
	}
	return n, nil
}

func (b *Builder) put(name string, n *node) error {
	if name != "." {
		parent, err := b.directory(path.Dir(name))
		if err != nil {
			return err
		}
		i, exists := slices.BinarySearchFunc(parent.children, n.name, func(e fs.DirEntry, name string) int {
			return strings.Compare(e.Name(), name)
		})
		if exists {
			parent.children[i] = n
		} else {
			parent.children = slices.Insert(parent.children, i, fs.DirEntry(n))
		}
	}
	b.nodes[name] = n
	return nil
}

// AddDir creates a directory and missing parents. A file or link at that path
// or any parent is a conflict; replacing a kind requires RemoveAll first.
func (b *Builder) AddDir(name string, mode fs.FileMode) error {
	p, err := b.normalize("adddir", name)
	if err != nil {
		return err
	}
	if mode.Type() != 0 && mode.Type() != fs.ModeDir {
		return &fs.PathError{Op: "adddir", Path: name, Err: fs.ErrInvalid}
	}
	n, err := b.directory(p)
	if err != nil {
		return &fs.PathError{Op: "adddir", Path: name, Err: err}
	}
	n.mode = mode | fs.ModeDir
	return nil
}

// AddFile copies data once. Duplicate files are last-wins; directories and
// symlinks are conflicts until removed explicitly.
func (b *Builder) AddFile(name string, data []byte, mode fs.FileMode) error {
	p, err := b.normalize("addfile", name)
	if err != nil {
		return err
	}
	if p == "." || mode.Type() != 0 {
		return &fs.PathError{Op: "addfile", Path: name, Err: fs.ErrInvalid}
	}
	if n := b.lookup(p); n != nil && n.mode.Type() != 0 {
		return &fs.PathError{Op: "addfile", Path: name, Err: ErrConflict}
	}
	n := &node{name: path.Base(p), mode: mode, data: bytes.Clone(data)}
	if err := b.put(p, n); err != nil {
		return &fs.PathError{Op: "addfile", Path: name, Err: err}
	}
	return nil
}

// AddSymlink stores a lexical target, including dangling, absolute and escaping
// targets. Follow operations enforce confinement; raw scans do not follow links.
func (b *Builder) AddSymlink(name, target string) error {
	p, err := b.normalize("addsymlink", name)
	if err != nil {
		return err
	}
	if p == "." || strings.ContainsRune(target, 0) {
		return &fs.PathError{Op: "addsymlink", Path: name, Err: fs.ErrInvalid}
	}
	if n := b.lookup(p); n != nil && n.mode&fs.ModeSymlink == 0 {
		return &fs.PathError{Op: "addsymlink", Path: name, Err: ErrConflict}
	}
	n := &node{name: path.Base(p), mode: fs.ModeSymlink | 0o777, target: target}
	if err := b.put(p, n); err != nil {
		return &fs.PathError{Op: "addsymlink", Path: name, Err: err}
	}
	return nil
}

// RemoveAll masks a subtree without mutating the lower tree. Recreated nodes
// never expose lower descendants of that subtree.
func (b *Builder) RemoveAll(name string) error {
	p, err := b.normalize("removeall", name)
	if err != nil {
		return err
	}
	for key := range b.nodes {
		if p == "." || key == p || strings.HasPrefix(key, p+"/") {
			delete(b.nodes, key)
		}
	}
	b.removed[p] = struct{}{}
	if p == "." {
		b.nodes["."] = &node{name: ".", mode: fs.ModeDir | 0o755}
		return nil
	}
	parentPath := path.Dir(p)
	if n := b.lookup(parentPath); n == nil || !n.IsDir() {
		return nil
	}
	parent, err := b.directory(parentPath)
	if err != nil {
		return &fs.PathError{Op: "removeall", Path: name, Err: err}
	}
	i, exists := slices.BinarySearchFunc(parent.children, path.Base(p), func(e fs.DirEntry, name string) int {
		return strings.Compare(e.Name(), name)
	})
	if exists {
		parent.children = slices.Delete(parent.children, i, i+1)
	}
	return nil
}

// Build transfers ownership to an immutable Tree and seals the builder.
func (b *Builder) Build() (*Tree, error) {
	if b.sealed {
		return nil, &fs.PathError{Op: "build", Path: ".", Err: ErrSealed}
	}
	b.sealed = true
	t := &Tree{view: b.view}
	b.view = view{}
	return t, nil
}
