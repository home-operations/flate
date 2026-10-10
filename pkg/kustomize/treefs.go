package kustomize

import (
	"bytes"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"time"

	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/pkg/tree"
)

// treeFS adapts the common resolver to kyaml's OS mount names. Mutable state
// belongs to one render; only the sealed lower tree is shared.
type treeFS struct {
	upper   *tree.Builder
	lower   fs.FS
	root    string
	rec     *readSet
	changed map[string]struct{}
}

func newTreeFS(lower fs.FS, root string, rec *readSet) *treeFS {
	t := &treeFS{upper: tree.NewBuilder(lower), lower: lower, root: root, rec: rec}
	if rec != nil {
		t.changed = make(map[string]struct{})
	}
	return t
}

// fromKyaml strips only a component-boundary mount prefix. Parent components
// stay unresolved until tree.Resolve follows any preceding link.
func fromKyaml(root, name string, separator byte) (string, error) {
	if separator == '\\' {
		root = strings.ReplaceAll(root, `\`, "/")
		name = strings.ReplaceAll(name, `\`, "/")
	}
	root = strings.TrimRight(root, "/")
	if name == root {
		return ".", nil
	}
	if len(name) > len(root) && strings.HasPrefix(name, root) && name[len(root)] == '/' {
		rel := name[len(root)+1:]
		if rel == "" {
			return ".", nil
		}
		return rel, nil
	}
	if strings.HasPrefix(name, "/") || (separator == '\\' && len(name) > 1 && name[1] == ':') {
		return "", &fs.PathError{Op: "mount", Path: name, Err: tree.ErrEscape}
	}
	return name, nil
}

func toKyaml(root, name string, separator byte) string {
	if name == "." {
		return root
	}
	if separator == '\\' {
		name = strings.ReplaceAll(name, "/", `\`)
	}
	return strings.TrimRight(root, string(separator)) + string(separator) + name
}

func treeError(op, name string, err error) error {
	if errors.Is(err, tree.ErrEscape) {
		return &fluxfilesys.ConstraintError{Op: op, Path: name, Err: err}
	}
	return err
}

func (t *treeFS) mount(name string) string {
	if d, ok := t.lower.(diskInput); ok {
		return d.mounted(name)
	}
	return toKyaml(t.root, name, filepath.Separator)
}

func (t *treeFS) rel(name string) (string, error) {
	rel, err := fromKyaml(t.root, name, filepath.Separator)
	return rel, treeError("mount", name, err)
}

func (t *treeFS) resolve(name string) (string, error) {
	rel, err := t.rel(name)
	if err == nil {
		rel, err = t.upper.Resolve(rel)
	}
	return rel, treeError("resolve", name, err)
}

// raw resolves intermediate links while retaining the final lexical node.
func (t *treeFS) raw(name string) (string, error) {
	rel, err := t.rel(name)
	if err != nil {
		return "", err
	}
	parent, leaf, found := strings.CutLast(rel, "/")
	if !found {
		return rel, nil
	}
	if parent == "" {
		parent = "."
	}
	parent, err = t.upper.Resolve(parent)
	if err != nil {
		return "", treeError("resolve", name, err)
	}
	if parent == "." {
		return leaf, nil
	}
	return parent + "/" + leaf, nil
}

func (t *treeFS) writePath(name string) (string, error) {
	rel, err := t.raw(name)
	if errors.Is(err, fs.ErrNotExist) {
		raw, rawErr := t.rel(name)
		if rawErr == nil && fs.ValidPath(raw) {
			return raw, nil
		}
	}
	return rel, err
}

func (t *treeFS) modified(rel string) bool {
	for p := rel; ; p = path.Dir(p) {
		if _, ok := t.changed[p]; ok {
			return true
		}
		if p == "." {
			return false
		}
	}
}

func (t *treeFS) mark(rel string) {
	if t.rec != nil {
		t.changed[rel] = struct{}{}
	}
}

func (t *treeFS) probe(name, rel string) {
	if t.rec == nil || t.modified(rel) {
		return
	}
	info, err := fs.Stat(t.lower, rel)
	t.rec.recordNode(name, diskKind(err == nil, err == nil && info.IsDir()))
}

func (t *treeFS) Exists(name string) bool {
	rel, err := t.resolve(name)
	if err != nil {
		if raw, e := t.rel(name); e == nil {
			t.probe(name, raw)
		}
		return false
	}
	t.probe(name, rel)
	return true
}

func (t *treeFS) IsDir(name string) bool {
	rel, err := t.resolve(name)
	if err != nil {
		if raw, e := t.rel(name); e == nil {
			t.probe(name, raw)
		}
		return false
	}
	t.probe(name, rel)
	info, err := t.upper.Stat(rel)
	return err == nil && info.IsDir()
}

func (t *treeFS) ReadFile(name string) ([]byte, error) {
	rel, err := t.resolve(name)
	if err != nil {
		return nil, err
	}
	data, err := t.upper.ReadFile(rel)
	if t.rec != nil && !t.modified(rel) {
		t.rec.recordFile(name, data, err)
	}
	return data, treeError("read", name, err)
}

type readOnlyFile struct {
	fs.File
	name string
}

type namedInfo struct {
	fs.FileInfo
	name string
}

func (i namedInfo) Name() string { return i.name }

func (f readOnlyFile) Stat() (fs.FileInfo, error) {
	info, err := f.File.Stat()
	if err == nil && info.Name() != f.name {
		return namedInfo{FileInfo: info, name: f.name}, nil
	}
	return info, err
}

func (f readOnlyFile) Write([]byte) (int, error) { return 0, fs.ErrPermission }

func (t *treeFS) Open(name string) (filesys.File, error) {
	rel, err := t.resolve(name)
	if err != nil {
		return nil, err
	}
	if t.rec != nil && !t.modified(rel) {
		data, err := fs.ReadFile(t.lower, rel)
		t.rec.recordFile(name, data, err)
	}
	file, err := t.upper.Open(rel)
	if err != nil {
		return nil, treeError("open", name, err)
	}
	return readOnlyFile{File: file, name: filepath.Base(name)}, nil
}

func (t *treeFS) CleanedAbs(name string) (filesys.ConfirmedDir, string, error) {
	rel, err := t.resolve(name)
	if err != nil {
		return "", "", err
	}
	t.probe(name, rel)
	info, err := t.upper.Stat(rel)
	if err != nil {
		return "", "", treeError("stat", name, err)
	}
	if info.IsDir() {
		return filesys.ConfirmedDir(t.mount(rel)), "", nil
	}
	return filesys.ConfirmedDir(t.mount(path.Dir(rel))), path.Base(rel), nil
}

func entryNames(entries []fs.DirEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}

func (t *treeFS) ReadDir(name string) ([]string, error) {
	rel, err := t.resolve(name)
	if err != nil {
		return nil, err
	}
	entries, err := t.upper.ReadDir(rel)
	if t.rec != nil {
		if lower, e := fs.ReadDir(t.lower, rel); e == nil {
			t.rec.recordDir(name, entryNames(lower))
		}
	}
	return entryNames(entries), treeError("readdir", name, err)
}

func (t *treeFS) Glob(pattern string) ([]string, error) {
	if t.rec != nil {
		t.rec.markBypass()
	}
	rel, err := t.rel(pattern)
	if err != nil {
		return nil, err
	}
	matches, err := t.upper.Glob(rel)
	for i, name := range matches {
		matches[i] = t.mount(name)
	}
	return matches, treeError("glob", pattern, err)
}

func (t *treeFS) Walk(name string, fn filepath.WalkFunc) error {
	rel, err := t.raw(name)
	if err != nil {
		return err
	}
	return t.upper.WalkRaw(rel, func(p string, entry fs.DirEntry, err error) error {
		mounted := t.mount(p)
		if err != nil {
			return fn(mounted, nil, err)
		}
		if _, err := t.upper.Resolve(p); errors.Is(err, tree.ErrEscape) {
			return treeError("walk", mounted, err)
		}
		info, err := entry.Info()
		if err == nil && info.IsDir() && t.rec != nil {
			if entries, e := fs.ReadDir(t.lower, p); e == nil {
				t.rec.recordDir(mounted, entryNames(entries))
			}
		}
		return fn(mounted, info, err)
	})
}

func (t *treeFS) WriteFile(name string, data []byte) error {
	rel, err := t.writePath(name)
	if err != nil {
		return err
	}
	if err := t.upper.AddFile(rel, data, 0o644); err != nil {
		return treeError("write", name, err)
	}
	t.mark(path.Clean(rel))
	return nil
}

func (t *treeFS) Mkdir(name string) error {
	rel, err := t.writePath(name)
	if err != nil {
		return err
	}
	if _, err := t.upper.Lstat(rel); err == nil {
		return &fs.PathError{Op: "mkdir", Path: name, Err: fs.ErrExist}
	}
	if _, err := t.upper.Stat(path.Dir(rel)); err != nil {
		return err
	}
	return treeError("mkdir", name, t.upper.AddDir(rel, 0o755))
}

func (t *treeFS) MkdirAll(name string) error {
	rel, err := t.writePath(name)
	if err != nil {
		return err
	}
	return treeError("mkdir", name, t.upper.AddDir(rel, 0o755))
}

func (t *treeFS) RemoveAll(name string) error {
	rel, err := t.writePath(name)
	if err != nil {
		return err
	}
	if err := t.upper.RemoveAll(rel); err != nil {
		return treeError("remove", name, err)
	}
	t.mark(path.Clean(rel))
	return nil
}

type writeFile struct {
	data   bytes.Buffer
	tree   *treeFS
	name   string
	closed bool
}

func (f *writeFile) Close() error {
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	return f.tree.WriteFile(f.name, f.data.Bytes())
}

type writeInfo struct {
	name string
	size int64
}

func (i writeInfo) Name() string       { return i.name }
func (i writeInfo) Size() int64        { return i.size }
func (i writeInfo) Mode() fs.FileMode  { return 0o644 }
func (i writeInfo) ModTime() time.Time { return time.Time{} }
func (i writeInfo) IsDir() bool        { return false }
func (i writeInfo) Sys() any           { return nil }

func (f *writeFile) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, fs.ErrClosed
	}
	return writeInfo{name: filepath.Base(f.name), size: int64(f.data.Len())}, nil
}
func (f *writeFile) Write(data []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.data.Write(data)
}
func (f *writeFile) Read(data []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.data.Read(data)
}

func (t *treeFS) Create(name string) (filesys.File, error) {
	if _, err := t.writePath(name); err != nil {
		return nil, err
	}
	return &writeFile{tree: t, name: name}, nil
}
