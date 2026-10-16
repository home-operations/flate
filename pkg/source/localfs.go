package source

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"

	fluxignore "github.com/fluxcd/pkg/sourceignore"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/tree"
)

// NewLocalFS admits a mutable local input, omitting links and nonregular files.
// Root coordinates are canonicalized once; ignore policy is lazy per admission.
func NewLocalFS(ctx context.Context, root string) (fs.FS, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("%w: local source %s: %w", manifest.ErrInput, root, err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return nil, fmt.Errorf("%w: local source root: %w", manifest.ErrInput, err)
	}
	l := &localFS{root: resolved}
	l.ignored = sync.OnceValues(func() (localIgnore, error) {
		matcher, err := sourceignore.New(resolved, nil, true)
		if err != nil {
			return localIgnore{}, err
		}
		var content []struct {
			Path string
			Data []byte
		}
		err = filepath.WalkDir(resolved, func(name string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name != resolved && d.Name() == ".git" {
					return filepath.SkipDir
				}
				return nil
			}
			if d.Name() != fluxignore.IgnoreFile {
				return nil
			}
			rel, err := filepath.Rel(resolved, name)
			if err != nil {
				return err
			}
			data, err := os.ReadFile(name) //nolint:gosec // name is an ignore file found within the admitted root
			if err != nil {
				return err
			}
			content = append(content, struct {
				Path string
				Data []byte
			}{filepath.ToSlash(rel), data})
			return nil
		})
		if err != nil {
			return localIgnore{}, fmt.Errorf("sourceignore fingerprint: %w", err)
		}
		return localIgnore{matcher: matcher, content: content}, nil
	})
	return l, nil
}

// ReadView creates private observations for one local consumer operation.
// The content is an ordered local ignore snapshot, or the immutable acquisition
// identity of a cache-owned view. Re-admission refreshes local ignore policy.
func ReadView(ctx context.Context, input fs.FS, applyIgnore bool) (fs.FS, any, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	l, ok := input.(*localFS)
	if !ok {
		if git, ok := input.(*gittree.FS); ok {
			refreshed, pin, err := gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: git})
			return refreshed, pin, err
		}
		return input, cache.AcquisitionIdentity(input), nil
	}
	v := &localReadView{root: l.root, dirs: make(map[string]localDirectory)}
	if !applyIgnore {
		return v, nil, nil
	}
	policy, err := l.ignored()
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	v.matcher, v.hiddenDirs = policy.matcher, make(map[string]bool)
	return v, policy.content, nil
}

type localFS struct {
	root    string
	ignored func() (localIgnore, error)
}

// Descriptor calls have ephemeral observations so the descriptor is shareable.
func (l *localFS) Open(name string) (fs.File, error) {
	v := &localReadView{root: l.root}
	return v.Open(name)
}

func (l *localFS) Stat(name string) (fs.FileInfo, error) {
	v := &localReadView{root: l.root}
	return v.Stat(name)
}

type localIgnore struct {
	matcher *sourceignore.Matcher
	content any
}

type localReadView struct {
	root       string
	matcher    *sourceignore.Matcher
	dirs       map[string]localDirectory
	hiddenDirs map[string]bool
	rootInfo   fs.FileInfo
}

type localDirectory struct {
	entries []fs.DirEntry
	err     error
}

type localEntry struct {
	fs.DirEntry
	info fs.FileInfo
	err  error
	read bool
}

func (e *localEntry) Info() (fs.FileInfo, error) {
	if !e.read {
		e.info, e.err = e.DirEntry.Info()
		e.read = true
	}
	return e.info, e.err
}

func (v *localReadView) filename(name string) string {
	return filepath.Join(v.root, filepath.FromSlash(name))
}

// Directory observations, including misses, belong only to this operation.
func (v *localReadView) directory(name string) ([]fs.DirEntry, error) {
	if observed, ok := v.dirs[name]; ok {
		return observed.entries, observed.err
	}
	if name != "." {
		entry, err := v.entry(name)
		if err != nil {
			return nil, err
		}
		if !entry.IsDir() {
			return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
		}
	}
	entries, err := os.ReadDir(v.filename(name))
	kept := entries[:0]
	for _, e := range entries {
		if e.Type().IsRegular() || e.IsDir() {
			kept = append(kept, &localEntry{DirEntry: e})
		}
	}
	if v.dirs != nil {
		v.dirs[name] = localDirectory{entries: kept, err: err}
	}
	return kept, err
}

func (v *localReadView) entry(name string) (fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	if v.dirs == nil {
		parent := path.Dir(name)
		info, err := v.Stat(parent)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, &fs.PathError{Op: "readdir", Path: parent, Err: fs.ErrInvalid}
		}
		info, err = os.Lstat(v.filename(name))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
		}
		// Denied ancestor searches retain directory-read error coordinates.
		if errors.Is(err, fs.ErrPermission) {
			return nil, &fs.PathError{Op: "open", Path: v.filename(parent), Err: errors.Unwrap(err)}
		}
		if err != nil {
			return nil, &fs.PathError{Op: "stat", Path: name, Err: errors.Unwrap(err)}
		}
		if !info.Mode().IsRegular() && !info.IsDir() {
			return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
		}
		return fs.FileInfoToDirEntry(info), nil
	}
	entries, err := v.directory(path.Dir(name))
	if err != nil {
		return nil, err
	}
	i, found := slices.BinarySearchFunc(entries, path.Base(name), func(e fs.DirEntry, name string) int {
		return cmp.Compare(e.Name(), name)
	})
	if !found {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return entries[i], nil
}

// Matched directories survive when a negated rule retains a regular descendant.
// Survivor walks use the same link-omitting directory observations as reads.
func (v *localReadView) hidden(name string, isDir bool) bool {
	if v.matcher == nil || name == "." || !v.matcher.Match(name, isDir) {
		return false
	}
	if !isDir {
		return true
	}
	if hidden, ok := v.hiddenDirs[name]; ok {
		return hidden
	}
	hidden := !v.hasSurvivor(name)
	v.hiddenDirs[name] = hidden
	return hidden
}

func (v *localReadView) hasSurvivor(name string) bool {
	entries, err := v.directory(name)
	if err != nil {
		return false
	}
	for _, e := range entries {
		child := path.Join(name, e.Name())
		if e.IsDir() {
			if v.hasSurvivor(child) {
				return true
			}
		} else if !v.matcher.Match(child, false) {
			return true
		}
	}
	return false
}

func (v *localReadView) admitted(name string) (fs.DirEntry, error) {
	e, err := v.entry(name)
	if err != nil {
		return nil, err
	}
	for p := name; p != "."; p = path.Dir(p) {
		if v.hidden(p, p != name || e.IsDir()) {
			return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
		}
	}
	return e, nil
}

func (v *localReadView) Stat(name string) (fs.FileInfo, error) {
	if name == "." {
		if v.rootInfo == nil {
			var err error
			v.rootInfo, err = os.Stat(v.root)
			if err != nil {
				return nil, err
			}
		}
		return v.rootInfo, nil
	}
	e, err := v.admitted(name)
	if err != nil {
		return nil, err
	}
	return e.Info()
}

func (v *localReadView) Open(name string) (fs.File, error) {
	if name == "." {
		return tree.NewBuilder(v).Open(name)
	}
	e, err := v.admitted(name)
	if err != nil {
		return nil, err
	}
	if e.IsDir() {
		return tree.NewBuilder(v).Open(name)
	}
	return os.Open(v.filename(name))
}

func (v *localReadView) ReadFile(name string) ([]byte, error) {
	if _, err := v.admitted(name); err != nil {
		return nil, err
	}
	return os.ReadFile(v.filename(name))
}

func (v *localReadView) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	if name != "." {
		if _, err := v.admitted(name); err != nil {
			return nil, err
		}
	}
	entries, err := v.directory(name)
	if err != nil || v.matcher == nil {
		return entries, err
	}
	kept := make([]fs.DirEntry, 0, len(entries))
	for _, e := range entries {
		if !v.hidden(path.Join(name, e.Name()), e.IsDir()) {
			kept = append(kept, e)
		}
	}
	return kept, nil
}
