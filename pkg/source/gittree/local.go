package gittree

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// AdmitDirectory constructs the direct input artifact at a normalized local root.
// Synthetic source labels MUST use their already admitted filesystem instead.
func AdmitDirectory(ctx context.Context, root string) (*store.SourceArtifact, error) {
	input, _, err := New(ctx, nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Worktree: osfs.New(root, osfs.WithBoundOS())})
	if err != nil {
		return nil, err
	}
	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "file://" + root, LocalPath: root, LocalRoot: root, FS: input}, nil
}

type localView struct {
	root        string
	directories sync.Map
}

// The public wrapper and mounted state MUST share one allocation on loader paths.
func newLocal(root string) *FS {
	input := &struct {
		FS
		localView
	}{root: root}
	input.view = &input.localView
	return &input.FS
}

func (g *localView) Stat(name string) (fs.FileInfo, error) {
	info, err := g.Lstat(name)
	if pathErr, ok := errors.AsType[*fs.PathError](err); ok {
		return info, &fs.PathError{Op: "stat", Path: pathErr.Path, Err: pathErr.Err}
	}
	return info, err
}

func (g *localView) ReadFile(name string) ([]byte, error) {
	f, err := g.Open(name)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	return data, errors.Join(err, f.Close())
}

func (g *localView) OpenRegular(name string) (fs.File, error) {
	abs, err := g.mounted(name)
	if err != nil {
		return nil, err
	}
	return os.Open(abs) //nolint:gosec // Callers MUST supply a regular file admitted from the stable checkout.
}

type localDirectory struct {
	listed bool
	stat   func() (fs.FileInfo, error)
}

func (g *localView) Lstat(name string) (fs.FileInfo, error) {
	abs, err := g.mounted(name)
	if err != nil {
		return nil, err
	}
	return g.localStat(name, abs)
}

func (g *localView) ReadDir(name string) ([]fs.DirEntry, error) {
	abs, err := g.mounted(name)
	if err != nil {
		return nil, err
	}
	if err := g.admitDirectory(name, abs); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(abs)
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != ".git" {
			g.directories.LoadOrStore(filepath.Join(abs, entry.Name()), &localDirectory{listed: true, stat: sync.OnceValues(entry.Info)})
		}
	}
	return slices.DeleteFunc(entries, func(entry fs.DirEntry) bool {
		return entry.Name() == ".git" || !entry.Type().IsRegular() && !entry.IsDir()
	}), err
}

func (g *localView) Open(name string) (fs.File, error) {
	abs, err := g.mounted(name)
	if err != nil {
		return nil, err
	}
	info, err := g.localStat(name, abs)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		entries, err := g.ReadDir(name)
		if err != nil {
			return nil, err
		}
		return &gitFile{info: info, input: g, entries: entries}, nil
	}
	return os.Open(abs) //nolint:gosec // Admission omits links; the checkout MUST remain stable during reads.
}

func (g *localView) ReadLink(name string) (string, error) {
	abs, err := g.mounted(name)
	if err != nil {
		return "", err
	}
	if _, err := g.localStat(name, abs); err != nil {
		return "", err
	}
	target, err := os.Readlink(abs)
	if err != nil {
		return "", err
	}
	return target, nil
}

func (g *localView) mounted(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	for parent := name; parent != "."; parent = path.Dir(parent) {
		if path.Base(parent) == ".git" {
			return "", &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
	}
	if name == "." {
		return g.root, nil
	}
	return filepath.Join(g.root, filepath.FromSlash(name)), nil
}

// Directory admission belongs to one view; readers MUST retain its snapshot
// assumption until a new view is constructed for the next operation.
func (g *localView) localStat(name, abs string) (fs.FileInfo, error) {
	var info fs.FileInfo
	var err error
	if cached, ok := g.directories.Load(abs); ok {
		info, err = cached.(*localDirectory).stat()
	} else {
		if parent := path.Dir(name); parent != "." {
			parentPath := parentPath(name, abs)
			if err := g.admitDirectory(parent, parentPath); err != nil {
				return nil, err
			}
		}
		info, err = os.Lstat(abs)
		if err == nil && info.IsDir() {
			g.directories.LoadOrStore(abs, &localDirectory{stat: func() (fs.FileInfo, error) { return info, nil }})
		}
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return info, nil
}

func (g *localView) admitDirectory(name, abs string) error {
	cached, ok := g.directories.Load(abs)
	if !ok {
		candidate := &localDirectory{stat: sync.OnceValues(func() (fs.FileInfo, error) {
			if parent := path.Dir(name); parent != "." {
				parentPath := parentPath(name, abs)
				if err := g.admitDirectory(parent, parentPath); err != nil {
					return nil, err
				}
			}
			return os.Lstat(abs)
		})}
		cached, _ = g.directories.LoadOrStore(abs, candidate)
	}
	directory := cached.(*localDirectory)
	if directory.listed {
		return nil
	}
	info, err := directory.stat()
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return nil
}

func parentPath(name, abs string) string {
	return abs[:len(abs)-len(name)+len(path.Dir(name))]
}
