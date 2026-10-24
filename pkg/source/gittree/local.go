package gittree

import (
	"cmp"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// AdmitDirectory constructs the direct input artifact at the resolved local root.
// Synthetic source labels MUST use their already admitted filesystem instead.
func AdmitDirectory(ctx context.Context, location *gitrepo.Location) (*store.SourceArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := location.ContentRoot
	if _, err := os.Stat(root); err != nil {
		return nil, err
	}
	input := newLocal(root, false, &localPolicy{})
	input.location = location
	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "file://" + root, LocalPath: root, LocalRoot: root, FS: input, ContentFingerprint: ContentFingerprint}, nil
}

type localView struct {
	root         string
	operation    bool
	observations sync.Map
}

type localPolicy struct {
	once    sync.Once
	matcher *sourceignore.Matcher
	err     error
}

func (p *localPolicy) load(input fs.FS) (*sourceignore.Matcher, error) {
	p.once.Do(func() {
		p.matcher, p.err = sourceignore.NewFS(input, nil, "", true)
	})
	return p.matcher, p.err
}

// The public wrapper and mounted state MUST share one allocation on loader paths.
func newLocal(root string, operation bool, policy *localPolicy) *FS {
	input := &struct {
		FS
		localView
	}{root: root, operation: operation}
	input.view = &input.localView
	input.snapshot = policy
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

type localObservation struct {
	listed  atomic.Bool
	once    sync.Once
	info    fs.FileInfo
	err     error
	entries []fs.DirEntry
	listErr error
}

func (o *localObservation) list(readDir func() ([]fs.DirEntry, error)) ([]fs.DirEntry, error) {
	o.once.Do(func() {
		o.entries, o.listErr = readDir()
		o.listed.Store(true)
	})
	return o.entries, o.listErr
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
	if !g.operation {
		return g.readDir(abs)
	}
	cached, _ := g.observations.Load(abs)
	entries, err := cached.(*localObservation).list(func() ([]fs.DirEntry, error) { return g.readDir(abs) })
	return slices.Clone(entries), err
}

func (g *localView) readDir(abs string) ([]fs.DirEntry, error) {
	entries, err := os.ReadDir(abs)
	return slices.DeleteFunc(entries, func(entry fs.DirEntry) bool {
		if g.operation && entry.Type()&fs.ModeSymlink != 0 && entry.Name() != ".git" {
			name := filepath.Join(abs, entry.Name())
			g.observations.LoadOrStore(name, &localObservation{err: &fs.PathError{Op: "stat", Path: name, Err: fs.ErrPermission}})
		}
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

// Directory admission belongs to one operation; descriptors retain no observations.
func (g *localView) localStat(name, abs string) (fs.FileInfo, error) {
	var info fs.FileInfo
	var err error
	if cached, ok := g.observations.Load(abs); g.operation && ok {
		observation := cached.(*localObservation)
		info, err = observation.info, observation.err
	} else {
		info, err = g.statPath(name, abs)
		if g.operation && (err != nil || info.IsDir()) {
			candidate := &localObservation{info: info, err: err}
			cached, _ := g.observations.LoadOrStore(abs, candidate)
			observation := cached.(*localObservation)
			info, err = observation.info, observation.err
		}
	}
	if err == nil && info.Mode()&fs.ModeSymlink != 0 {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrPermission}
	}
	if errors.Is(err, fs.ErrNotExist) || err == nil && !info.Mode().IsRegular() && !info.IsDir() {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return info, err
}

func (g *localView) statPath(name, abs string) (fs.FileInfo, error) {
	if g.operation && name != "." {
		if cached, ok := g.observations.Load(filepath.Dir(abs)); ok && cached.(*localObservation).listed.Load() {
			entries, err := cached.(*localObservation).list(func() ([]fs.DirEntry, error) {
				return g.readDir(filepath.Dir(abs))
			})
			if err != nil {
				return nil, err
			}
			i, found := slices.BinarySearchFunc(entries, path.Base(name), func(entry fs.DirEntry, base string) int {
				return cmp.Compare(entry.Name(), base)
			})
			if !found {
				if cached, ok := g.observations.Load(abs); ok {
					observation := cached.(*localObservation)
					return observation.info, observation.err
				}
				return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
			}
			return entries[i].Info()
		}
	}
	if parent := path.Dir(name); parent != "." {
		if err := g.admitDirectory(parent, filepath.Dir(abs)); err != nil {
			return nil, err
		}
	}
	return os.Lstat(abs)
}

func (g *localView) admitDirectory(name, abs string) error {
	info, err := g.localStat(name, abs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return nil
}
