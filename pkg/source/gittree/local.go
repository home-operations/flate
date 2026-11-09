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
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// AdmitDirectory constructs the direct input artifact at the resolved local root.
// Synthetic source labels MUST use their already admitted filesystem instead.
func AdmitDirectory(ctx context.Context, location *gitrepo.Location) (*store.SourceArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	root := location.ContentRoot
	if _, err := os.Stat(root); err != nil {
		return nil, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	visibility, err := newGitVisibility(location)
	if err != nil {
		return nil, err
	}
	input := newLocal(root, false, &localPolicy{}, visibility)
	input.location = location
	return &store.SourceArtifact{
		Kind:               manifest.KindGitRepository,
		URL:                "file://" + root,
		LocalPath:          root,
		LocalRoot:          root,
		FS:                 input,
		ContentFingerprint: ContentFingerprint,
	}, nil
}

type localView struct {
	visibility   gitVisibility
	root         string
	operation    bool
	observations sync.Map
}

// gitVisibility hides untracked paths that the repository ignore rules match.
// Flux only receives committed content, so these paths never reach its source artifact.
type gitVisibility struct {
	matcher gitignore.Matcher
	tracked map[string]bool
	prefix  string
}

func newGitVisibility(location *gitrepo.Location) (gitVisibility, error) {
	if location.Repository == nil {
		return gitVisibility{}, nil
	}
	patterns, err := gitignore.ReadPatterns(osfs.New(location.RepositoryRoot, osfs.WithBoundOS()), nil)
	if err != nil {
		return gitVisibility{}, fmt.Errorf("read git ignore rules: %w", err)
	}
	if len(patterns) == 0 {
		return gitVisibility{}, nil
	}
	index, err := location.Repository.Storer.Index()
	if err != nil {
		return gitVisibility{}, fmt.Errorf("read git index: %w", err)
	}
	prefix, err := filepath.Rel(location.RepositoryRoot, location.ContentRoot)
	if err != nil {
		return gitVisibility{}, fmt.Errorf("locate content root: %w", err)
	}
	tracked := make(map[string]bool, len(index.Entries))
	for _, entry := range index.Entries {
		for name := entry.Name; name != "." && !tracked[name]; name = path.Dir(name) {
			tracked[name] = true
		}
	}
	return gitVisibility{
		matcher: gitignore.NewMatcher(patterns),
		tracked: tracked,
		prefix:  filepath.ToSlash(prefix),
	}, nil
}

func (v gitVisibility) hidden(name string, isDir bool) bool {
	if v.matcher == nil {
		return false
	}
	name = path.Join(v.prefix, name)
	return !v.tracked[name] && v.matcher.Match(strings.Split(name, "/"), isDir)
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
func newLocal(root string, operation bool, policy *localPolicy, visibility gitVisibility) *FS {
	input := &struct {
		FS
		localView
	}{root: root, operation: operation, visibility: visibility}
	input.view = &input.localView
	input.snapshot = policy
	return &input.FS
}

func (g *localView) Stat(name string) (fs.FileInfo, error) {
	info, err := g.Lstat(name)
	if pathErr, ok := errors.AsType[*fs.PathError](err); ok {
		return info, &fs.PathError{Op: statOperation, Path: pathErr.Path, Err: pathErr.Err}
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
	//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
	return os.Open(
		abs,
	)
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
	if admitDirectoryErr := g.admitDirectory(name, abs); admitDirectoryErr != nil {
		return nil, admitDirectoryErr
	}
	if !g.operation {
		return g.readDir(name, abs)
	}
	cached, _ := g.observations.Load(abs)
	entries, err := cached.(*localObservation).list( //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		func() ([]fs.DirEntry, error) { return g.readDir(name, abs) },
	)
	return slices.Clone(entries), err
}

func (g *localView) readDir(dir, abs string) ([]fs.DirEntry, error) {
	entries, err := os.ReadDir(abs)
	return slices.DeleteFunc(entries, func(entry fs.DirEntry) bool {
		if g.operation && entry.Type()&fs.ModeSymlink != 0 && entry.Name() != gitDirectory {
			name := filepath.Join(abs, entry.Name())
			g.observations.LoadOrStore(
				name,
				&localObservation{err: &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrPermission}},
			)
		}
		return entry.Name() == gitDirectory || !entry.Type().IsRegular() && !entry.IsDir() ||
			g.visibility.hidden(path.Join(dir, entry.Name()), entry.IsDir())
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
	//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
	return os.Open(
		abs,
	)
}

func (g *localView) ReadLink(name string) (string, error) {
	abs, err := g.mounted(name)
	if err != nil {
		return "", err
	}
	if _, localStatErr := g.localStat(name, abs); localStatErr != nil {
		return "", localStatErr
	}
	target, err := os.Readlink(abs)
	if err != nil {
		return "", err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	return target, nil
}

func (g *localView) mounted(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrInvalid}
	}
	for component := range strings.SplitSeq(name, "/") {
		if component == gitDirectory {
			return "", &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
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
		observation := cached.(*localObservation) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		info, err = observation.info, observation.err
	} else {
		info, err = g.statPath(name, abs)
		if g.operation && (err != nil || info.IsDir()) {
			candidate := &localObservation{info: info, err: err}
			cached, _ := g.observations.LoadOrStore(abs, candidate)
			observation := cached.(*localObservation) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
			info, err = observation.info, observation.err
		}
	}
	if err == nil && info.Mode()&fs.ModeSymlink != 0 {
		return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrPermission}
	}
	if errors.Is(err, fs.ErrNotExist) ||
		err == nil && (!info.Mode().IsRegular() && !info.IsDir() || g.visibility.hidden(name, info.IsDir())) {
		return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return info, err
}

func (g *localView) statPath(name, abs string) (fs.FileInfo, error) {
	var listing *localObservation
	if g.operation && name != "." {
		if cached, ok := g.observations.Load(
			filepath.Dir(abs),
		); ok &&
			cached.(*localObservation).listed.Load() { //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
			listing = cached.(*localObservation) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		}
	}
	if listing != nil {
		entries, err := listing.list(
			func() ([]fs.DirEntry, error) {
				return g.readDir(path.Dir(name), filepath.Dir(abs))
			},
		)
		if err != nil {
			return nil, err
		}
		i, found := slices.BinarySearchFunc(entries, path.Base(name), func(entry fs.DirEntry, base string) int {
			return cmp.Compare(entry.Name(), base)
		})
		if !found {
			if cached, ok := g.observations.Load(abs); ok {
				observation := cached.(*localObservation) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
				return observation.info, observation.err
			}
			return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
		}
		return entries[i].Info() //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
	}
	if parent := path.Dir(name); parent != "." {
		if err := g.admitDirectory(parent, filepath.Dir(abs)); err != nil {
			return nil, err
		}
	}
	//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
	return os.Lstat(
		abs,
	)
}

func (g *localView) admitDirectory(name, abs string) error {
	info, err := g.localStat(name, abs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return nil
}
