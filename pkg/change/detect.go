package change

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/source/sourceignore"
)

// Set is the immutable result of Detect — the set of file paths
// (relative to the scan roots) whose contents differ.
type Set struct {
	paths map[string]struct{}
}

// NewSet constructs a Set from an iterable of relative paths.
func NewSet(paths []string) *Set {
	out := &Set{paths: make(map[string]struct{}, len(paths))}
	for _, p := range paths {
		out.paths[filepath.ToSlash(p)] = struct{}{}
	}
	return out
}

// Len reports how many files differ.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.paths)
}

// Paths returns the changed files as a sorted slice.
func (s *Set) Paths() []string {
	if s == nil {
		return nil
	}
	// Stable order makes logs and CI output deterministic.
	return slices.Sorted(maps.Keys(s.paths))
}

// Contains reports whether rel is in the change set. rel is expected
// to be filepath.ToSlash-normalized.
func (s *Set) Contains(rel string) bool {
	if s == nil {
		return false
	}
	_, ok := s.paths[filepath.ToSlash(rel)]
	return ok
}

// Reroot returns a copy of s with prefix prepended to every entry —
// used to lift a change set produced from a subdir-relative diff up
// into the repo-relative coordinate system that SourceFiles uses.
func (s *Set) Reroot(prefix string) *Set {
	if s == nil {
		return nil
	}
	prefix = strings.TrimSuffix(filepath.ToSlash(prefix), "/")
	if prefix == "" || prefix == "." {
		return s
	}
	out := &Set{paths: make(map[string]struct{}, len(s.paths))}
	for p := range s.paths {
		out.paths[prefix+"/"+p] = struct{}{}
	}
	return out
}

// Detect returns the set of repo-relative file paths that differ between
// before and after, using each tree's own Flux sourceignore rules. File contents,
// additions, deletions, entry types and symlink targets participate in the diff.
func Detect(before, after string) (*Set, error) {
	if before == "" || after == "" {
		return nil, errors.New("change.Detect: both paths required")
	}
	before, err := filepath.Abs(before)
	if err != nil {
		return nil, err
	}
	after, err = filepath.Abs(after)
	if err != nil {
		return nil, err
	}
	var (
		eg       errgroup.Group
		beforeFS map[string]fileMeta
		afterFS  map[string]fileMeta
	)
	eg.Go(func() error {
		tree, err := scanTree(before, after)
		beforeFS = tree
		return err
	})
	eg.Go(func() error {
		tree, err := scanTree(after, before)
		afterFS = tree
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	paths := make(map[string]struct{}, len(afterFS)/8)
	type hashJob struct {
		rel                 string
		beforeAbs, afterAbs string
		// symlink applies to both sides: a job is only queued when the
		// before/after entry kinds agree (a type swap is handled above).
		symlink bool
	}
	var hashJobs []hashJob

	for rel, aft := range afterFS {
		bef, ok := beforeFS[rel]
		if !ok {
			paths[rel] = struct{}{}
			continue
		}
		if bef.symlink != aft.symlink {
			paths[rel] = struct{}{}
			continue
		}
		if bef.size != aft.size {
			paths[rel] = struct{}{}
			continue
		}
		// Coarse filesystem timestamps cannot establish content equality.
		hashJobs = append(hashJobs, hashJob{
			rel: rel, beforeAbs: bef.abs, afterAbs: aft.abs,
			symlink: aft.symlink,
		})
	}
	for rel := range beforeFS {
		if _, ok := afterFS[rel]; !ok {
			paths[rel] = struct{}{}
		}
	}

	if len(hashJobs) > 0 {
		var mu sync.Mutex
		var hg errgroup.Group
		const hashWorkers = 8
		jobs := make(chan hashJob, len(hashJobs))
		for range hashWorkers {
			hg.Go(func() error {
				for j := range jobs {
					b, err := hashEntry(j.beforeAbs, j.symlink)
					if err != nil {
						return err
					}
					a, err := hashEntry(j.afterAbs, j.symlink)
					if err != nil {
						return err
					}
					if a != b {
						mu.Lock()
						paths[j.rel] = struct{}{}
						mu.Unlock()
					}
				}
				return nil
			})
		}
		for _, j := range hashJobs {
			jobs <- j
		}
		close(jobs)
		if err := hg.Wait(); err != nil {
			return nil, err
		}
	}

	return &Set{paths: paths}, nil
}

type fileMeta struct {
	size    int64
	abs     string
	symlink bool
}

// scanTree must descend excluded directories so deeper re-includes survive.
// The opposite snapshot and .git metadata never belong to the artifact view.
func scanTree(root, opposite string) (map[string]fileMeta, error) {
	out := map[string]fileMeta{}
	var ignoreFiles []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != root && (p == opposite || d.Name() == ".git") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() == ".sourceignore" {
			ignoreFiles = append(ignoreFiles, p)
		}
		typ := d.Type()
		// Regular files and symlinks both participate in the diff;
		// other entry kinds (sockets, devices, named pipes) don't
		// land in a Flux tree and would confuse the comparison.
		isLink := typ&fs.ModeSymlink != 0
		if !typ.IsRegular() && !isLink {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = fileMeta{
			abs:     p,
			symlink: isLink,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Opposite-snapshot rules must not disable this side's Flux defaults.
	matcher, err := sourceignore.NewFromFiles(root, ignoreFiles, nil, true)
	if err != nil {
		return nil, err
	}
	for rel, meta := range out {
		if matcher.Match(rel, false) {
			delete(out, rel)
			continue
		}
		// Lstat preserves the link's own size for comparison via readlink.
		info, err := os.Lstat(meta.abs)
		if err != nil {
			return nil, err
		}
		meta.size = info.Size()
		out[rel] = meta
	}
	return out, nil
}

// Symlinks are compared without following them because flate renders in-root
// links. github.com/fluxcd/pkg/artifact v0.18.3 storage.Storage.Archive
// (storage/archive.go) omits non-regular files; changing that render-parity gap
// must also change detection's treatment of links.
func hashEntry(path string, isSymlink bool) (string, error) {
	if isSymlink {
		target, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256([]byte(target))
		return hex.EncodeToString(sum[:]), nil
	}
	return hashFile(path)
}

var hashBuffers = sync.Pool{New: func() any {
	buf := make([]byte, 32*1024)
	return &buf
}}

func hashFile(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // path is a tree-walk result, not user-controlled
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	buf := hashBuffers.Get().(*[]byte)
	defer hashBuffers.Put(buf)
	// Hide File.WriteTo so CopyBuffer uses the pooled buffer.
	if _, err := io.CopyBuffer(h, struct{ io.Reader }{f}, *buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
