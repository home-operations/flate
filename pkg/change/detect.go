package change

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
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
	out := make([]string, 0, len(s.paths))
	for p := range s.paths {
		out = append(out, p)
	}
	// Stable order makes logs and CI output deterministic.
	slices.Sort(out)
	return out
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

// Detect walks before and after concurrently, then compares files via a
// cheap (size, mtime) pre-pass before falling back to SHA-256 only for
// files where size matches but mtime differs. Files present on only
// one side are also included. The mtime check is opportunistic — when
// timestamps are unreliable (e.g. fresh `git checkout`) we still fall
// through to hashing same-sized files, so correctness is preserved.
//
// Directories whose name begins with "." (e.g. .git, .flate-cache)
// and well-known noise dirs (node_modules) are skipped.
func Detect(before, after string) (*Set, error) {
	if before == "" || after == "" {
		return nil, errors.New("change.Detect: both paths required")
	}

	var (
		eg       errgroup.Group
		beforeFS map[string]fileMeta
		afterFS  map[string]fileMeta
	)
	eg.Go(func() error {
		fs, err := scanTree(before)
		beforeFS = fs
		return err
	})
	eg.Go(func() error {
		fs, err := scanTree(after)
		afterFS = fs
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	paths := make(map[string]struct{}, len(afterFS)/8)
	type hashJob struct {
		rel              string
		beforeAbs, after string
	}
	var hashJobs []hashJob

	for rel, after := range afterFS {
		bef, ok := beforeFS[rel]
		if !ok {
			paths[rel] = struct{}{}
			continue
		}
		if bef.size != after.size {
			paths[rel] = struct{}{}
			continue
		}
		// Same size: trust matching mtime as identical, hash otherwise.
		if bef.mtime == after.mtime {
			continue
		}
		hashJobs = append(hashJobs, hashJob{rel: rel, beforeAbs: bef.abs, after: after.abs})
	}
	for rel := range beforeFS {
		if _, ok := afterFS[rel]; !ok {
			paths[rel] = struct{}{}
		}
	}

	if len(hashJobs) > 0 {
		var mu sync.Mutex
		hg, _ := errgroup.WithContext(context.Background())
		const hashWorkers = 8
		jobs := make(chan hashJob, len(hashJobs))
		for range hashWorkers {
			hg.Go(func() error {
				for j := range jobs {
					b, err := hashFile(j.beforeAbs)
					if err != nil {
						return err
					}
					a, err := hashFile(j.after)
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

// fileMeta is the (size, mtime, abs) tuple collected by scanTree.
type fileMeta struct {
	size  int64
	mtime int64 // unix nanos
	abs   string
}

// scanTree walks root collecting per-file (size, mtime, abs). Mirrors
// the directory pruning that hashTree previously did.
func scanTree(root string) (map[string]fileMeta, error) {
	out := map[string]fileMeta{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		base := d.Name()
		if d.IsDir() {
			if base != root && shouldSkipDir(base) {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = fileMeta{
			size:  info.Size(),
			mtime: info.ModTime().UnixNano(),
			abs:   p,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // path is a tree-walk result, not user-controlled
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func shouldSkipDir(name string) bool {
	if strings.HasPrefix(name, ".") {
		return true
	}
	switch name {
	case "node_modules", "vendor":
		return true
	}
	return false
}
