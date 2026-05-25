package change

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"os/exec"
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

// Detect returns the set of repo-relative file paths that differ
// between before and after.
//
// Fast path: when git is on $PATH, `git diff --no-index --name-status
// -z` does the comparison in C. For a 50k-file tree with one changed
// file, this finishes in ~10–50ms vs ~200ms–3s for the Go walker —
// because git's tree-walk + content-compare is implemented in C and
// uses index-style optimizations the Go walker can't match. The Go
// path remains as a fallback for: (a) systems without git installed,
// (b) git invocations that fail with an unexpected exit code, and
// (c) paths where git refuses to operate.
//
// Slow path: walks before and after concurrently, then hashes every
// same-sized file pair. The previous (size, mtime) fast-path was
// removed — on coarse-granularity filesystems (HFS+ 1s, fresh `git
// checkout` clock-stamping) two distinct same-sized files written in
// the same second produce indistinguishable mtimes, so trusting them
// as identical silently dropped real changes. Always hashing is the
// only correctness-preserving option on the fallback path; the git
// path doesn't need it because content comparison is intrinsic to
// git's diff machinery.
//
// Directories whose name begins with "." (e.g. .git, .flate-cache)
// and well-known noise dirs (node_modules, vendor) are skipped on
// both paths.
func Detect(before, after string) (*Set, error) {
	if before == "" || after == "" {
		return nil, errors.New("change.Detect: both paths required")
	}

	set, err := detectViaGit(before, after)
	if err == nil {
		return set, nil
	}
	// Distinguish "git not on PATH" (expected on minimal CI
	// containers) from "git failed unexpectedly" (worth a log so
	// operators can investigate). LookPath returns
	// *exec.Error{Err: ErrNotFound}; everything else is a real
	// fault on the git path that callers might want to know about.
	var lookErr *exec.Error
	if !errors.As(err, &lookErr) || !errors.Is(lookErr.Err, exec.ErrNotFound) {
		slog.Debug("change.Detect: git path failed, falling back to Go walker", "err", err)
	}
	return detectViaWalker(before, after)
}

// detectViaGit runs `git diff --no-index --name-status -z` between
// the two paths and parses the NUL-separated output. Each entry is a
// (status, path) pair: status is one byte (A/D/M/T...), path is the
// absolute path on whichever side reported the change. Strip the
// before/after prefix to get the repo-relative path; filter out paths
// inside skip-dirs the Go walker would have skipped (.git/, etc.).
//
// Returns an error if git is not installed, if the diff command
// errors out unexpectedly, or if the output is malformed. Callers
// fall back to the Go walker on any error.
func detectViaGit(before, after string) (*Set, error) {
	if _, err := exec.LookPath("git"); err != nil {
		return nil, err
	}
	absBefore, err := filepath.Abs(before)
	if err != nil {
		return nil, err
	}
	absAfter, err := filepath.Abs(after)
	if err != nil {
		return nil, err
	}
	// G204: git is a fixed binary on $PATH (we just LookPath'd it);
	// absBefore and absAfter are caller-controlled directory paths
	// the orchestrator passes from validated --path / --path-orig
	// flags. The "--" separator before them disambiguates against
	// any path starting with `-`.
	cmd := exec.Command("git", "diff", "--no-index", "--name-status", //nolint:gosec // see comment above
		"-z", "--no-renames", "--", absBefore, absAfter)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if runErr := cmd.Run(); runErr != nil {
		// Exit 1 = differences found (expected); other exits = real
		// failure (e.g. one path doesn't exist, missing-newline
		// complaints, etc.).
		var ee *exec.ExitError
		if !errors.As(runErr, &ee) || ee.ExitCode() != 1 {
			return nil, runErr
		}
	}

	beforePrefix := filepath.ToSlash(absBefore) + "/"
	afterPrefix := filepath.ToSlash(absAfter) + "/"
	paths := make(map[string]struct{})

	// Output format with --name-status -z (and --no-renames): an even
	// number of NUL-separated fields, status then path, repeated. A
	// trailing empty field after the final NUL is normal.
	parts := bytes.Split(stdout.Bytes(), []byte{0})
	for i := 0; i+1 < len(parts); i += 2 {
		if len(parts[i]) == 0 || len(parts[i+1]) == 0 {
			continue
		}
		p := filepath.ToSlash(string(parts[i+1]))
		var rel string
		switch {
		case strings.HasPrefix(p, beforePrefix):
			rel = p[len(beforePrefix):]
		case strings.HasPrefix(p, afterPrefix):
			rel = p[len(afterPrefix):]
		default:
			// Unexpected path shape — skip rather than mis-attribute.
			// Defensive only: git always reports paths under the input
			// directories we passed.
			continue
		}
		if isFilteredPath(rel) {
			continue
		}
		paths[rel] = struct{}{}
	}
	return &Set{paths: paths}, nil
}

// isFilteredPath reports whether any path segment matches the
// skip-dir rules (mirrors the directory-pruning the Go walker does
// inline). git diff doesn't honor .gitignore on --no-index mode and
// happily reports .git/ internals, so we post-filter to keep the
// fast and slow paths producing identical results.
func isFilteredPath(rel string) bool {
	for segment := range strings.SplitSeq(rel, "/") {
		if shouldSkipDir(segment) {
			return true
		}
	}
	return false
}

// detectViaWalker is the git-less fallback: walk both trees,
// content-hash every same-sized file pair. Slower than the git path
// but doesn't depend on an external binary, and is the only correct
// option for environments where git isn't available.
func detectViaWalker(before, after string) (*Set, error) {
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
		// Same size, unknown mtime trustworthiness — hash both sides.
		// The pre-removal mtime fast-path silently dropped real edits
		// on coarse-mtime filesystems; correctness over speed here.
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

// fileMeta is the (size, abs) tuple collected by scanTree. The
// previous (size, mtime) shape supported a same-mtime-skip fast path
// that was removed because coarse-granularity filesystems made it
// drop real changes.
type fileMeta struct {
	size int64
	abs  string
}

// scanTree walks root collecting per-file (size, abs). Mirrors
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
			size: info.Size(),
			abs:  p,
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
