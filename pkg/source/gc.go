package source

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SweepOpts tunes Sweep's behavior.
type SweepOpts struct {
	// MaxAge marks an entry stale when its mtime is older than now-MaxAge.
	// Stale entries are removed unless DryRun. Zero disables age-based
	// pruning (only orphan refs are cleaned).
	MaxAge time.Duration

	// IncludeMirrors enables age-based pruning of bare git mirrors at
	// <root>/git-mirrors. Mirrors are otherwise preserved because re-
	// hydrating them is expensive (a full clone over the network).
	// Set true when running an explicit "wipe stale state" pass.
	IncludeMirrors bool

	// DryRun reports what would be removed without touching disk.
	DryRun bool
}

// SweepResult summarizes what Sweep did (or would have done under DryRun).
type SweepResult struct {
	// Removed lists every entry that was deleted (or would be under
	// DryRun). Paths are absolute. Group by parent dir to recover the
	// "kind" of cache that lost the entry — sources/, baselines/, etc.
	Removed []string
	// Bytes is the cumulative size of removed entries before deletion.
	// Approximate — du-style recursive walk per entry.
	Bytes int64
	// Errors aggregates per-entry I/O errors. Sweep continues past
	// individual failures and reports them here so a single permissions
	// error on one slot doesn't abort the whole sweep.
	Errors []error
}

// Sweep prunes stale entries under root according to opts. Walks the
// known per-cache subdirectories (sources/, baselines/, blobs/sha256/)
// and removes top-level entries whose mtime is older than opts.MaxAge.
// Dangling refs files whose digest no longer materializes in
// blobs/sha256/ are removed regardless of age.
//
// Mirrors at <root>/git-mirrors are preserved by default; pass
// IncludeMirrors to age-prune them too.
//
// Returns a SweepResult with the (would-be-)removed paths and a count
// of recoverable bytes. Individual I/O errors land in Result.Errors
// rather than short-circuiting the sweep.
func Sweep(root string, opts SweepOpts) (SweepResult, error) {
	var res SweepResult
	if root == "" {
		return res, fmt.Errorf("sweep: empty cache root")
	}
	cutoff := time.Time{}
	if opts.MaxAge > 0 {
		cutoff = time.Now().Add(-opts.MaxAge)
	}

	ageRoots := []string{
		filepath.Join(root, "sources"),
		filepath.Join(root, "baselines"),
		filepath.Join(root, "blobs", "sha256"),
	}
	if opts.IncludeMirrors {
		ageRoots = append(ageRoots, filepath.Join(root, "git-mirrors"))
	}
	for _, dir := range ageRoots {
		sweepDirByAge(dir, cutoff, opts.DryRun, &res)
	}

	sweepDanglingRefs(filepath.Join(root, "refs"), root, opts.DryRun, &res)

	return res, nil
}

// sweepDirByAge removes immediate-child entries of dir whose mtime is
// older than cutoff. For sources/ this is one level deeper (slug/hash)
// — we descend one extra level so the slug/ wrapper doesn't shield
// stale hash slots from age comparison.
func sweepDirByAge(dir string, cutoff time.Time, dryRun bool, res *SweepResult) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			res.Errors = append(res.Errors, fmt.Errorf("read %s: %w", dir, err))
		}
		return
	}
	// sources/ has an extra slug/ level — recurse one deeper so the
	// final hash dirs are the age targets.
	if filepath.Base(dir) == "sources" {
		for _, e := range entries {
			sweepDirByAge(filepath.Join(dir, e.Name()), cutoff, dryRun, res)
		}
		return
	}
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		info, err := e.Info()
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("stat %s: %w", path, err))
			continue
		}
		if cutoff.IsZero() {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		size := entrySize(path)
		res.Removed = append(res.Removed, path)
		res.Bytes += size
		if dryRun {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("remove %s: %w", path, err))
		}
	}
}

// sweepDanglingRefs walks every file under refsRoot (recursively) and
// removes entries whose stored digest no longer materializes a blob
// under <cacheRoot>/blobs/sha256/. Refs files are tiny so we read
// each one to compare.
func sweepDanglingRefs(refsRoot, cacheRoot string, dryRun bool, res *SweepResult) {
	_ = filepath.WalkDir(refsRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return fs.SkipDir
			}
			res.Errors = append(res.Errors, fmt.Errorf("walk %s: %w", path, err))
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".tmp.") {
			// Orphaned staging file from a torn Refs.Put — clean.
			res.Removed = append(res.Removed, path)
			if !dryRun {
				_ = os.Remove(path) //nolint:gosec // path is a WalkDir result under refsRoot
			}
			return nil
		}
		b, rerr := os.ReadFile(path) //nolint:gosec // path is a WalkDir result under refsRoot
		if rerr != nil {
			res.Errors = append(res.Errors, fmt.Errorf("read %s: %w", path, rerr))
			return nil
		}
		digest := strings.TrimSpace(string(b))
		if digest == "" {
			res.Removed = append(res.Removed, path)
			if !dryRun {
				_ = os.Remove(path) //nolint:gosec // path is a WalkDir result under refsRoot
			}
			return nil
		}
		// Reject anything that doesn't look like a sha256 hex — defense
		// against a hostile or corrupt refs file injecting a traversal
		// path. The filepath.Join below would normalize "../..", but
		// rejecting upfront keeps the blame closer to the source.
		if !looksLikeDigest(digest) {
			res.Errors = append(res.Errors, fmt.Errorf("refs %s: bogus digest %q", path, digest))
			return nil
		}
		blobPath := filepath.Join(cacheRoot, "blobs", "sha256", digest)
		if _, err := os.Stat(blobPath); err == nil { //nolint:gosec // digest gated by looksLikeDigest above
			return nil
		}
		// The digest this ref points at has been swept away — drop the
		// pointer so future lookups don't keep claiming "we know about
		// this digest, but it's gone".
		res.Removed = append(res.Removed, path)
		if !dryRun {
			if err := os.Remove(path); err != nil { //nolint:gosec // path is a WalkDir result under refsRoot
				res.Errors = append(res.Errors, fmt.Errorf("remove %s: %w", path, err))
			}
		}
		return nil
	})
}

// looksLikeDigest reports whether s plausibly is a sha256 hex string.
// Used to gate refs files before we treat their contents as a path
// component — a corrupt file containing "../../etc/passwd" should fail
// closed rather than feed into filepath.Join.
func looksLikeDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// entrySize returns the cumulative byte size under path. Best-effort —
// returns 0 on any walk error rather than failing the sweep over a
// missing-file race.
func entrySize(path string) int64 {
	var total int64
	_ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// Log emits a structured summary of res at slog.LevelInfo. Convenience
// for CLI / orchestrator callers that don't want to format the result
// inline.
func (res *SweepResult) Log() {
	slog.Info("cache sweep",
		"removed", len(res.Removed),
		"bytes", res.Bytes,
		"errors", len(res.Errors))
}
