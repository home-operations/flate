package source

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/home-operations/flate/pkg/source/blob"
	"github.com/home-operations/flate/pkg/source/cacheroot"
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
	// Computed during the same walk that decides removal so the sweep
	// stays O(files) overall.
	Bytes int64
	// Errors aggregates per-entry I/O errors. Sweep continues past
	// individual failures and reports them here so a single permissions
	// error on one slot doesn't abort the whole sweep.
	Errors []error
}

// Sweep prunes stale entries from the cache root described by layout
// using a mark-sweep strategy:
//
//  1. MARK — read every refs/<category>/<key> file, build the set of
//     digests still referenced from disk.
//  2. SWEEP — walk the layout-managed subtrees (sources, baselines,
//     blobs, optionally git-mirrors) and remove entries older than
//     MaxAge. Blobs whose digest is in the live set survive
//     regardless of age — a fresh ref must always resolve.
//  3. ORPHAN refs — drop refs whose digest no longer materializes a
//     blob. Runs regardless of MaxAge; these are dead pointers and
//     refs cost nothing on disk anyway.
//
// Mirrors are preserved by default; pass IncludeMirrors to age-prune
// them too. Individual I/O errors land in Result.Errors rather than
// short-circuiting the sweep.
func Sweep(layout cacheroot.Layout, opts SweepOpts) (SweepResult, error) {
	var res SweepResult
	if layout.Root == "" {
		return res, fmt.Errorf("sweep: empty cache root")
	}
	cutoff := time.Time{}
	if opts.MaxAge > 0 {
		cutoff = time.Now().Add(-opts.MaxAge)
	}

	// Hold the exclusive GC lock for the entire mark + sweep so no
	// blob.Refs.Put can land an atomic-rename between markLiveDigests
	// and the blob sweep — the race would otherwise purge a freshly-
	// referenced blob as orphan-old. Refs.Put takes the shared lock so
	// the gate is invisible for the common case (no GC running).
	unlockGC := blob.LockGC()
	defer unlockGC()

	live := markLiveDigests(layout, &res)

	// Each entry pairs a directory to sweep with the depth at which
	// the age comparison applies and whether the leaf's basename is
	// a content digest to consult the live set with. Sources land at
	// <root>/sources/<slug>/<hash>/ so age comparison is two levels
	// in; blobs sit one level below the algo segment and are gated
	// by mark.
	ageRoots := []struct {
		dir   string
		depth int
		gate  func(name string) bool // skip when gate returns true
	}{
		{layout.Sources(), 2, nil},
		{layout.Baselines(), 1, nil},
		{layout.Blobs(), 1, func(name string) bool { _, ok := live[name]; return ok }},
	}
	if opts.IncludeMirrors {
		ageRoots = append(ageRoots, struct {
			dir   string
			depth int
			gate  func(name string) bool
		}{layout.GitMirrors(), 1, nil})
	}
	for _, ar := range ageRoots {
		sweepDirByAge(ar.dir, ar.depth, cutoff, ar.gate, opts.DryRun, &res)
	}

	sweepDanglingRefs(layout, opts.DryRun, &res)

	return res, nil
}

// markLiveDigests walks every refs/<category>/<key> file and returns
// the set of digests they point at. Used to gate blob sweep: a fresh
// ref must always be able to resolve, even if its blob is "old".
// Malformed or missing refs land in res.Errors but don't abort.
func markLiveDigests(layout cacheroot.Layout, res *SweepResult) map[string]struct{} {
	live := map[string]struct{}{}
	refsRoot := layout.RefsRoot()
	_ = filepath.WalkDir(refsRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return fs.SkipDir
			}
			res.Errors = append(res.Errors, fmt.Errorf("mark refs %s: %w", path, err))
			return nil
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".tmp.") {
			return nil
		}
		b, rerr := os.ReadFile(path) //nolint:gosec // path is a WalkDir result under refsRoot
		if rerr != nil {
			res.Errors = append(res.Errors, fmt.Errorf("mark read %s: %w", path, rerr))
			return nil
		}
		digest := strings.TrimSpace(string(b))
		if looksLikeDigest(digest) {
			live[digest] = struct{}{}
		}
		return nil
	})
	return live
}

// sweepDirByAge removes immediate-child entries of dir whose mtime is
// older than cutoff. depth>1 recurses (sources/<slug>/<hash> — the
// slug wrapper would otherwise shield stale hash slots from
// comparison). gate, when non-nil, is consulted with the leaf name
// before age is checked; returning true preserves the entry.
//
// Sizes accumulate during this single walk; entries that survive the
// age check don't contribute to res.Bytes, but neither do they get
// re-walked elsewhere — the sweep stays O(files) overall.
func sweepDirByAge(dir string, depth int, cutoff time.Time, gate func(string) bool, dryRun bool, res *SweepResult) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			res.Errors = append(res.Errors, fmt.Errorf("read %s: %w", dir, err))
		}
		return
	}
	if depth > 1 {
		for _, e := range entries {
			sweepDirByAge(filepath.Join(dir, e.Name()), depth-1, cutoff, gate, dryRun, res)
		}
		return
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".lock") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if gate != nil && gate(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("stat %s: %w", path, err))
			continue
		}
		if cutoff.IsZero() || info.ModTime().After(cutoff) {
			continue
		}
		res.Bytes += entrySize(path)
		res.Removed = append(res.Removed, path)
		if dryRun {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("remove %s: %w", path, err))
		}
	}
}

// sweepDanglingRefs walks every file under the layout's refs root
// (recursively) and removes entries whose stored digest no longer
// materializes a blob in the layout's blob store. Refs files are tiny
// so we read each one to compare.
//
// This runs AFTER the blob age sweep — if the mark phase preserved
// blobs that live refs point at, only genuinely-dead pointers (or
// orphaned .tmp.* staging files from torn writes) remain here.
func sweepDanglingRefs(layout cacheroot.Layout, dryRun bool, res *SweepResult) {
	refsRoot := layout.RefsRoot()
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
		if !looksLikeDigest(digest) {
			res.Errors = append(res.Errors, fmt.Errorf("refs %s: bogus digest %q", path, digest))
			return nil
		}
		blobPath := layout.Blob(digest)
		if _, err := os.Stat(blobPath); err == nil { //nolint:gosec // digest gated by looksLikeDigest above
			return nil
		}
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
