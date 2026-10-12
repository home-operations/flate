package source

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
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

// Sweep prunes live Git/source/baseline cache entries by age.
func Sweep(layout cacheroot.Layout, opts SweepOpts) (SweepResult, error) {
	var res SweepResult
	if layout.Root == "" {
		return res, fmt.Errorf("sweep: empty cache root")
	}
	cutoff := time.Time{}
	if opts.MaxAge > 0 {
		cutoff = time.Now().Add(-opts.MaxAge)
	}
	res.sweepDirByAge(layout.Sources(), 2, cutoff, opts.DryRun)
	res.sweepDirByAge(layout.Baselines(), 1, cutoff, opts.DryRun)
	if opts.IncludeMirrors {
		res.sweepDirByAge(layout.GitMirrors(), 1, cutoff, opts.DryRun)
	}
	res.sweepGitGenerations(layout, opts.DryRun)
	return res, nil
}

// sweepDirByAge removes immediate-child entries of dir whose mtime is
// older than cutoff. depth>1 recurses (sources/<slug>/<hash> — the
// slug wrapper would otherwise shield stale hash slots from
// comparison).
//
// Sizes accumulate during this single walk; entries that survive the
// age check don't contribute to res.Bytes, but neither do they get
// re-walked elsewhere — the sweep stays O(files) overall.
func (res *SweepResult) sweepDirByAge(dir string, depth int, cutoff time.Time, dryRun bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			res.Errors = append(res.Errors, fmt.Errorf("read %s: %w", dir, err))
		}
		return
	}
	if depth > 1 {
		for _, e := range entries {
			res.sweepDirByAge(filepath.Join(dir, e.Name()), depth-1, cutoff, dryRun)
		}
		return
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".lock") {
			continue
		}
		path := filepath.Join(dir, e.Name())

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

// The existing store ownership lock pins every acquired reader until Cache.Close.
func (res *SweepResult) sweepGitGenerations(layout cacheroot.Layout, dryRun bool) {
	dir := filepath.Dir(layout.GitObjects(""))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			res.Errors = append(res.Errors, fmt.Errorf("read git generations: %w", err))
		}
		return
	}
	lock := flock.New(layout.OCILock(), flock.SetPermissions(0o600))
	defer func() { _ = lock.Close() }()
	locked, err := lock.TryLock()
	if err != nil {
		res.Errors = append(res.Errors, fmt.Errorf("lock git generations: %w", err))
		return
	}
	if !locked {
		return
	}
	data, err := os.ReadFile(filepath.Join(layout.OCI(), ocispec.ImageIndexFile))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		res.Errors = append(res.Errors, fmt.Errorf("read git index: %w", err))
		return
	}
	var index ocispec.Index
	if err == nil {
		if err := json.Unmarshal(data, &index); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("read git index: %w", err))
			return
		}
	}
	referenced := make(map[string]struct{}, len(index.Manifests))
	for _, desc := range index.Manifests {
		if desc.Annotations[ocispec.AnnotationRefName] == "" {
			continue
		}
		referenced[desc.Annotations["flate.git-generation"]] = struct{}{}
	}
	for _, entry := range entries {
		id := entry.Name()
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.String() != id || !entry.IsDir() {
			continue
		}
		if _, ok := referenced[id]; ok {
			continue
		}
		path := filepath.Dir(layout.GitObjects(id))
		res.Bytes += entrySize(path)
		res.Removed = append(res.Removed, path)
		if !dryRun {
			if err := os.RemoveAll(path); err != nil {
				res.Errors = append(res.Errors, fmt.Errorf("remove git generation: %w", err))
			}
		}
	}
}
