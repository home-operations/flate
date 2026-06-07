package kustomize

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/home-operations/flate/internal/cas"
	"github.com/home-operations/flate/internal/diskcache"
	"github.com/home-operations/flate/internal/keylock"
	atomicfile "github.com/home-operations/flate/pkg/source/atomic"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// stageCompleteSentinel marks a content-addressed stage directory as
// fully materialized. Its presence lets subsequent processes skip the
// copyTree pass entirely — a crash mid-build leaves no sentinel, so
// the next Stage call rebuilds from scratch.
const stageCompleteSentinel = ".flate-stage-complete"

// StagingCache materializes one-or-more source roots into a stage
// directory so Flux's kustomize Generator can safely write into the
// staged copy without touching the user's working tree.
//
// Two staging modes:
//   - Content-addressed (the fast path). When the caller supplies a
//     source fingerprint (git commit SHA, OCI digest, etc.), the stage
//     lands at <layout.Stage()>/<fp[:2]>/<fp>/ guarded by a sentinel
//     file. Subsequent runs — including across processes — observe
//     the sentinel and skip the copyTree pass entirely. Eviction is
//     LRU by mtime, capped by maxBytes.
//   - Per-process scratch (the fallback). Used when the source has no
//     stable fingerprint (local-path sources whose mtimes shift on
//     every editor save, for instance). Each Stage call materializes
//     a `flate-stage-*` tempdir under layout.Stage(); the entire set
//     is removed on Close.
//
// Lifecycle for both modes is tied to the surrounding orchestrator
// run — call Close to clean up scratch stages and release in-memory
// state. Persistent CAS stages survive Close so the next process
// reuses them.
type StagingCache struct {
	// root is the persistent stage cache parent (layout.Stage()). All
	// persistent CAS directories land beneath it. When empty, the
	// cache degrades to per-process behavior in an OS tempdir.
	root string

	// maxBytes caps the persistent stage cache. 0 disables eviction
	// (unbounded growth — the GC subcommand still handles cleanup).
	maxBytes int64

	// mu guards stages + remoteFetches initialization. The persistent
	// path uses fpLocks for per-fingerprint serialization; mu is only
	// held briefly to look up or insert the in-process stage record.
	mu     sync.Mutex
	stages map[string]*stage
	// fpLocks serializes concurrent Stage calls for the same
	// fingerprint within one process. Cross-process serialization is
	// not provided — concurrent flate invocations rebuilding the same
	// fingerprint just race on the atomic rename; the loser observes
	// the winner's sentinel via the cross-process retry below.
	fpLocks *keylock.KeyMap[string]

	// sweepGate is a single-flight gate: when the persistent cache
	// trips its size cap, exactly one Stage call kicks the sweep on a
	// background goroutine and every other concurrent caller observes
	// the in-flight bit and skips.
	sweepGate diskcache.Gate

	// remoteFetches dedupes URL fetches across every preflight pass in
	// one orchestrator run. A single kustomization.yaml URL may be
	// reached via multiple Flux Kustomizations (parent emits a child
	// that shares the same path; multiple KSes whose subPath crosses
	// the same nested kustomization). Without this, every reconcile
	// re-fetches the same URL and re-emits the same WARN line.
	remoteFetches sync.Map // url string -> *remoteFetch

	// sizes memoizes each completed stage's byte size by directory path
	// for the LRU sweep, which needs the per-stage size to enforce the
	// cap. A completed stage is content-addressed and effectively
	// immutable (only its per-render kustomization.yaml is rewritten, a
	// few bytes against a soft cap), so its size is computed once per run
	// and reused across the sweeps a cold/multi-miss run triggers —
	// instead of re-walking (filepath.WalkDir) every file of every staged
	// tree on every sweep. Entries are dropped on eviction; a re-staged
	// fingerprint repopulates on the next miss. (Warm runs sweep zero
	// times — see maybeKickSweep — so this only matters on the cold path.)
	sizes sync.Map // stage dir path string -> int64

	// sweepKicks counts calls to maybeKickSweep (sweep launches attempted,
	// gate or not). Cache hits must not bump it — a hit doesn't grow the
	// cache — so a warm run's count stays flat. Lightweight observability;
	// asserted by TestStagePersistent_HitDoesNotKickSweep.
	sweepKicks atomic.Int64
}

// stage holds the per-source materialization promise. The OnceValues
// pattern keeps copyTree work to one execution per key within the
// process even if multiple goroutines call Stage concurrently.
type stage struct {
	once func() (string, error)
	// persistent flags content-addressed stages so Close skips them —
	// only per-process scratch stages get removed on shutdown.
	persistent bool
}

// NewStagingCache constructs a cache that places per-process scratch
// stages and persistent content-addressed stages under parent. If
// parent is empty, the OS tempdir is used (and persistent staging is
// effectively disabled — every Stage call materializes a scratch
// dir). maxBytes caps the persistent cache; 0 disables eviction.
//
// Sweeps any `flate-stage-*` directory under parent that's older
// than staleStageAge — those are crashed-process leftovers from
// runs where Close didn't fire (SIGKILL, panic, ctx not honored).
// Best-effort: a sweep error doesn't fail construction; the dirs
// just stay until the next successful sweep.
func NewStagingCache(parent string, maxBytes int64) (*StagingCache, error) {
	root := parent
	if root == "" {
		root = os.TempDir()
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	sweepStaleStageDirs(root)
	return &StagingCache{
		stages:   make(map[string]*stage),
		root:     root,
		maxBytes: maxBytes,
		fpLocks:  keylock.New[string](),
	}, nil
}

// NewStagingCacheFromLayout is the orchestrator's preferred constructor
// — it pulls the persistent stage root from the supplied Layout so all
// path policy lives in one place.
func NewStagingCacheFromLayout(layout cacheroot.Layout, maxBytes int64) (*StagingCache, error) {
	return NewStagingCache(layout.Stage(), maxBytes)
}

// staleStageAge is the age threshold for the crash-leftover sweep.
// 24h is conservative — long enough to never reap a long-running
// concurrent flate process (which can't realistically run a day),
// short enough to keep $TMPDIR from accumulating.
const staleStageAge = 24 * time.Hour

// sweepStaleStageDirs removes `flate-stage-*` directories under
// parent whose mtime is older than staleStageAge. Best-effort: any
// per-entry error is logged at Debug and the sweep continues.
//
// Persistent content-addressed dirs use 2-char fan-out prefixes and
// are explicitly NOT touched here — their lifecycle is governed by
// LRU eviction (sweepBySize) and the GC subcommand.
func sweepStaleStageDirs(parent string) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-staleStageAge)
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "flate-stage-") {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		_ = os.RemoveAll(filepath.Join(parent, e.Name()))
	}
}

// Stage returns the on-disk staged copy of source.
//
// When fingerprint is non-empty, the persistent content-addressed
// path is used: <root>/<fp[:2]>/<fp>/ guarded by a sentinel. A
// previous (or concurrent peer) run that finished the same
// fingerprint lets us skip copyTree entirely — the single largest
// CPU saving across cold and warm reruns.
//
// When fingerprint is empty (local-path sources whose mtimes shift
// faster than a hash could keep up, or any source for which no
// canonical digest is available), the cache falls back to the legacy
// per-process behavior: a `flate-stage-*` tempdir under root,
// memoized by source path for the life of the cache, removed on
// Close.
func (c *StagingCache) Stage(ctx context.Context, source, fingerprint string) (string, error) {
	resolved, err := filepath.EvalSymlinks(source)
	if err == nil {
		source = resolved
	}
	if fingerprint == "" {
		return c.stagePerProcess(source)
	}
	return c.stagePersistent(ctx, source, fingerprint)
}

// stagePerProcess implements the legacy fallback for sources that
// don't carry a stable fingerprint: one tempdir per source, memoized
// for the life of the cache via sync.OnceValues, removed on Close.
func (c *StagingCache) stagePerProcess(source string) (string, error) {
	c.mu.Lock()
	s, ok := c.stages[source]
	if !ok {
		copyOnce := sync.OnceValues(func() (string, error) {
			dst, err := os.MkdirTemp(c.root, "flate-stage-*")
			if err != nil {
				return "", err
			}
			if err := c.copyTreeInto(source, dst); err != nil {
				_ = os.RemoveAll(dst)
				return "", err
			}
			return dst, nil
		})
		s = &stage{once: copyOnce}
		c.stages[source] = s
	}
	c.mu.Unlock()
	return s.once()
}

// stagePersistent implements the content-addressed fast path. The
// staged tree lands at root/<fp[:2]>/<fp>/ and a sentinel file is
// written atomically on completion so subsequent processes can skip
// copyTree.
//
// Per-fingerprint serialization within the process is handled by
// fpLocks. Cross-process races (two flate invocations rebuilding the
// same fingerprint simultaneously) are tolerated: each writes into
// its own tempdir; the loser's rename returns ENOTEMPTY and we
// retry against the winner's sentinel.
func (c *StagingCache) stagePersistent(ctx context.Context, source, fingerprint string) (string, error) {
	// Compose the on-disk slot. We reproduce the layout's prefix math
	// here to keep this package free of an explicit Layout dep — the
	// Stage constructor is given a parent dir, not a typed Layout, so
	// downstream tests stay simple.
	prefix := fingerprint
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}
	dir := filepath.Join(c.root, prefix, fingerprint)
	sentinel := filepath.Join(dir, stageCompleteSentinel)

	// Cache an in-memory record so repeated lookups for the same
	// fingerprint within one process skip even the sentinel stat.
	c.mu.Lock()
	s, ok := c.stages[fingerprint]
	if ok && s.persistent {
		c.mu.Unlock()
		if dir, err := s.once(); err == nil {
			return dir, nil
		}
		// Fall through to a fresh attempt if the cached promise
		// failed — a transient error shouldn't poison the slot for
		// the run.
	} else {
		c.mu.Unlock()
	}

	// Fast path: sentinel already present from a previous (or
	// concurrent peer) run. Touch the dir's mtime so LRU eviction
	// keeps recently-used stages alive.
	if _, err := os.Stat(sentinel); err == nil {
		c.cacheStagePromise(fingerprint, dir)
		now := time.Now()
		_ = os.Chtimes(dir, now, now)
		return dir, nil
	}

	// Slow path: serialize concurrent builds within the process.
	release, err := c.fpLocks.Acquire(ctx, fingerprint)
	if err != nil {
		return "", err
	}
	defer release()

	// Recheck under the lock — another goroutine may have populated
	// the sentinel while we were blocked.
	if _, err := os.Stat(sentinel); err == nil {
		c.cacheStagePromise(fingerprint, dir)
		now := time.Now()
		_ = os.Chtimes(dir, now, now)
		return dir, nil
	}

	if err := os.MkdirAll(filepath.Dir(dir), 0o750); err != nil {
		return "", fmt.Errorf("stage parent: %w", err)
	}

	// Clear any partial stage left over from a crash window between
	// rename and sentinel-write: the final dir exists but carries no
	// sentinel. The rebuild rename below would otherwise hit
	// ENOTEMPTY/EEXIST. Removing here is safe — the sentinel check
	// above ran under fpLocks so we're not racing an in-process peer,
	// and a cross-process peer that lands a complete tree+sentinel
	// after our recheck would be observed below via the rename's
	// adoption branch.
	if _, err := os.Stat(dir); err == nil {
		_ = os.RemoveAll(dir)
	}

	// Stage into a sibling tempdir + atomic rename so concurrent
	// readers never observe a partial tree. On a lost rename race we
	// adopt the winner only when its sentinel is present — content
	// addressing guarantees the trees are equivalent.
	adopted, err := cas.Stage(filepath.Dir(dir), dir, "stage tmp", "stage finalize",
		func(staging string) error { return c.copyTreeInto(source, staging) },
		func() bool { _, statErr := os.Stat(sentinel); return statErr == nil },
	)
	if err != nil {
		return "", err
	}
	if adopted {
		c.cacheStagePromise(fingerprint, dir)
		c.maybeKickSweep()
		return dir, nil
	}
	// Rename FIRST (inside cas.Stage above), sentinel AFTER. If we
	// wrote the sentinel into the tmp dir and then crashed (or the
	// rename failed mid-flight on a filesystem that doesn't truly
	// fulfill POSIX atomicity), a subsequent reader could observe an
	// incomplete tree with a sentinel inside it and skip the rebuild.
	// By renaming the sentinel-free tree first and writing the sentinel
	// into the renamed dir, any crash window leaves the dir without a
	// sentinel, which the read path correctly treats as "not complete,
	// rebuild."
	//
	// Sentinel is empty; its presence alone is the signal. Use
	// atomic.WriteFile so a crash during this write doesn't leave a
	// partially-named or zero-length sentinel that confuses a later
	// reader — either the sentinel is fully present or it isn't.
	if err := atomicfile.WriteFile(sentinel, nil, 0o600, false); err != nil {
		// The tree is intact at dir; we just couldn't write the
		// sentinel. Leave the tree in place — the next Stage call
		// observes the sentinel-free dir and rebuilds via the
		// partial-stage-clear path above. Returning an error here
		// surfaces the underlying problem to the caller.
		return "", fmt.Errorf("stage sentinel: %w", err)
	}
	c.cacheStagePromise(fingerprint, dir)
	c.maybeKickSweep()
	return dir, nil
}

// cacheStagePromise installs a satisfied stage record so subsequent
// in-process lookups for the same fingerprint return the same path
// without an additional os.Stat.
func (c *StagingCache) cacheStagePromise(fingerprint, dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.stages[fingerprint]; ok && existing.persistent {
		return
	}
	c.stages[fingerprint] = &stage{
		persistent: true,
		once:       func() (string, error) { return dir, nil },
	}
}

// Close removes every per-process scratch stage. Persistent
// content-addressed stages survive — they're the whole point of the
// cache and are owned by the LRU sweep + GC subcommand.
func (c *StagingCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for key, s := range c.stages {
		if s.persistent {
			continue
		}
		path, err := s.once()
		if err != nil {
			delete(c.stages, key)
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			errs = append(errs, err)
		}
		delete(c.stages, key)
	}
	return errors.Join(errs...)
}

// maybeKickSweep starts an asynchronous LRU sweep when the persistent
// cache exceeds maxBytes. sweepGate single-flights the check so
// concurrent Stage calls don't each launch a sweep.
//
// Only called after a stage is newly materialized (cache MISS — adopt
// or build), never on a cache hit: a hit doesn't grow the cache, so a
// cap that held before the hit still holds. A fully warm run (all hits)
// therefore performs zero sweeps. The cap is soft — the GC subcommand
// is the authority — so deferring eviction to the next growth (or GC)
// is fine, and it keeps the async sweep off the warm hot path (where it
// otherwise contended for cores; ~30% wall-time on a 2-core runner).
func (c *StagingCache) maybeKickSweep() {
	c.sweepKicks.Add(1)
	if c.maxBytes <= 0 || c.root == "" {
		return
	}
	if !c.sweepGate.TryAcquire() {
		return
	}
	go func() {
		defer c.sweepGate.Release()
		if err := c.sweepBySize(); err != nil {
			slog.Debug("stage cache sweep", "err", err)
		}
	}()
}

// stageSize returns the byte size of a completed (immutable) stage dir,
// memoized by path. A content-addressed stage's files don't change after
// its sentinel lands, so the cumulative size is computed once per run and
// reused across the many per-Stage sweeps. See the sizes field.
func (c *StagingCache) stageSize(dir string) int64 {
	if v, ok := c.sizes.Load(dir); ok {
		return v.(int64)
	}
	sz := dirSize(dir)
	c.sizes.Store(dir, sz)
	return sz
}

// sweepBySize walks root and, when total size exceeds maxBytes, removes
// the oldest entries (by mtime) until size is at or below the cap.
// Entries currently being staged into (`.tmp.*` siblings) are ignored —
// they don't carry sentinels yet and would be wasteful to reap
// mid-build. The per-process scratch tempdirs (flate-stage-*) are
// likewise skipped here; their lifecycle is Close-bound. Per-stage sizes
// are memoized (stageSize) so repeat sweeps don't re-walk unchanged trees.
func (c *StagingCache) sweepBySize() error {
	root, maxBytes := c.root, c.maxBytes
	// Walk the two-char prefix layer and collect every fingerprint
	// dir's mtime + size. Stage entries don't recurse — they cap at
	// one fingerprint deep.
	prefixes, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	var entries []diskcache.Entry
	var total int64
	for _, p := range prefixes {
		if !p.IsDir() {
			continue
		}
		name := p.Name()
		// Skip per-process scratch + transient tempdirs.
		if strings.HasPrefix(name, "flate-stage-") || strings.HasPrefix(name, ".tmp.") {
			continue
		}
		prefixDir := filepath.Join(root, name)
		fps, err := os.ReadDir(prefixDir)
		if err != nil {
			continue
		}
		for _, fp := range fps {
			if !fp.IsDir() {
				continue
			}
			fpName := fp.Name()
			if strings.Contains(fpName, ".tmp.") {
				continue
			}
			full := filepath.Join(prefixDir, fpName)
			// Reject anything missing the sentinel — likely an
			// abandoned partial build that some other process is
			// still racing on. The legacy crash sweep cleans those
			// up; LRU shouldn't.
			if _, err := os.Stat(filepath.Join(full, stageCompleteSentinel)); err != nil {
				continue
			}
			info, err := os.Stat(full)
			if err != nil {
				continue
			}
			size := c.stageSize(full)
			entries = append(entries, diskcache.Entry{Path: full, MTime: info.ModTime().UnixNano(), Size: size})
			total += size
		}
	}
	// Oldest first; evict until under cap.
	diskcache.EvictOldest(entries, total, maxBytes,
		func(a, b diskcache.Entry) int { return cmp.Compare(a.MTime, b.MTime) },
		func(e diskcache.Entry) error {
			if err := os.RemoveAll(e.Path); err != nil {
				slog.Debug("stage cache evict", "path", e.Path, "err", err)
				return err
			}
			c.sizes.Delete(e.Path) // forget the memo for the reaped stage

			// Best-effort: drop the prefix dir if it's now empty so
			// repeated sweeps don't accumulate dead 2-char shells.
			prefixDir := filepath.Dir(e.Path)
			if rem, err := os.ReadDir(prefixDir); err == nil && len(rem) == 0 {
				_ = os.Remove(prefixDir)
			}
			return nil
		},
	)
	return nil
}

// dirSize walks path and returns the cumulative byte size of every
// regular file. Hardlinks inflate the count (one byte counted per
// link, not per inode); accuracy here is "close enough" — the cap is
// for soft eviction, not accounting.
func dirSize(path string) int64 {
	var total int64
	_ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
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
