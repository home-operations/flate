package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

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

	// sweepInflight is a single-flight gate: when the persistent cache
	// trips its size cap, exactly one Stage call kicks the sweep on a
	// background goroutine and every other concurrent caller observes
	// the in-flight bit and skips.
	sweepInflight atomic.Bool

	// remoteFetches dedupes URL fetches across every preflight pass in
	// one orchestrator run. A single kustomization.yaml URL may be
	// reached via multiple Flux Kustomizations (parent emits a child
	// that shares the same path; multiple KSes whose subPath crosses
	// the same nested kustomization). Without this, every reconcile
	// re-fetches the same URL and re-emits the same WARN line.
	remoteFetches sync.Map // url string -> *remoteFetch
}

// remoteFetch carries the result of one URL fetch. The fetch runs in
// a single background goroutine (gated by start.Do) detached from any
// caller's ctx so a cancellation on the first caller doesn't poison
// the cached result for everyone else — the previous OnceValues+ctx
// capture would freeze ctx.Canceled into every subsequent FetchRemote
// call for the same URL. Callers select on their own ctx vs the
// done channel; the fetch runs to completion under the package-level
// remoteFetchTimeout.
type remoteFetch struct {
	start sync.Once
	done  chan struct{}
	body  []byte
	err   error
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

// FetchRemote returns the body of urlStr, fetched at most once per
// (url, success) cache entry. Successful bodies are cached for the
// StagingCache lifetime; transient errors (DNS, connection reset,
// timeout, 5xx) are NOT cached — the next caller retries. Only
// definitive HTTP 4xx responses are cached as negative entries
// (they won't change between retries within a run).
//
// Without the success-only cache, a single transient hiccup at
// orchestrator startup poisoned every subsequent reconcile of every
// KS referencing that URL for the rest of the run.
//
// The fetch runs in a background goroutine seeded with a detached
// context (httpGetURL applies remoteFetchTimeout internally) so a
// cancellation on the first caller doesn't propagate into the
// cached error. Each caller still honors its own ctx via the
// select below.
func (c *StagingCache) FetchRemote(ctx context.Context, urlStr string) ([]byte, error) {
	loaded, _ := c.remoteFetches.LoadOrStore(urlStr, &remoteFetch{done: make(chan struct{})})
	rf := loaded.(*remoteFetch)
	rf.start.Do(func() {
		go func() {
			rf.body, rf.err = httpGetURL(context.Background(), urlStr)
			close(rf.done)
			// On transient failure (network / 5xx / timeout — anything
			// that isn't a definitive 4xx), drop the cache entry so
			// the next caller retries instead of inheriting our
			// failure for the rest of the run. isHTTPClientError uses
			// errors.As against httpStatusError so it stays correct
			// even when the error is wrapped (e.g. "preflight: %w").
			if rf.err != nil && !isHTTPClientError(rf.err) {
				c.remoteFetches.CompareAndDelete(urlStr, rf)
			}
		}()
	})
	select {
	case <-rf.done:
		return rf.body, rf.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// isHTTPClientError reports whether err is a definitive HTTP 4xx
// response (which won't change between retries within one run).
// Anything else — transport errors, timeouts, 5xx — is treated as
// transient so the cache entry gets dropped.
//
// Uses errors.As against httpStatusError so the check stays correct
// when the error is wrapped (e.g. fmt.Errorf("preflight: %w", err)).
func isHTTPClientError(err error) bool {
	var hse *httpStatusError
	return errors.As(err, &hse) && hse.Code >= 400 && hse.Code < 500
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
			c.maybeKickSweep()
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
		c.maybeKickSweep()
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
		c.maybeKickSweep()
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
	// readers never observe a partial tree.
	staging, err := os.MkdirTemp(filepath.Dir(dir), filepath.Base(dir)+".tmp.*")
	if err != nil {
		return "", fmt.Errorf("stage tmp: %w", err)
	}
	if err := c.copyTreeInto(source, staging); err != nil {
		_ = os.RemoveAll(staging)
		return "", err
	}
	// Rename FIRST, sentinel AFTER. If we wrote the sentinel into the
	// tmp dir and then crashed (or the rename failed mid-flight on a
	// filesystem that doesn't truly fulfill POSIX atomicity), a
	// subsequent reader could observe an incomplete tree with a
	// sentinel inside it and skip the rebuild. By renaming the
	// sentinel-free tree first and writing the sentinel into the
	// renamed dir, any crash window leaves the dir without a sentinel,
	// which the read path correctly treats as "not complete, rebuild."
	if err := os.Rename(staging, dir); err != nil {
		_ = os.RemoveAll(staging)
		// A racing peer process beat us to it. Adopt the winner —
		// content addressing guarantees the trees are equivalent.
		if _, statErr := os.Stat(sentinel); statErr == nil {
			c.cacheStagePromise(fingerprint, dir)
			c.maybeKickSweep()
			return dir, nil
		}
		return "", fmt.Errorf("stage finalize: %w", err)
	}
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
	d := dir
	c.stages[fingerprint] = &stage{
		persistent: true,
		once:       func() (string, error) { return d, nil },
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

// copyTreeInto materializes every regular file from src into dst.
// Symlinks are dereferenced, dotfiles are skipped to keep stages
// clean. Caller owns dst — copyTreeInto neither creates nor removes
// it on failure (so the persistent path can stage into a sibling
// tempdir and atomically rename).
//
// The walk collects file-copy tasks serially (cheap, also creates
// the destination directory skeleton) and then fans them out across
// a worker pool. Each task is independent — hardlinks are atomic;
// byte copies operate on distinct dst paths — so concurrency is
// safe. The pool is capped at runtime.NumCPU because the cost per
// task is I/O, not CPU, and over-fanning would just thrash the page
// cache.
func (c *StagingCache) copyTreeInto(src, dst string) error {
	type task struct {
		srcPath, dstPath string
		mode             os.FileMode
	}
	var tasks []task

	walkErr := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		base := d.Name()
		if d.IsDir() {
			// Skip anything that isn't user content: .git / node_modules
			// and every dot-prefixed dir (which captures .flate-cache).
			if base == "node_modules" || strings.HasPrefix(base, ".") {
				return fs.SkipDir
			}
			return os.MkdirAll(filepath.Join(dst, rel), 0o750)
		}
		// Only stat when we need to follow a symlink — DirEntry already
		// carries the file-type bits for regular entries. Skipping the
		// stat on 50k regular files in a monorepo eliminates the same
		// number of syscalls; hardlinks inherit mode from source so the
		// fallback-copy's mode field is only consulted on cross-FS
		// stages (EXDEV), where 0o600 is acceptable for kustomize input.
		if d.Type()&fs.ModeSymlink == 0 {
			if !d.Type().IsRegular() {
				return nil
			}
			tasks = append(tasks, task{path, filepath.Join(dst, rel), 0o600})
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			// A dangling symlink in the user's working tree (a common
			// local-only state for editor lockfiles, gitignored
			// .pre-commit-config.yaml, IDE caches) used to abort the
			// entire stage. flate doesn't need the link target — Flux's
			// reconcile wouldn't either — so skip silently when the
			// target is missing. Other Stat errors (permissions, I/O)
			// still surface.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		tasks = append(tasks, task{path, filepath.Join(dst, rel), info.Mode()})
		return nil
	})
	if walkErr != nil {
		return fmt.Errorf("stage %s: %w", src, walkErr)
	}

	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.NumCPU())
	for _, t := range tasks {
		g.Go(func() error { return copyFile(t.srcPath, t.dstPath, t.mode) })
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("stage %s: %w", src, err)
	}
	return nil
}

// copyFile materializes srcPath at dstPath. Hardlinks when source and
// destination sit on the same filesystem — a stage of a monorepo would
// otherwise duplicate gigabytes of bytes on every render. Falls back to
// a stream copy on cross-device (EXDEV) failures so the cache continues
// to work when a user points --cache-dir at a different volume than
// their working tree.
//
// Callers that mutate the staged file MUST first os.Remove it so the
// hardlink is broken before write — otherwise an O_TRUNC|O_WRONLY open
// on the staged path would modify the source's underlying inode.
// flux.go's restoreKustomizationFile follows that protocol; new write
// sites must too.
func copyFile(srcPath, dstPath string, mode os.FileMode) error {
	if err := os.Link(srcPath, dstPath); err == nil {
		return nil
	} else if !errors.Is(err, syscall.EXDEV) {
		// Other Link failures (permissions, source missing) fall
		// through to the copy path so unusual filesystems still work.
		// The cross-device case is the only one we explicitly classify
		// to keep the fast path readable.
		_ = err
	}
	src, err := os.Open(srcPath) //nolint:gosec // srcPath is a tree-walk result under our source root
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode.Perm()) //nolint:gosec // dstPath is inside our staging tempdir
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}

// maybeKickSweep starts an asynchronous LRU sweep when the persistent
// cache exceeds maxBytes. The sweepInflight flag single-flights the
// check so concurrent Stage calls don't each launch a sweep.
func (c *StagingCache) maybeKickSweep() {
	if c.maxBytes <= 0 || c.root == "" {
		return
	}
	if !c.sweepInflight.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.sweepInflight.Store(false)
		if err := sweepStageBySize(c.root, c.maxBytes); err != nil {
			slog.Debug("stage cache sweep", "err", err)
		}
	}()
}

// stageEntry summarizes one persistent stage directory under the
// cache root for the LRU sweep.
type stageEntry struct {
	path  string
	mtime time.Time
	size  int64
}

// sweepStageBySize walks root and, when total size exceeds maxBytes,
// removes the oldest entries (by mtime) until size is at or below the
// cap. Entries currently being staged into (`.tmp.*` siblings) are
// ignored — they don't carry sentinels yet and would be wasteful to
// reap mid-build. The per-process scratch tempdirs (flate-stage-*)
// are likewise skipped here; their lifecycle is Close-bound.
func sweepStageBySize(root string, maxBytes int64) error {
	// Walk the two-char prefix layer and collect every fingerprint
	// dir's mtime + size. Stage entries don't recurse — they cap at
	// one fingerprint deep.
	prefixes, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	var entries []stageEntry
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
			size := dirSize(full)
			entries = append(entries, stageEntry{path: full, mtime: info.ModTime(), size: size})
			total += size
		}
	}
	if total <= maxBytes {
		return nil
	}
	// Oldest first; evict until under cap.
	slices.SortFunc(entries, func(a, b stageEntry) int {
		return a.mtime.Compare(b.mtime)
	})
	for _, e := range entries {
		if total <= maxBytes {
			break
		}
		if err := os.RemoveAll(e.path); err != nil {
			slog.Debug("stage cache evict", "path", e.path, "err", err)
			continue
		}
		total -= e.size
		// Best-effort: drop the prefix dir if it's now empty so
		// repeated sweeps don't accumulate dead 2-char shells.
		prefixDir := filepath.Dir(e.path)
		if rem, err := os.ReadDir(prefixDir); err == nil && len(rem) == 0 {
			_ = os.Remove(prefixDir)
		}
	}
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
