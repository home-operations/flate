package helm

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	atomicflag "sync/atomic"
	"time"

	"github.com/home-operations/flate/pkg/source/atomic"
)

// diskRenderCache persists helm template-output across `flate`
// invocations. It sits behind the in-process templateCache (Phase 2.2):
// the in-process LRU has zero hit rate across processes, and profile
// evidence from `buroa/k8s-gitops` puts helm.TemplateDocs at ~64% of
// warm allocations and ~13% of warm CPU — most of that re-runs across
// CLI invocations.
//
// Layout under root (the layout's RenderHelmCache directory):
//
//	<root>/<hex[:2]>/<hex>
//
// where <hex> is the full hex-encoded sha256 cache key produced by
// computeTemplateKey. The two-char shard avoids one giant directory
// (mkdir scan / readdir cost balloons past ~100k entries on common
// filesystems). Contents are gzip-encoded rendered manifest bytes.
//
// Concurrency: Get is unsynchronized — the filesystem already
// serialises atomic-rename writes against partial reads. Put is
// likewise unsynchronized; the only coordination is the single-flight
// background sweep gated by sweepBusy so a burst of Puts triggers one
// eviction pass instead of N.
type diskRenderCache struct {
	root  string
	limit int64 // total disk bytes; <=0 disables disk caching

	sweepBusy atomicflag.Int32 // 1 = a sweep is in flight; gates re-trigger

	// rootOnce ensures the root directory is mkdir'd exactly once
	// per process. The first Put creates the directory tree lazily;
	// subsequent Puts skip the syscall.
	rootOnce sync.Once
}

// newDiskRenderCache returns a disk-backed cache rooted at root with
// the supplied byte cap. A non-positive limit or empty root returns
// nil — treated as "disk caching disabled" by the in-process layer.
//
// The root is not created here; the first Put materialises it via
// rootOnce so we never leave an empty directory behind on disabled
// configurations.
func newDiskRenderCache(root string, limitBytes int64) *diskRenderCache {
	if root == "" || limitBytes <= 0 {
		return nil
	}
	return &diskRenderCache{root: root, limit: limitBytes}
}

// pathFor returns the on-disk path for a key. Sharding by the first
// two hex chars caps any one directory at ~16k peer entries even for
// caches in the millions — readdir stays cheap.
func (c *diskRenderCache) pathFor(key string) string {
	if len(key) < 2 {
		// Defensive: computeTemplateKey returns a 64-hex digest, so
		// in practice this branch never fires. The zero-pad shards a
		// pathological short key into the "00" bucket rather than
		// landing it at the root.
		return filepath.Join(c.root, "00", key)
	}
	return filepath.Join(c.root, key[:2], key)
}

// Get reads and decompresses the cached payload for key. Returns
// (nil, false) on any miss (including I/O / decompression errors —
// they're best-effort surfaced via slog Debug). A nil receiver is the
// "disk cache disabled" sentinel and silently misses.
//
// Best-effort mtime bump: a successful read chtimes the file so the
// next sweep treats it as "recently used" (the OS atime is not a safe
// proxy on noatime filesystems). The bump is async-safe; concurrent
// sweep already takes the sweep coordination flag.
func (c *diskRenderCache) Get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	p := c.pathFor(key)
	raw, err := os.ReadFile(p) //nolint:gosec // path derived from sha256 hex of caller-controlled key
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Debug("helm render cache: read", "path", p, "err", err)
		}
		return nil, false
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		slog.Debug("helm render cache: gunzip header", "path", p, "err", err)
		return nil, false
	}
	defer func() { _ = gz.Close() }()
	out, err := io.ReadAll(gz)
	if err != nil {
		slog.Debug("helm render cache: gunzip body", "path", p, "err", err)
		return nil, false
	}
	// Touch the file so LRU eviction by mtime treats this entry as
	// fresh. Best-effort — a missing chtimes (race with sweep, EROFS)
	// just falls back to the original mtime.
	now := nowFn()
	_ = os.Chtimes(p, now, now) //nolint:gosec // path derived from sha256 hex of caller-controlled key
	return out, true
}

// Put gzip-encodes payload and atomically writes it to the sharded
// path. Subsequent reads either observe the previous complete file or
// the new one — never a partial. After the write we kick a background
// sweep (single-flight via sweepBusy) so the fast path doesn't block
// on directory walks.
//
// nil-receiver no-ops so call sites can unconditionally Put without
// guarding the wiring constructor.
func (c *diskRenderCache) Put(key string, payload []byte) {
	if c == nil {
		return
	}
	c.rootOnce.Do(func() {
		if err := os.MkdirAll(c.root, 0o750); err != nil {
			slog.Debug("helm render cache: mkdir root", "root", c.root, "err", err)
		}
	})

	p := c.pathFor(key)
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		slog.Debug("helm render cache: mkdir shard", "dir", dir, "err", err)
		return
	}

	var buf bytes.Buffer
	// Default gzip level — rendered manifests are mostly text and
	// compress 4-6x; the CPU cost of level=DefaultCompression vs.
	// BestSpeed is dominated by the helm render we just skipped.
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(payload); err != nil {
		slog.Debug("helm render cache: gzip write", "path", p, "err", err)
		return
	}
	if err := gw.Close(); err != nil {
		slog.Debug("helm render cache: gzip close", "path", p, "err", err)
		return
	}

	// syncDir=false: a render cache miss is cheap to re-derive on the
	// next invocation, so we trade durability for write throughput.
	// Mirrors atomic.WriteFile's documented "high-churn cache" mode.
	//
	// atomic.WriteFile removes its staged tmpfile on any error path
	// (see pkg/source/atomic/file.go's committed-bool defer guard),
	// so a failed Put can't leak partial state into the cache root.
	if err := atomic.WriteFile(p, buf.Bytes(), 0o600, false); err != nil {
		slog.Debug("helm render cache: write", "path", p, "err", err)
		return
	}

	// Kick a sweep if one isn't already running. CompareAndSwap keeps
	// the trigger single-flight: a burst of Puts past the limit will
	// schedule exactly one sweep, which sees every entry written
	// before it starts walking and prunes the oldest until total ≤
	// limit.
	if c.sweepBusy.CompareAndSwap(0, 1) {
		go c.sweep()
	}
}

// SweepBlocking forces a synchronous eviction pass. Test affordance —
// production callers use the async trigger inside Put. Useful for
// asserting eviction ordering without flake.
func (c *diskRenderCache) SweepBlocking() {
	if c == nil {
		return
	}
	// Wait for any in-flight async sweep to drain before kicking off
	// our own so the synchronous call sees a stable view.
	for !c.sweepBusy.CompareAndSwap(0, 1) {
		time.Sleep(time.Millisecond)
	}
	c.sweep()
}

// sweep walks the cache root, totals byte usage, and (if over the
// limit) deletes oldest-by-mtime entries until total ≤ limit. Runs
// on a single goroutine — sweepBusy gates re-trigger so a sustained
// write storm doesn't fork ~one sweep per Put. The flag clears at the
// end so the *next* over-limit Put can re-trigger.
//
// Errors are swallowed (logged at Debug) — the cache is best-effort
// and a stat / unlink failure on one entry shouldn't stop the rest of
// the sweep.
func (c *diskRenderCache) sweep() {
	defer c.sweepBusy.Store(0)

	type entry struct {
		path  string
		size  int64
		mtime int64 // unix nanos for stable sort
	}
	var (
		entries []entry
		total   int64
	)
	walkErr := filepath.WalkDir(c.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		entries = append(entries, entry{
			path:  path,
			size:  info.Size(),
			mtime: info.ModTime().UnixNano(),
		})
		total += info.Size()
		return nil
	})
	if walkErr != nil {
		slog.Debug("helm render cache: sweep walk", "root", c.root, "err", walkErr)
	}
	if total <= c.limit {
		return
	}

	// Oldest first — those evict before the most recently used. Stable
	// against ties (sort by mtime then path) so two test entries
	// written within the same nanosecond don't evict in
	// platform-dependent order.
	slices.SortFunc(entries, func(a, b entry) int {
		if c := cmp.Compare(a.mtime, b.mtime); c != 0 {
			return c
		}
		return cmp.Compare(a.path, b.path)
	})

	for _, e := range entries {
		if total <= c.limit {
			break
		}
		if err := os.Remove(e.path); err != nil {
			slog.Debug("helm render cache: sweep remove", "path", e.path, "err", err)
			continue
		}
		total -= e.size
	}
}

// nowFn is the wall-clock used by Get's mtime bump. Pulled out so
// tests that need a deterministic clock can rebind it; production code
// reads time.Now directly.
var nowFn = time.Now
