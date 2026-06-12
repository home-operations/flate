package diskcache

import (
	"cmp"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/home-operations/flate/pkg/source/atomic"
)

// zstdEncoder and zstdDecoder are process-lifetime singletons. zstd's EncodeAll
// and DecodeAll are safe for concurrent use, so every Store shares one of each
// rather than allocating a codec per call (the idiomatic, near-zero-allocation
// usage). SpeedDefault compresses flate's rendered-manifest payloads ~2x smaller
// than gzip while compressing ~8x and decompressing ~3.5x faster, at one
// allocation per call instead of gzip's 20-30 — see the codec benchmark in the
// PR that introduced this. NewWriter/NewReader cannot error with nil options, so
// the errors are discarded; both live for the process and are never closed.
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// Store is a persistent, cross-process disk cache of zstd-compressed byte
// payloads keyed by a content hash. Both render caches sit on it: the helm
// template-output cache (pkg/helm) stores rendered manifest bytes directly; the
// kustomize render cache (pkg/kustomize) stores a framed read-set + output and
// owns that framing on top. The Store owns everything below the value: the
// sharded on-disk layout, zstd compression, atomic writes, and the single-flight
// mtime-LRU sweep that bounds total bytes.
//
// Layout under root:
//
//	<root>/<hex[:2]>/<hex>
//
// where <hex> is the full hex-encoded sha256 cache key. The two-char shard
// avoids one giant directory (mkdir scan / readdir cost balloons past ~100k
// entries on common filesystems). Contents are zstd(payload).
//
// Concurrency: Get is unsynchronized — the filesystem already serialises
// atomic-rename writes against partial reads. Put is likewise unsynchronized;
// the only coordination is the single-flight background sweep gated by sweepGate
// so a burst of Puts triggers one eviction pass instead of N. A nil *Store is the
// "caching disabled" sentinel: every method no-ops / misses, so call sites need
// not guard the wiring.
type Store struct {
	root  string
	limit int64 // total disk bytes; <=0 disables caching

	sweepGate gate // single-flight gate so a burst of Puts triggers one sweep

	// rootOnce ensures the root directory is mkdir'd exactly once per process.
	// The first Put creates the directory tree lazily; subsequent Puts skip the
	// syscall.
	rootOnce sync.Once
}

// NewStore returns a disk-backed Store rooted at root with the supplied byte
// cap. A non-positive limit or empty root returns nil — the "caching disabled"
// sentinel the callers wire through.
//
// The root is not created here; the first Put materialises it via rootOnce so we
// never leave an empty directory behind on disabled configurations.
func NewStore(root string, limitBytes int64) *Store {
	if root == "" || limitBytes <= 0 {
		return nil
	}
	return &Store{root: root, limit: limitBytes}
}

// pathFor returns the on-disk path for a key. Sharding by the first two hex
// chars caps any one directory at ~16k peer entries even for caches in the
// millions — readdir stays cheap.
func (s *Store) pathFor(key string) string {
	if len(key) < 2 {
		// Defensive: keys are sha256 hex (64 chars), so in practice this branch
		// never fires. The zero-pad shards a pathological short key into the
		// "00" bucket rather than landing it at the root.
		return filepath.Join(s.root, "00", key)
	}
	return filepath.Join(s.root, key[:2], key)
}

// Get reads and decompresses the cached payload for key. Returns (nil, false) on
// any miss (including I/O / decompression errors — they're best-effort surfaced
// via slog Debug). A nil receiver is the "disabled" sentinel and silently
// misses.
//
// Best-effort mtime bump: a successful read chtimes the file so the next sweep
// treats it as "recently used" (the OS atime is not a safe proxy on noatime
// filesystems). The bump is async-safe; a concurrent sweep already takes the
// sweep coordination flag.
func (s *Store) Get(key string) ([]byte, bool) {
	if s == nil {
		return nil, false
	}
	p := s.pathFor(key)
	raw, err := os.ReadFile(p) //nolint:gosec // path derived from sha256 hex of caller-controlled key
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Debug("disk render cache: read", "path", p, "err", err)
		}
		return nil, false
	}
	out, err := zstdDecoder.DecodeAll(raw, nil)
	if err != nil {
		// A decode failure is a clean miss: a torn write, or an entry left by a
		// prior build that used a different codec (the format rolls forward by
		// re-rendering, never by a flag day).
		slog.Debug("disk render cache: decompress", "path", p, "err", err)
		return nil, false
	}
	// Touch the file so LRU eviction by mtime treats this entry as fresh.
	// Best-effort — a missing chtimes (race with sweep, EROFS) just falls back
	// to the original mtime.
	now := nowFn()
	_ = os.Chtimes(p, now, now) //nolint:gosec // path derived from sha256 hex of caller-controlled key
	return out, true
}

// Put zstd-compresses payload and atomically writes it to the sharded path.
// Subsequent reads either observe the previous complete file or the new one —
// never a partial. After the write we kick a background sweep (single-flight via
// sweepGate) so the fast path doesn't block on directory walks.
//
// nil-receiver no-ops so call sites can unconditionally Put without guarding the
// wiring constructor.
func (s *Store) Put(key string, payload []byte) {
	if s == nil {
		return
	}
	s.rootOnce.Do(func() {
		if err := os.MkdirAll(s.root, 0o750); err != nil {
			slog.Debug("disk render cache: mkdir root", "root", s.root, "err", err)
		}
	})

	p := s.pathFor(key)
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		slog.Debug("disk render cache: mkdir shard", "dir", dir, "err", err)
		return
	}

	// EncodeAll with the shared encoder is allocation-light and needs no
	// Close; rendered manifests are repetitive text that zstd packs ~2x tighter
	// than gzip, and the compress cost is dominated by the render we just
	// skipped on this (miss) path anyway.
	comp := zstdEncoder.EncodeAll(payload, nil)

	// syncDir=false: a render cache miss is cheap to re-derive on the next
	// invocation, so we trade durability for write throughput. Mirrors
	// atomic.WriteFile's documented "high-churn cache" mode.
	//
	// atomic.WriteFile removes its staged tmpfile on any error path (see
	// pkg/source/atomic/file.go's committed-bool defer guard), so a failed Put
	// can't leak partial state into the cache root.
	if err := atomic.WriteFile(p, comp, 0o600, false); err != nil {
		slog.Debug("disk render cache: write", "path", p, "err", err)
		return
	}

	// Kick a sweep if one isn't already running. The gate keeps the trigger
	// single-flight: a burst of Puts past the limit will schedule exactly one
	// sweep, which sees every entry written before it starts walking and prunes
	// the oldest until total ≤ limit.
	if s.sweepGate.TryAcquire() {
		go s.sweep()
	}
}

// sweep walks the cache root, totals byte usage, and (if over the limit) deletes
// oldest-by-mtime entries until total ≤ limit. Runs on a single goroutine —
// sweepGate gates re-trigger so a sustained write storm doesn't fork ~one sweep
// per Put. The flag clears at the end so the *next* over-limit Put can
// re-trigger.
//
// Errors are swallowed (logged at Debug) — the cache is best-effort and a stat /
// unlink failure on one entry shouldn't stop the rest of the sweep.
func (s *Store) sweep() {
	defer s.sweepGate.Release()

	var (
		entries []entry
		total   int64
	)
	walkErr := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
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
			Path:  path,
			Size:  info.Size(),
			MTime: info.ModTime().UnixNano(), // unix nanos for stable sort
		})
		total += info.Size()
		return nil
	})
	if walkErr != nil {
		slog.Debug("disk render cache: sweep walk", "root", s.root, "err", walkErr)
	}

	// Oldest first — those evict before the most recently used. Stable against
	// ties (sort by mtime then path) so two entries written within the same
	// nanosecond don't evict in platform-dependent order.
	evictOldest(entries, total, s.limit,
		func(a, b entry) int {
			return cmp.Or(
				cmp.Compare(a.MTime, b.MTime),
				cmp.Compare(a.Path, b.Path),
			)
		},
		func(e entry) error {
			if err := os.Remove(e.Path); err != nil {
				slog.Debug("disk render cache: sweep remove", "path", e.Path, "err", err)
				return err
			}
			return nil
		},
	)
}

// nowFn is the wall-clock used by Get's mtime bump. Pulled out so tests that
// need a deterministic clock can rebind it; production code reads time.Now
// directly.
var nowFn = time.Now
