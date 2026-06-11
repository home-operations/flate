package kustomize

// render_cache.go persists kustomize render output across `flate` invocations,
// giving kustomize the cross-run cache helm already has. Each entry pairs the
// rendered YAML with the readSet snapshot of the disk inputs that produced it
// (see readset.go); RenderFlux validates a candidate by replaying that snapshot
// against the live tree, so a hit provably reproduces a fresh render. Layout,
// gzip framing, atomic writes, and the single-flight LRU sweep mirror
// pkg/helm/render_cache_disk.go.

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/home-operations/flate/internal/diskcache"
	"github.com/home-operations/flate/pkg/source/atomic"
)

// renderCache is a disk-backed, content-validated kustomize render cache. A nil
// *renderCache is the "disabled" sentinel: every method no-ops / misses, so call
// sites need not guard the wiring.
type renderCache struct {
	root  string
	limit int64 // total disk bytes; <=0 disables caching

	sweepGate diskcache.Gate
	rootOnce  sync.Once
}

// newRenderCache returns a cache rooted at root with the supplied byte cap, or
// nil (disabled) for an empty root or non-positive limit. The root is created
// lazily on the first Put.
func newRenderCache(root string, limitBytes int64) *renderCache {
	if root == "" || limitBytes <= 0 {
		return nil
	}
	return &renderCache{root: root, limit: limitBytes}
}

// pathFor shards by the first two key chars so no directory balloons past ~16k
// peers — readdir stays cheap even for caches in the millions.
func (c *renderCache) pathFor(key string) string {
	if len(key) < 2 {
		return filepath.Join(c.root, "00", key)
	}
	return filepath.Join(c.root, key[:2], key)
}

// get returns the cached snapshot + output for key, or ok=false on any miss
// (including I/O / decode errors, surfaced at Debug). A nil receiver misses.
func (c *renderCache) get(key string) (snap readSetSnapshot, output []byte, ok bool) {
	if c == nil {
		return readSetSnapshot{}, nil, false
	}
	p := c.pathFor(key)
	raw, err := os.ReadFile(p) //nolint:gosec // path derived from sha256 hex of caller-controlled key
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Debug("kustomize render cache: read", "path", p, "err", err)
		}
		return readSetSnapshot{}, nil, false
	}
	snap, output, err = decodeEntry(raw)
	if err != nil {
		slog.Debug("kustomize render cache: decode", "path", p, "err", err)
		return readSetSnapshot{}, nil, false
	}
	// Bump mtime so the LRU sweep treats this entry as freshly used.
	now := nowFn()
	_ = os.Chtimes(p, now, now) //nolint:gosec // path derived from sha256 hex of caller-controlled key
	return snap, output, true
}

// put stores snapshot + output under key. A nil receiver no-ops.
func (c *renderCache) put(key string, snap readSetSnapshot, output []byte) {
	if c == nil {
		return
	}
	payload, err := encodeEntry(snap, output)
	if err != nil {
		slog.Debug("kustomize render cache: encode", "err", err)
		return
	}
	c.rootOnce.Do(func() {
		if err := os.MkdirAll(c.root, 0o750); err != nil {
			slog.Debug("kustomize render cache: mkdir root", "root", c.root, "err", err)
		}
	})
	p := c.pathFor(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		slog.Debug("kustomize render cache: mkdir shard", "dir", filepath.Dir(p), "err", err)
		return
	}
	// syncDir=false: a miss is cheap to re-derive, so trade durability for write
	// throughput (mirrors the helm render cache).
	if err := atomic.WriteFile(p, payload, 0o600, false); err != nil {
		slog.Debug("kustomize render cache: write", "path", p, "err", err)
		return
	}
	if c.sweepGate.TryAcquire() {
		go c.sweep()
	}
}

// encodeEntry frames the entry as gzip( uint32(len(snapshotJSON)) | snapshotJSON
// | rawOutput ). Output stays raw (not base64) so gzip compresses it as text.
func encodeEntry(snap readSetSnapshot, output []byte) ([]byte, error) {
	js, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	if len(js) > math.MaxUint32 {
		return nil, errors.New("read-set snapshot too large to frame")
	}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(js))) //nolint:gosec // bounded by the MaxUint32 check above
	if _, err := gw.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err := gw.Write(js); err != nil {
		return nil, err
	}
	if _, err := gw.Write(output); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeEntry(raw []byte) (readSetSnapshot, []byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return readSetSnapshot{}, nil, err
	}
	defer func() { _ = gz.Close() }()
	plain, err := io.ReadAll(gz)
	if err != nil {
		return readSetSnapshot{}, nil, err
	}
	if len(plain) < 4 {
		return readSetSnapshot{}, nil, errors.New("entry too short")
	}
	n := binary.BigEndian.Uint32(plain[:4])
	if int(n) > len(plain)-4 {
		return readSetSnapshot{}, nil, errors.New("entry header length out of range")
	}
	var snap readSetSnapshot
	if err := json.Unmarshal(plain[4:4+n], &snap); err != nil {
		return readSetSnapshot{}, nil, err
	}
	output := plain[4+n:]
	return snap, output, nil
}

// sweep totals the cache and evicts oldest-by-mtime entries until within limit.
// Single-flight via sweepGate; errors are best-effort (logged at Debug).
func (c *renderCache) sweep() {
	defer c.sweepGate.Release()
	var (
		entries []diskcache.Entry
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
		entries = append(entries, diskcache.Entry{Path: path, Size: info.Size(), MTime: info.ModTime().UnixNano()})
		total += info.Size()
		return nil
	})
	if walkErr != nil {
		slog.Debug("kustomize render cache: sweep walk", "root", c.root, "err", walkErr)
	}
	diskcache.EvictOldest(entries, total, c.limit,
		func(a, b diskcache.Entry) int {
			return cmp.Or(cmp.Compare(a.MTime, b.MTime), cmp.Compare(a.Path, b.Path))
		},
		func(e diskcache.Entry) error {
			if err := os.Remove(e.Path); err != nil {
				slog.Debug("kustomize render cache: sweep remove", "path", e.Path, "err", err)
				return err
			}
			return nil
		},
	)
}

// nowFn is the wall-clock for Get's mtime bump; tests rebind it.
var nowFn = time.Now
