package kustomize

// render_cache.go persists kustomize render output across `flate` invocations,
// giving kustomize the cross-run cache helm already has. Each entry pairs the
// rendered YAML with the readSet snapshot of the disk inputs that produced it
// (see readset.go); RenderFlux validates a candidate by replaying that snapshot
// against the live tree, so a hit provably reproduces a fresh render.
//
// renderCache is the kustomize-domain layer: the read-set framing (encodeFrame /
// decodeFrame) on top of the shared diskcache.Store, which owns the sharded
// layout, zstd compression, atomic writes, and the mtime-LRU sweep. helm's
// render cache sits on the same Store with a plain-bytes value instead of a frame.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"math"

	"github.com/home-operations/flate/internal/diskcache"
)

// renderCache is a disk-backed, content-validated kustomize render cache. A nil
// *renderCache is the "disabled" sentinel: every method no-ops / misses, so call
// sites need not guard the wiring.
type renderCache struct {
	store *diskcache.Store
}

// newRenderCache returns a cache rooted at root with the supplied byte cap, or
// nil (disabled) for an empty root or non-positive limit. The nil is load-
// bearing: RenderFlux/tree.go gate on `c.render != nil` by identity, so a
// disabled Store must collapse the whole renderCache to nil rather than wrap a
// nil Store.
func newRenderCache(root string, limitBytes int64) *renderCache {
	s := diskcache.NewStore(root, limitBytes)
	if s == nil {
		return nil
	}
	return &renderCache{store: s}
}

// get returns the cached snapshot + output for key, or ok=false on any miss
// (including a decode error, surfaced at Debug). A nil receiver misses.
func (c *renderCache) get(key string) (snap readSetSnapshot, output []byte, ok bool) {
	if c == nil {
		return readSetSnapshot{}, nil, false
	}
	raw, hit := c.store.Get(key)
	if !hit {
		return readSetSnapshot{}, nil, false
	}
	snap, output, err := decodeFrame(raw)
	if err != nil {
		slog.Debug("kustomize render cache: decode", "err", err)
		return readSetSnapshot{}, nil, false
	}
	return snap, output, true
}

// put stores snapshot + output under key. A nil receiver no-ops.
func (c *renderCache) put(key string, snap readSetSnapshot, output []byte) {
	if c == nil {
		return
	}
	payload, err := encodeFrame(snap, output)
	if err != nil {
		slog.Debug("kustomize render cache: encode", "err", err)
		return
	}
	c.store.Put(key, payload)
}

// encodeFrame frames the entry as uint32(len(snapshotJSON)) | snapshotJSON |
// rawOutput. The Store compresses this plain frame on write (and decompresses on
// read), so the output stays raw here — zstd compresses it as text.
func encodeFrame(snap readSetSnapshot, output []byte) ([]byte, error) {
	js, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	if len(js) > math.MaxUint32 {
		return nil, errors.New("read-set snapshot too large to frame")
	}
	buf := make([]byte, 4+len(js)+len(output))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(js))) //nolint:gosec // bounded by the MaxUint32 check above
	copy(buf[4:], js)
	copy(buf[4+len(js):], output)
	return buf, nil
}

// decodeFrame is the inverse of encodeFrame over the Store's already-decompressed
// bytes: read the 4-byte length header, unmarshal that many bytes as the
// snapshot JSON, and return the remainder as the rendered output.
func decodeFrame(plain []byte) (readSetSnapshot, []byte, error) {
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
