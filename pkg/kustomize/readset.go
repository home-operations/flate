package kustomize

// readset.go records exactly which on-disk inputs a single RenderFlux build
// reads, so a later run can validate a cached render by replaying those reads:
// if every recorded file's content, every listed directory's entries, and every
// probed path's node kind still match, the build's inputs are unchanged and the
// cached output is still valid. Because the transitive input set is captured
// from the build's *actual* reads (not guessed from the spec), it is sound
// across the cases a naive subtree hash misses — `..`-escaping resources,
// transitive components, and files added to or removed from an auto-scanned
// directory.
//
// Only disk-layer reads are recorded. The in-memory overlay holds the generated
// kustomization.yaml — already captured by the cache key, which hashes the Flux
// spec — and, in the uncacheable remote-resource case, pre-fetched content (that
// build bypasses the cache; see RenderFlux). An unexpected read the recorder
// can't represent sets bypass, disabling caching for that render rather than
// risking a stale hit.

import (
	"cmp"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// nodeKind classifies a probed path so a dir↔file↔absent transition the build
// would observe cannot slip through as a stale hit.
type nodeKind uint8

const (
	kindAbsent nodeKind = iota
	kindFile
	kindDir
)

func diskKind(exists, isDir bool) nodeKind {
	switch {
	case !exists:
		return kindAbsent
	case isDir:
		return kindDir
	default:
		return kindFile
	}
}

// hashEntries is the order-independent digest of a directory's child names. The
// record side (recordDir) and the validate side (stillValid) must agree
// byte-for-byte, so both go through this one definition.
func hashEntries(entries []string) string {
	sorted := slices.Clone(entries)
	slices.Sort(sorted)
	return manifest.SHA256Hex([]byte(strings.Join(sorted, "\x00")))
}

// readSet accumulates a build's disk reads. Safe for the concurrent access a
// single krusty build performs through the overlay.
type readSet struct {
	root string // source root; recorded paths are stored relative to it

	mu     sync.Mutex
	files  map[string]string   // relpath -> sha256(content), or "" when the read errored
	dirs   map[string]string   // relpath -> sha256(sorted child names)
	nodes  map[string]nodeKind // relpath -> kind, for paths only probed (not read/listed)
	bypass bool                // an uninstrumentable read occurred — do not cache
}

func newReadSet(root string) *readSet {
	return &readSet{
		root:  root,
		files: map[string]string{},
		dirs:  map[string]string{},
		nodes: map[string]nodeKind{},
	}
}

// rel converts an absolute path the overlay saw into a slash-relative path under
// root. A path outside root (shouldn't happen under the secure FS) is reported
// not-ok so the caller can bypass.
func (rs *readSet) rel(abs string) (string, bool) {
	r, err := filepath.Rel(rs.root, abs)
	if err != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(r), true
}

func (rs *readSet) recordFile(abs string, content []byte, readErr error) {
	rel, ok := rs.rel(abs)
	if !ok {
		rs.markBypass()
		return
	}
	h := ""
	if readErr == nil {
		h = manifest.SHA256Hex(content)
	}
	rs.mu.Lock()
	rs.files[rel] = h
	rs.mu.Unlock()
}

func (rs *readSet) recordDir(abs string, entries []string) {
	rel, ok := rs.rel(abs)
	if !ok {
		rs.markBypass()
		return
	}
	h := hashEntries(entries)
	rs.mu.Lock()
	rs.dirs[rel] = h
	rs.mu.Unlock()
}

func (rs *readSet) recordNode(abs string, kind nodeKind) {
	rel, ok := rs.rel(abs)
	if !ok {
		rs.markBypass()
		return
	}
	rs.mu.Lock()
	rs.nodes[rel] = kind
	rs.mu.Unlock()
}

func (rs *readSet) markBypass() {
	rs.mu.Lock()
	rs.bypass = true
	rs.mu.Unlock()
}

func (rs *readSet) bypassed() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.bypass
}

// fileEntry / nodeEntry are the serialized, sorted snapshot the disk cache
// stores beside a render's output. Slices (not maps) so the encoding is
// deterministic.
type fileEntry struct {
	Path string `json:"p"`
	Hash string `json:"h"`
}
type nodeEntry struct {
	Path string   `json:"p"`
	Kind nodeKind `json:"k"`
}

// readSetSnapshot is the cache-stored, deterministic form of a readSet.
type readSetSnapshot struct {
	Files []fileEntry `json:"files"`
	Dirs  []fileEntry `json:"dirs"`
	Nodes []nodeEntry `json:"nodes"`
}

// snapshot freezes the recorder into its deterministic serialized form.
func (rs *readSet) snapshot() readSetSnapshot {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	snap := readSetSnapshot{
		Files: make([]fileEntry, 0, len(rs.files)),
		Dirs:  make([]fileEntry, 0, len(rs.dirs)),
		Nodes: make([]nodeEntry, 0, len(rs.nodes)),
	}
	for p, h := range rs.files {
		snap.Files = append(snap.Files, fileEntry{Path: p, Hash: h})
	}
	for p, h := range rs.dirs {
		snap.Dirs = append(snap.Dirs, fileEntry{Path: p, Hash: h})
	}
	for p, k := range rs.nodes {
		// A bare node bit is redundant when the same path was recorded as a read
		// file or listed dir (those validate strictly already).
		if _, isFile := rs.files[p]; isFile {
			continue
		}
		if _, isDir := rs.dirs[p]; isDir {
			continue
		}
		snap.Nodes = append(snap.Nodes, nodeEntry{Path: p, Kind: k})
	}
	slices.SortFunc(snap.Files, func(a, b fileEntry) int { return cmp.Compare(a.Path, b.Path) })
	slices.SortFunc(snap.Dirs, func(a, b fileEntry) int { return cmp.Compare(a.Path, b.Path) })
	slices.SortFunc(snap.Nodes, func(a, b nodeEntry) int { return cmp.Compare(a.Path, b.Path) })
	return snap
}

// diskReader is the read subset of filesys.FileSystem a snapshot replays
// against to validate a cached render.
type diskReader interface {
	ReadFile(path string) ([]byte, error)
	ReadDir(path string) ([]string, error)
	Exists(path string) bool
	IsDir(path string) bool
}

// stillValid replays the snapshot against the live disk under root: a cache HIT
// iff every recorded file still hashes the same, every recorded directory still
// lists the same children, and every probed path is still the same kind. Any
// edit, add, or delete krusty would have observed flips one of these, forcing a
// re-render — so a HIT provably reproduces the cached output.
func (snap readSetSnapshot) stillValid(disk diskReader, root string) bool {
	for _, f := range snap.Files {
		b, err := disk.ReadFile(filepath.Join(root, filepath.FromSlash(f.Path)))
		if (err != nil) != (f.Hash == "") {
			return false // file appeared or vanished since recording
		}
		if err == nil && manifest.SHA256Hex(b) != f.Hash {
			return false // content changed
		}
	}
	for _, d := range snap.Dirs {
		entries, err := disk.ReadDir(filepath.Join(root, filepath.FromSlash(d.Path)))
		if err != nil {
			return false
		}
		if hashEntries(entries) != d.Hash {
			return false // a child was added or removed
		}
	}
	for _, n := range snap.Nodes {
		abs := filepath.Join(root, filepath.FromSlash(n.Path))
		if diskKind(disk.Exists(abs), disk.IsDir(abs)) != n.Kind {
			return false // a probed path appeared, vanished, or changed file↔dir
		}
	}
	return true
}
