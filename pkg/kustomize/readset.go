package kustomize

import (
	"cmp"
	"context"
	"io/fs"
	"path/filepath"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
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
	return manifest.Fingerprint(sorted)
}

// readSet accumulates a build's disk reads. Safe for the concurrent access a
// single krusty build performs through the overlay.
type readSet struct {
	root      string // source root; recorded paths are stored relative to it
	immutable bool

	mu    sync.Mutex
	files map[string]string   // relpath -> sha256(content), or "" when the read errored
	dirs  map[string]string   // relpath -> sha256(sorted child names)
	nodes map[string]nodeKind // relpath -> kind, for paths only probed (not read/listed)

	// Remote observations are collected only during serial preflight.
	remotes map[remoteInput]remoteIdentity
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
// root. treeFS rejects escaping paths before observing source content.
func (rs *readSet) rel(abs string) (string, bool) {
	r, err := filepath.Rel(rs.root, abs)
	if err != nil || safepath.Escaped(r) {
		return "", false
	}
	return filepath.ToSlash(r), true
}

func (rs *readSet) recordFile(abs string, content []byte, readErr error) {
	rel, ok := rs.rel(abs)
	if !ok {
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
		return
	}
	rs.mu.Lock()
	rs.nodes[rel] = kind
	rs.mu.Unlock()
}

func (rs *readSet) recordRemote(input remoteInput, identity remoteIdentity) {
	if rs.remotes == nil {
		rs.remotes = make(map[remoteInput]remoteIdentity)
	}
	rs.remotes[input] = identity
}

type remoteEntry struct {
	Input    remoteInput    `json:"input"`
	Identity remoteIdentity `json:"identity"`
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

// readSetSnapshot is the deterministic read set persisted beside the output.
type readSetSnapshot struct {
	Files   []fileEntry   `json:"files"`
	Dirs    []fileEntry   `json:"dirs"`
	Nodes   []nodeEntry   `json:"nodes"`
	Remotes []remoteEntry `json:"remotes,omitempty"`
}

// snapshot freezes the recorder into its deterministic serialized form.
func (rs *readSet) snapshot() readSetSnapshot {
	var remotes []remoteEntry
	for input, identity := range rs.remotes {
		remotes = append(remotes, remoteEntry{Input: input, Identity: identity})
	}
	slices.SortFunc(remotes, func(a, b remoteEntry) int {
		return cmp.Or(cmp.Compare(a.Input.Operation, b.Input.Operation), cmp.Compare(a.Input.URL, b.Input.URL), cmp.Compare(a.Input.Ref, b.Input.Ref))
	})
	rs.mu.Lock()
	defer rs.mu.Unlock()
	snap := readSetSnapshot{
		Remotes: remotes,
		Files:   make([]fileEntry, 0, len(rs.files)),
		Dirs:    make([]fileEntry, 0, len(rs.dirs)),
		Nodes:   make([]nodeEntry, 0, len(rs.nodes)),
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

func (snap readSetSnapshot) stillValid(ctx context.Context, cache *TreeCache, input *treeFS, immutable bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !immutable {
		lower := input.lower
		for _, f := range snap.Files {
			b, err := fs.ReadFile(lower, f.Path)
			if (err != nil) != (f.Hash == "") {
				return false, ctx.Err()
			}
			if err == nil && manifest.SHA256Hex(b) != f.Hash {
				return false, ctx.Err()
			}
		}
		for _, d := range snap.Dirs {
			entries, err := fs.ReadDir(lower, d.Path)
			if err != nil {
				return false, ctx.Err()
			}
			if hashEntries(entryNames(entries)) != d.Hash {
				return false, ctx.Err()
			}
		}
		for _, n := range snap.Nodes {
			info, err := fs.Stat(lower, n.Path)
			if diskKind(err == nil, err == nil && info.IsDir()) != n.Kind {
				return false, ctx.Err()
			}
		}
	}
	for _, remote := range snap.Remotes {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		_, identity, err := remote.Input.acquire(ctx, cache, input)
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if err != nil || identity != remote.Identity {
			return false, nil
		}
	}
	return ctx.Err() == nil, ctx.Err()
}
