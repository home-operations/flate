package kustomize

// overlayfs.go is a memory-over-disk filesystem for in-memory kustomize builds.
// Reads check an in-memory layer first, then fall back to a read-only disk
// layer; writes go only to memory, so the on-disk source tree is never touched.
//
// It mirrors fluxcd/pkg/kustomize/filesys.MakeFsInMemory with one essential
// difference: CleanedAbs (which kustomize's loader uses to resolve every
// resource path) checks the memory layer first. Flux's version delegates
// CleanedAbs to disk only, so files that exist solely in memory — flate's
// pre-fetched remote resources and materialized git bases (see preflight.go /
// gitbase.go) — cannot be resolved as resources. Checking memory first lets the
// build load them while still reading the bulk of the tree from disk (which
// also sidesteps the in-memory fs's filename restriction for exotic source
// names like spaces). The disk layer is a secure FS, so disk reads stay
// confined to the source root.
//
// When a non-nil *readSet is threaded in, every read served from the DISK layer
// is recorded (file content, directory listing, or probed-path kind) so the
// render cache can validate a later run by replaying those reads. Memory-layer
// reads are not recorded: the generated kustomization.yaml is captured by the
// cache key, and remote-resource builds bypass the cache entirely. With a nil
// readSet the methods are byte-identical to the uninstrumented overlay.

import (
	"os"
	"path/filepath"

	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// newOverlayFS returns a filesystem that reads from disk and writes to memory.
// rec, when non-nil, records disk-layer reads for render-cache validation.
func newOverlayFS(disk filesys.FileSystem, rec *readSet) filesys.FileSystem {
	return overlayFS{disk: disk, memory: filesys.MakeFsInMemory(), rec: rec}
}

// overlayFS layers an in-memory filesystem over a read-only disk filesystem.
type overlayFS struct {
	disk   filesys.FileSystem
	memory filesys.FileSystem
	rec    *readSet // nil disables recording (and keeps reads byte-identical)
}

// Write operations: memory only.

func (fs overlayFS) Create(path string) (filesys.File, error) { return fs.memory.Create(path) }
func (fs overlayFS) Mkdir(path string) error                  { return fs.memory.Mkdir(path) }
func (fs overlayFS) MkdirAll(path string) error               { return fs.memory.MkdirAll(path) }
func (fs overlayFS) RemoveAll(path string) error              { return fs.memory.RemoveAll(path) }
func (fs overlayFS) WriteFile(path string, d []byte) error    { return fs.memory.WriteFile(path, d) }

// Read operations: memory first, then disk. Disk reads are recorded.

// recordProbe records the disk-layer node kind of a path that was only probed
// (existence / type checked, not read or listed). diskExists is the already-
// computed disk.Exists result so the common path adds at most one IsDir stat.
func (fs overlayFS) recordProbe(path string, diskExists bool) {
	if fs.rec == nil {
		return
	}
	isDir := diskExists && fs.disk.IsDir(path)
	fs.rec.recordNode(path, diskKind(diskExists, isDir))
}

func (fs overlayFS) Exists(path string) bool {
	if fs.memory.Exists(path) {
		return true
	}
	e := fs.disk.Exists(path)
	fs.recordProbe(path, e)
	return e
}

func (fs overlayFS) IsDir(path string) bool {
	if fs.memory.IsDir(path) {
		return true
	}
	d := fs.disk.IsDir(path)
	if fs.rec != nil {
		fs.rec.recordNode(path, diskKind(d || fs.disk.Exists(path), d))
	}
	return d
}

func (fs overlayFS) Open(path string) (filesys.File, error) {
	if fs.memory.Exists(path) {
		return fs.memory.Open(path)
	}
	if fs.rec != nil {
		// Open streams content the recorder can't observe; re-read once to hash
		// it (rare — krusty Opens at most a handful of files per build).
		b, err := fs.disk.ReadFile(path)
		fs.rec.recordFile(path, b, err)
	}
	return fs.disk.Open(path)
}

func (fs overlayFS) ReadFile(path string) ([]byte, error) {
	if fs.memory.Exists(path) {
		return fs.memory.ReadFile(path)
	}
	b, err := fs.disk.ReadFile(path)
	if fs.rec != nil {
		fs.rec.recordFile(path, b, err)
	}
	return b, err
}

// CleanedAbs resolves memory paths against the memory layer (so added files —
// pre-fetched resources, git bases — are resolvable) and everything else
// against the secure disk layer.
func (fs overlayFS) CleanedAbs(path string) (filesys.ConfirmedDir, string, error) {
	if fs.memory.Exists(path) {
		return fs.memory.CleanedAbs(path)
	}
	d, s, err := fs.disk.CleanedAbs(path)
	fs.recordProbe(path, err == nil)
	return d, s, err
}

func (fs overlayFS) ReadDir(path string) ([]string, error) {
	if entries, err := fs.disk.ReadDir(path); err == nil && fs.rec != nil {
		fs.rec.recordDir(path, entries)
	}
	return mergeFSResults(fs.memory.ReadDir(path))(fs.disk.ReadDir(path))
}

func (fs overlayFS) Glob(pattern string) ([]string, error) {
	// Glob results aren't a single path we can replay soundly; disable caching
	// for any build that globs rather than risk a stale hit.
	if fs.rec != nil {
		fs.rec.markBypass()
	}
	return mergeFSResults(fs.memory.Glob(pattern))(fs.disk.Glob(pattern))
}

func (fs overlayFS) Walk(path string, walkFn filepath.WalkFunc) error {
	visited := make(map[string]struct{})
	// skipped records directories whose memory-layer visit returned SkipDir.
	// The disk walk must honor that too: a dir the caller asked to skip in
	// memory must not have its disk subtree descended into — otherwise a
	// SkipDir'd kustomize package (added once by scanManifests) is re-walked
	// on disk and its kustomization.yaml is picked up as a stray resource.
	skipped := make(map[string]struct{})
	if fs.memory.Exists(path) {
		if err := fs.memory.Walk(path, func(p string, info os.FileInfo, err error) error {
			visited[p] = struct{}{}
			ret := walkFn(p, info, err)
			if ret == filepath.SkipDir && info != nil && info.IsDir() {
				skipped[p] = struct{}{}
			}
			return ret
		}); err != nil {
			return err
		}
	}
	return fs.disk.Walk(path, func(p string, info os.FileInfo, err error) error {
		if _, ok := skipped[p]; ok {
			return filepath.SkipDir
		}
		if _, ok := visited[p]; ok {
			return nil
		}
		// Record each disk directory entered so an added/removed sibling
		// invalidates the cached render (file content is recorded via ReadFile
		// when the walk body reads it).
		if fs.rec != nil && info != nil && info.IsDir() {
			if entries, derr := fs.disk.ReadDir(p); derr == nil {
				fs.rec.recordDir(p, entries)
			}
		}
		return walkFn(p, info, err)
	})
}

// mergeFSResults deduplicates two ([]string, error) results, preferring the
// first set. Returns a closure so both calls can be inlined at the call site.
func mergeFSResults(primary []string, pErr error) func([]string, error) ([]string, error) {
	return func(secondary []string, sErr error) ([]string, error) {
		if pErr != nil && sErr != nil {
			return nil, sErr
		}
		seen := make(map[string]struct{}, len(primary))
		merged := make([]string, 0, len(primary)+len(secondary))
		for _, e := range primary {
			seen[e] = struct{}{}
			merged = append(merged, e)
		}
		for _, e := range secondary {
			if _, ok := seen[e]; !ok {
				merged = append(merged, e)
			}
		}
		return merged, nil
	}
}
