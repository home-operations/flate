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

import (
	"os"
	"path/filepath"

	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// newOverlayFS returns a filesystem that reads from disk and writes to memory.
func newOverlayFS(disk filesys.FileSystem) filesys.FileSystem {
	return overlayFS{disk: disk, memory: filesys.MakeFsInMemory()}
}

// overlayFS layers an in-memory filesystem over a read-only disk filesystem.
type overlayFS struct {
	disk   filesys.FileSystem
	memory filesys.FileSystem
}

// Write operations: memory only.

func (fs overlayFS) Create(path string) (filesys.File, error) { return fs.memory.Create(path) }
func (fs overlayFS) Mkdir(path string) error                  { return fs.memory.Mkdir(path) }
func (fs overlayFS) MkdirAll(path string) error               { return fs.memory.MkdirAll(path) }
func (fs overlayFS) RemoveAll(path string) error              { return fs.memory.RemoveAll(path) }
func (fs overlayFS) WriteFile(path string, d []byte) error    { return fs.memory.WriteFile(path, d) }

// Read operations: memory first, then disk.

func (fs overlayFS) Exists(path string) bool { return fs.memory.Exists(path) || fs.disk.Exists(path) }
func (fs overlayFS) IsDir(path string) bool  { return fs.memory.IsDir(path) || fs.disk.IsDir(path) }

func (fs overlayFS) Open(path string) (filesys.File, error) {
	if fs.memory.Exists(path) {
		return fs.memory.Open(path)
	}
	return fs.disk.Open(path)
}

func (fs overlayFS) ReadFile(path string) ([]byte, error) {
	if fs.memory.Exists(path) {
		return fs.memory.ReadFile(path)
	}
	return fs.disk.ReadFile(path)
}

// CleanedAbs resolves memory paths against the memory layer (so added files —
// pre-fetched resources, git bases — are resolvable) and everything else
// against the secure disk layer.
func (fs overlayFS) CleanedAbs(path string) (filesys.ConfirmedDir, string, error) {
	if fs.memory.Exists(path) {
		return fs.memory.CleanedAbs(path)
	}
	return fs.disk.CleanedAbs(path)
}

func (fs overlayFS) ReadDir(path string) ([]string, error) {
	return mergeFSResults(fs.memory.ReadDir(path))(fs.disk.ReadDir(path))
}

func (fs overlayFS) Glob(pattern string) ([]string, error) {
	return mergeFSResults(fs.memory.Glob(pattern))(fs.disk.Glob(pattern))
}

func (fs overlayFS) Walk(path string, walkFn filepath.WalkFunc) error {
	visited := make(map[string]struct{})
	if fs.memory.Exists(path) {
		if err := fs.memory.Walk(path, func(p string, info os.FileInfo, err error) error {
			visited[p] = struct{}{}
			return walkFn(p, info, err)
		}); err != nil {
			return err
		}
	}
	return fs.disk.Walk(path, func(p string, info os.FileInfo, err error) error {
		if _, ok := visited[p]; ok {
			return nil
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
