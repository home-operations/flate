// Package cacheroot owns the layout of flate's on-disk cache. Every
// other package — fetchers, baseline materialization, GC, helm — asks
// a Layout for the path of the subtree it operates on; nothing else
// constructs those paths by hand. The intent is that renaming a
// subdirectory means editing one method here, not chasing string
// literals across seven files (and silently breaking the sweeper in
// the process).
//
// Layout is a zero-cost value type. Construct via Layout{Root: …} or
// New(root); pass by value freely.
package cacheroot

import (
	"os"
	"path/filepath"
)

// Default returns the on-disk cache root for embedders that don't
// override it. Prefers the OS user cache dir ($XDG_CACHE_HOME on
// Linux, ~/Library/Caches on macOS, %LocalAppData% on Windows) with
// a "flate" subdir, so caches survive reboots and OS tmpfs cleanups.
// Falls back to $TMPDIR/flate-cache only when UserCacheDir errors
// (HOME unset, etc.).
//
// One canonical implementation here; the CLI and the orchestrator
// both consume it, and tests that want a determinic root pass an
// explicit Root via Layout{Root: …} or New(...).
func Default() string {
	if d, err := os.UserCacheDir(); err == nil && d != "" {
		return filepath.Join(d, "flate")
	}
	return filepath.Join(os.TempDir(), "flate-cache")
}

// Layout describes where the various caches live under a single root.
// All methods return absolute paths derived from Root + a constant
// subdirectory name. Methods that take a key (slug, hash, digest, …)
// append it; methods that return a parent directory are the GC and
// listing entry points.
type Layout struct {
	// Root is the on-disk cache root (typically $XDG_CACHE_HOME/flate
	// or the user-supplied --cache-dir override).
	Root string
}

// New is a convenience constructor; same as Layout{Root: root}.
func New(root string) Layout { return Layout{Root: root} }

// Subdirectory names. Exported as constants so a future audit of "who
// touches the sources directory" can grep these specifically; writers
// and the sweeper consume them via the Layout methods, not directly.
const (
	SourcesDir    = "sources"
	BaselinesDir  = "baselines"
	BlobsDir      = "blobs"
	BlobsAlgo     = "sha256"
	RefsDir       = "refs"
	GitMirrorsDir = "git-mirrors"
	HelmTmpDir    = "helm-tmp"
	HelmCacheDir  = "helm-cache"
	StageDir      = "stage"
)

// Sources returns the parent directory of every source slot. Used by
// GC's age sweep and listing tools.
func (l Layout) Sources() string { return filepath.Join(l.Root, SourcesDir) }

// SourceSlot returns the on-disk slot for a given (slug, hash) pair.
// slug is a human-readable repo name; hash is the content-keyed
// identifier source.Cache computes from (url, ref, authID).
func (l Layout) SourceSlot(slug, hash string) string {
	return filepath.Join(l.Root, SourcesDir, slug, hash)
}

// Baselines returns the parent directory of every materialized
// baseline tree.
func (l Layout) Baselines() string { return filepath.Join(l.Root, BaselinesDir) }

// Baseline returns the on-disk path for a baseline tree keyed by its
// commit sha.
func (l Layout) Baseline(commitSHA string) string {
	return filepath.Join(l.Root, BaselinesDir, commitSHA)
}

// Blobs returns the parent of every content-addressed blob.
// Always includes the algorithm segment so blobs/sha512/ etc. can land
// here later without rewriting the GC's walk.
func (l Layout) Blobs() string { return filepath.Join(l.Root, BlobsDir, BlobsAlgo) }

// Blob returns the on-disk directory for a single blob keyed by its
// hex sha256 digest.
func (l Layout) Blob(digest string) string {
	return filepath.Join(l.Root, BlobsDir, BlobsAlgo, digest)
}

// RefsRoot returns the parent of every refs table. Walked by GC to
// clean dangling pointers.
func (l Layout) RefsRoot() string { return filepath.Join(l.Root, RefsDir) }

// RefsCategory carves out a subdirectory under refs/ for one
// caller's identity→digest mapping. The first arg is a stable name
// (e.g. "chart-tarballs", "git-revisions") shared between the writer
// and any introspection tooling.
func (l Layout) RefsCategory(name string) string {
	return filepath.Join(l.Root, RefsDir, name)
}

// GitMirrors returns the parent of every bare git mirror.
func (l Layout) GitMirrors() string { return filepath.Join(l.Root, GitMirrorsDir) }

// GitMirror returns the on-disk path for a bare mirror keyed by the
// stable hash of an upstream URL.
func (l Layout) GitMirror(urlHash string) string {
	return filepath.Join(l.Root, GitMirrorsDir, urlHash)
}

// HelmTmp returns the scratch directory the helm client uses for
// transient writes (index.yaml downloads, TLS cert materialization).
func (l Layout) HelmTmp() string { return filepath.Join(l.Root, HelmTmpDir) }

// HelmCache returns the helm client's cacheDir — the on-disk root the
// chart tarball CAS and chart-tarball refs table sit under.
func (l Layout) HelmCache() string { return filepath.Join(l.Root, HelmCacheDir) }

// Stage returns the kustomize staging root. Per-render stage dirs land
// inside as flate-stage-<rand>.
func (l Layout) Stage() string { return filepath.Join(l.Root, StageDir) }
