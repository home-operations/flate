// Package cacheroot owns the layout of flate's on-disk cache. Every
// other package — fetchers, baseline materialization, GC, helm — asks
// a Layout for the path of the subtree it operates on; nothing else
// constructs those paths by hand. The intent is that renaming a
// subdirectory means editing one method here, not chasing string
// literals across seven files (and silently breaking the sweeper in
// the process).
//
// Layout is a zero-cost value type. Construct via New(root) or
// Layout{Root: …}; pass by value freely. The zero value (Root == "")
// is valid and means "no persistent cache"; callers that skip
// materialization for empty roots guard the Root field themselves.
package cacheroot

import (
	"os"
	"path/filepath"
	"strings"
)

// Default returns the on-disk cache root for embedders that don't
// override it. Prefers the OS user cache dir ($XDG_CACHE_HOME on
// Linux, ~/Library/Caches on macOS, %LocalAppData% on Windows) with
// a "flate" subdir, so caches survive reboots and OS tmpfs cleanups.
// Falls back to $TMPDIR/flate-cache only when UserCacheDir errors
// (HOME unset, etc.).
//
// One canonical implementation here; the CLI and the orchestrator
// both consume it, and tests that want a deterministic root pass an
// explicit Root via New(...) or Layout{Root: …}.
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

// New cleans root and returns a Layout. Prefer New over Layout{Root:}
// to ensure Root is canonical; the path methods skip a redundant
// filepath.Clean pass when Root is already clean.
func New(root string) Layout { return Layout{Root: filepath.Clean(root)} }

// Subdirectory names. Exported so audit tooling can grep for which
// packages touch a given subtree without chasing raw string literals.
// Writers and the sweeper consume these via Layout methods, not directly.
const (
	SourcesDir    = "sources"
	BaselinesDir  = "baselines"
	BlobsDir      = "blobs"
	BlobsAlgo     = "sha256"
	RefsDir       = "refs"
	GitMirrorsDir = "git-mirrors"
	HelmTmpDir    = "helm-tmp"
	// RenderHelmCacheDir holds the persisted helm template-output
	// cache. Entries are sharded `<root>/render/helm/<hex[:2]>/<hex>`
	// where <hex> is the full sha256 of the template-cache key. Content
	// is zstd-compressed rendered manifest bytes. Cross-process safe via
	// atomic rename; eviction is mtime-LRU bounded by the caller's byte cap.
	RenderHelmCacheDir = "render/helm"
	// RenderKustomizeCacheDir holds the persisted kustomize render-output
	// cache: `<root>/render/kustomize/<hex[:2]>/<hex>` where <hex> is the
	// sha256 render key; content is zstd-compressed {read-set snapshot +
	// rendered bytes}. An entry is reused only when its recorded disk inputs still
	// validate (see pkg/kustomize/render_cache.go) — same cross-process /
	// mtime-LRU machinery as the helm render cache.
	RenderKustomizeCacheDir = "render/kustomize"
)

// pathSep is the platform separator as a string, used by join so it can
// stay allocation-free for the separator itself.
const pathSep = string(filepath.Separator)

// join appends segs to a clean base separated by pathSep, without a
// filepath.Clean pass. New guarantees Root is clean; all callers pass
// string constants (or already-validated keys) as the segments. It
// pre-sizes the builder to the exact final length so each path is a
// single allocation.
func join(base string, segs ...string) string {
	n := len(base)
	for _, s := range segs {
		n += 1 + len(s)
	}
	var b strings.Builder
	b.Grow(n)
	b.WriteString(base)
	for _, s := range segs {
		b.WriteString(pathSep)
		b.WriteString(s)
	}
	return b.String()
}

// Sources returns the parent directory of every source slot. Used by
// GC's age sweep and listing tools.
func (l Layout) Sources() string { return join(l.Root, SourcesDir) }

// SourceSlot returns the on-disk slot for a given (slug, hash) pair.
// slug is a human-readable repo name; hash is the content-keyed
// identifier source.Cache computes from (url, ref, authID).
func (l Layout) SourceSlot(slug, hash string) string {
	return join(l.Root, SourcesDir, slug, hash)
}

// Baselines returns the parent directory of every materialized
// baseline tree.
func (l Layout) Baselines() string { return join(l.Root, BaselinesDir) }

// Baseline returns the on-disk path for a baseline tree keyed by its
// commit sha.
func (l Layout) Baseline(commitSHA string) string {
	return join(l.Root, BaselinesDir, commitSHA)
}

// Blobs returns the parent of every content-addressed blob.
// Always includes the algorithm segment so blobs/sha512/ etc. can land
// here later without rewriting the GC's walk.
func (l Layout) Blobs() string { return join(l.Root, BlobsDir, BlobsAlgo) }

// Blob returns the on-disk directory for a single blob keyed by its
// hex sha256 digest.
func (l Layout) Blob(digest string) string {
	return join(l.Root, BlobsDir, BlobsAlgo, digest)
}

// RefsRoot returns the parent of every refs table. Walked by GC to
// clean dangling pointers.
func (l Layout) RefsRoot() string { return join(l.Root, RefsDir) }

// RefsCategory carves out a subdirectory under refs/ for one
// caller's identity→digest mapping. The first arg is a stable name
// (e.g. "chart-tarballs", "git-revisions") shared between the writer
// and any introspection tooling.
func (l Layout) RefsCategory(name string) string {
	return join(l.Root, RefsDir, name)
}

// GitMirrors returns the parent of every bare git mirror.
func (l Layout) GitMirrors() string { return join(l.Root, GitMirrorsDir) }

// GitMirror returns the on-disk path for a bare mirror keyed by the
// stable hash of an upstream URL.
func (l Layout) GitMirror(urlHash string) string {
	return join(l.Root, GitMirrorsDir, urlHash)
}

// HelmTmp returns the scratch directory the HelmChart fetcher uses for
// transient writes (index.yaml downloads, TLS cert materialization).
func (l Layout) HelmTmp() string { return join(l.Root, HelmTmpDir) }

// RenderHelmCache returns the parent directory of the persisted helm
// template-output cache. Entries live under sharded subdirs keyed by
// the first two hex chars of the cache key; the disk layer in
// pkg/helm owns the layout below this root.
func (l Layout) RenderHelmCache() string { return join(l.Root, RenderHelmCacheDir) }

// RenderKustomizeCache is the root of the persisted kustomize render cache; the
// disk layer in pkg/kustomize owns the sharded layout below it.
func (l Layout) RenderKustomizeCache() string { return join(l.Root, RenderKustomizeCacheDir) }
