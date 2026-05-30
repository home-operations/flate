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
	HelmCacheDir  = "helm-cache"
	StageDir      = "stage"
	// RenderHelmCacheDir holds the persisted helm template-output
	// cache. Entries are sharded `<root>/render/helm/<hex[:2]>/<hex>`
	// where <hex> is the full sha256 of the template-cache key. Content
	// is gzipped rendered manifest bytes. Cross-process safe via atomic
	// rename; eviction is mtime-LRU bounded by the caller's byte cap.
	RenderHelmCacheDir = "render/helm"
)

// pathSep is the platform separator as a string, used by the join
// helpers so each can stay allocation-free for the separator itself.
var pathSep = string(filepath.Separator)

// app2 appends one path segment to a clean base without a filepath.Clean
// pass. New guarantees Root is clean; all callers pass string constants
// as the second argument.
func app2(base, seg string) string { return base + pathSep + seg }

// app3 appends two path segments to a clean base; see app2.
func app3(base, seg1, seg2 string) string {
	var b strings.Builder
	b.Grow(len(base) + 1 + len(seg1) + 1 + len(seg2))
	b.WriteString(base)
	b.WriteString(pathSep)
	b.WriteString(seg1)
	b.WriteString(pathSep)
	b.WriteString(seg2)
	return b.String()
}

// app4 appends three path segments to a clean base; see app2.
func app4(base, seg1, seg2, seg3 string) string {
	var b strings.Builder
	b.Grow(len(base) + 1 + len(seg1) + 1 + len(seg2) + 1 + len(seg3))
	b.WriteString(base)
	b.WriteString(pathSep)
	b.WriteString(seg1)
	b.WriteString(pathSep)
	b.WriteString(seg2)
	b.WriteString(pathSep)
	b.WriteString(seg3)
	return b.String()
}

// Sources returns the parent directory of every source slot. Used by
// GC's age sweep and listing tools.
func (l Layout) Sources() string { return app2(l.Root, SourcesDir) }

// SourceSlot returns the on-disk slot for a given (slug, hash) pair.
// slug is a human-readable repo name; hash is the content-keyed
// identifier source.Cache computes from (url, ref, authID).
func (l Layout) SourceSlot(slug, hash string) string {
	return app4(l.Root, SourcesDir, slug, hash)
}

// Baselines returns the parent directory of every materialized
// baseline tree.
func (l Layout) Baselines() string { return app2(l.Root, BaselinesDir) }

// Baseline returns the on-disk path for a baseline tree keyed by its
// commit sha.
func (l Layout) Baseline(commitSHA string) string {
	return app3(l.Root, BaselinesDir, commitSHA)
}

// Blobs returns the parent of every content-addressed blob.
// Always includes the algorithm segment so blobs/sha512/ etc. can land
// here later without rewriting the GC's walk.
func (l Layout) Blobs() string { return app3(l.Root, BlobsDir, BlobsAlgo) }

// Blob returns the on-disk directory for a single blob keyed by its
// hex sha256 digest.
func (l Layout) Blob(digest string) string {
	return app4(l.Root, BlobsDir, BlobsAlgo, digest)
}

// RefsRoot returns the parent of every refs table. Walked by GC to
// clean dangling pointers.
func (l Layout) RefsRoot() string { return app2(l.Root, RefsDir) }

// RefsCategory carves out a subdirectory under refs/ for one
// caller's identity→digest mapping. The first arg is a stable name
// (e.g. "chart-tarballs", "git-revisions") shared between the writer
// and any introspection tooling.
func (l Layout) RefsCategory(name string) string {
	return app3(l.Root, RefsDir, name)
}

// GitMirrors returns the parent of every bare git mirror.
func (l Layout) GitMirrors() string { return app2(l.Root, GitMirrorsDir) }

// GitMirror returns the on-disk path for a bare mirror keyed by the
// stable hash of an upstream URL.
func (l Layout) GitMirror(urlHash string) string {
	return app3(l.Root, GitMirrorsDir, urlHash)
}

// HelmTmp returns the scratch directory the helm client uses for
// transient writes (index.yaml downloads, TLS cert materialization).
func (l Layout) HelmTmp() string { return app2(l.Root, HelmTmpDir) }

// HelmCache returns the helm client's cacheDir — the on-disk root the
// chart tarball CAS and chart-tarball refs table sit under.
func (l Layout) HelmCache() string { return app2(l.Root, HelmCacheDir) }

// Stage returns the kustomize staging root. Two consumers share it:
//   - Per-process scratch stages land as flate-stage-<rand> children (legacy
//     fallback for sources without a content-addressable fingerprint).
//   - Persistent content-addressed stages land at <stage>/<digest[:2]>/<digest>/
//     keyed by the source artifact's fingerprint (git SHA, OCI digest…).
//
// The two-char fan-out prefix keeps any one subdir below typical FS dirent
// limits and never collides with the `flate-stage-` legacy prefix.
func (l Layout) Stage() string { return app2(l.Root, StageDir) }

// RenderHelmCache returns the parent directory of the persisted helm
// template-output cache. Entries live under sharded subdirs keyed by
// the first two hex chars of the cache key; the disk layer in
// pkg/helm owns the layout below this root.
func (l Layout) RenderHelmCache() string { return app2(l.Root, RenderHelmCacheDir) }

// StageEntry returns the on-disk persistent stage directory for the
// given source fingerprint. The fingerprint must be a non-empty
// printable token (commit SHA, OCI digest, content hash); callers
// fall back to per-process staging when no fingerprint is available.
func (l Layout) StageEntry(fingerprint string) string {
	prefix := fingerprint
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}
	return app4(l.Root, StageDir, prefix, fingerprint)
}
