package source

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// Cache manages a content-addressed on-disk directory for fetched
// sources. Each (url, ref) tuple gets its own slot, so multiple revisions
// of the same upstream coexist without clobbering one another.
//
// The cache is safe for concurrent use. Per-slot locks serialize the
// full fetch-write-read lifecycle on a single slot. Different slots
// proceed in parallel.
type Cache struct {
	layout cacheroot.Layout
	// locks is the canonical per-slot mutex provider. Sharing the
	// keylock.KeyMap implementation with helm + blob keeps cancellation
	// semantics and lifetime management consistent across the cache.
	locks *keylock.KeyMap[string]
}

// NewCache constructs a Cache backed by the supplied Layout. The
// caller-owned Layout is the single source of truth for cache paths;
// this Cache asks it for SourceSlot positions and never composes its
// own. When the Layout's Root is empty, a flate-cache subdirectory
// under os.TempDir() is used so embedders that don't wire CacheDir
// still work.
func NewCache(layout cacheroot.Layout) *Cache {
	if layout.Root == "" {
		layout.Root = filepath.Join(os.TempDir(), "flate-cache")
	}
	return &Cache{layout: layout, locks: keylock.New[string]()}
}

// Slot allocates a per-(url, ref) slot under the cache root with
// atomic-rename finalization. Holds the slot mutex until Release is
// called; serializes against other Slot acquisitions on the same key.
//
// The returned Slot's Path is:
//
//   - The final slot directory when Exists is true (cache hit — the
//     fetcher reads from it directly).
//   - A sibling staging directory when Exists is false (cache miss —
//     the fetcher writes into it, then calls Commit to atomic-rename
//     into the final slot).
//
// The staging dir lives at `<final>.tmp.<rand>`, on the same filesystem
// as the final, so os.Rename is atomic per POSIX. If the fetcher
// returns without committing (any error path, any panic), Release
// removes the staging dir and the final slot is left absent — the
// next fetch starts clean. This atomicity replaces the older
// "write in place + .flate-* sentinels" pattern: the final slot is
// either complete or doesn't exist, never torn.
func (c *Cache) Slot(ctx context.Context, url, ref, authID string) (*Slot, error) {
	slug := slugifyRepo(url)
	// authID participates in the cache key so two source CRs with the
	// same (URL, ref) but different SecretRef / RegistryConfig don't
	// share a slot — otherwise the first fetch's clone is silently
	// reused for the second auth context, bypassing the access check.
	// Pass "" when the fetcher has no auth bound to this slot.
	h := sha256.Sum256([]byte(url + "@" + ref + "#" + authID))
	hash := hex.EncodeToString(h[:])[:16]
	final := c.layout.SourceSlot(slug, hash)

	release, err := c.locks.Acquire(ctx, final)
	if err != nil {
		return nil, err
	}
	s := &Slot{final: final, release: release}
	if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
		s.unlock()
		return nil, fmt.Errorf("cache slot parent: %w", err)
	}
	if err := s.lockFile(ctx); err != nil {
		s.unlock()
		return nil, err
	}

	info, statErr := os.Stat(final)
	switch {
	case statErr == nil && info.IsDir():
		// Non-empty directory counts as populated. We use the presence
		// of any entry as the indicator so a bare `mkdir` from a prior
		// aborted run doesn't masquerade as a hit.
		//
		// Propagate Open / Readdirnames errors instead of silently
		// treating them as "empty directory". A populated slot that
		// the runner can't read (EACCES, EMFILE, a half-broken FUSE
		// mount) used to fall through into the empty-dir RemoveAll
		// branch — which would also fail — and the user saw a
		// misleading "cache slot clean empty" instead of the real
		// underlying error.
		f, ferr := os.Open(final) //nolint:gosec // final is under the cache root
		if ferr != nil {
			s.unlock()
			return nil, fmt.Errorf("cache slot open: %w", ferr)
		}
		entries, readErr := f.Readdirnames(1)
		_ = f.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			s.unlock()
			return nil, fmt.Errorf("cache slot read: %w", readErr)
		}
		s.Exists = len(entries) > 0
		if s.Exists {
			s.Path = final
			return s, nil
		}
		// Empty directory left from a prior aborted run — remove
		// so the staging dir can take over cleanly.
		if err := os.RemoveAll(final); err != nil {
			s.unlock()
			return nil, fmt.Errorf("cache slot clean empty: %w", err)
		}
		fallthrough
	case os.IsNotExist(statErr):
		// Allocate a sibling staging dir on the same filesystem so
		// the eventual rename is atomic.
		staging, err := os.MkdirTemp(filepath.Dir(final), filepath.Base(final)+".tmp.*")
		if err != nil {
			s.unlock()
			return nil, fmt.Errorf("cache slot staging: %w", err)
		}
		s.Path = staging
		s.staging = staging
		return s, nil
	default:
		s.unlock()
		return nil, fmt.Errorf("cache slot stat: %w", statErr)
	}
}

// Slot is one allocated cache slot. Acquired by Cache.Slot, released
// by the caller's deferred Release. On a cache miss the fetcher writes
// into Path (a staging dir) and calls Commit to atomic-rename into the
// final slot; on a cache hit Path is already the final slot.
type Slot struct {
	// Path is where the fetcher reads / writes:
	//   Exists == true  → Path is the final slot (read-only use).
	//   Exists == false → Path is the staging dir (write here, then
	//                     Commit to finalize).
	Path string
	// Exists reports whether the final slot was already populated by
	// a prior fetch. When true, the staging dance is skipped and
	// Path is the final slot directly.
	Exists bool

	final     string
	staging   string
	release   func() // keylock release; populated by Cache.Slot
	fileLock  *flock.Flock
	committed bool
	unlocked  bool
}

// Commit finalizes a successful fetch: atomic-rename the staging dir
// over the final slot. No-op on a cache hit (Exists == true). Safe to
// call multiple times. After a successful commit, Path is updated to
// the final slot so subsequent reads work uniformly.
func (s *Slot) Commit() error {
	if s.Exists && s.staging != "" {
		return s.commitRefresh()
	}
	if s.Exists || s.committed {
		return nil
	}
	if err := os.Rename(s.staging, s.final); err != nil {
		if existingSlotPopulated(s.final) {
			// Another process using the same cache root may have
			// finalized this slot while we were staging. keylock only
			// coordinates goroutines in this process, so adopt the
			// existing non-empty slot instead of deleting it.
			_ = os.RemoveAll(s.staging)
			s.committed = true
			s.staging = ""
			s.Path = s.final
			s.Exists = true
			return nil
		}
		return fmt.Errorf("cache commit: %w", err)
	}
	s.committed = true
	s.staging = ""
	s.Path = s.final
	return nil
}

func (s *Slot) commitRefresh() error {
	backup := filepath.Join(filepath.Dir(s.final), filepath.Base(s.final)+".old."+randomHex(8))
	if err := os.Rename(s.final, backup); err != nil {
		_ = os.RemoveAll(backup)
		return fmt.Errorf("cache refresh move old: %w", err)
	}
	if err := os.Rename(s.staging, s.final); err != nil {
		_ = os.Rename(backup, s.final)
		return fmt.Errorf("cache refresh commit: %w", err)
	}
	_ = os.RemoveAll(backup)
	s.committed = true
	s.staging = ""
	s.Path = s.final
	return nil
}

// randomHex returns n random bytes encoded as a lowercase hex string (2n chars).
// Panics if crypto/rand is unavailable — which cannot happen on supported platforms.
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}

func existingSlotPopulated(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return false
	}
	f, err := os.Open(path) //nolint:gosec // path is the cache slot final path
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	entries, err := f.Readdirnames(1)
	return err == nil && len(entries) > 0
}

// Release drops the slot mutex AND, if Commit wasn't called, removes
// the orphan staging dir. MUST be deferred by every Cache.Slot caller.
// Safe to call multiple times — second+ calls are no-ops.
func (s *Slot) Release() {
	if s.unlocked {
		return
	}
	if !s.committed && s.staging != "" {
		_ = os.RemoveAll(s.staging)
	}
	s.unlock()
}

// Reset wipes the final slot — used by callers that detected a stale
// cache hit (e.g. cosign signature changed against the cached digest).
// After Reset the slot looks like an Exists=false miss; the caller
// can write to a new staging via a fresh Cache.Slot call, OR can call
// Stage on this same Slot to allocate staging in place.
func (s *Slot) Reset() error {
	if err := os.RemoveAll(s.final); err != nil {
		return err
	}
	s.Exists = false
	s.Path = ""
	return nil
}

// Refresh wipes the final slot and allocates a fresh staging directory
// in one step. It is equivalent to calling Reset followed by Stage and
// is intended for callers that detect a stale immutable cache hit and
// need to re-fetch from scratch. The Reset+Stage sequence is preserved
// unchanged; this method exists only to avoid repeating the pair.
func (s *Slot) Refresh() error {
	if err := s.Reset(); err != nil {
		return err
	}
	return s.Stage()
}

// Stage allocates the staging dir for a Slot that was returned with
// Exists == true and then explicitly Reset by the caller. The new Path
// is the staging dir. No-op when staging is already allocated.
func (s *Slot) Stage() error {
	if s.staging != "" {
		s.Path = s.staging
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.final), 0o750); err != nil {
		return fmt.Errorf("cache slot stage parent: %w", err)
	}
	return s.allocStaging("stage")
}

// allocStaging creates the .tmp.* staging dir under the final slot's
// parent and points Path at it. label distinguishes the wrapped error
// between the Stage and refresh-stage callers. Assumes s.staging is
// empty — callers guard that and the parent-dir creation themselves.
func (s *Slot) allocStaging(label string) error {
	staging, err := os.MkdirTemp(filepath.Dir(s.final), filepath.Base(s.final)+".tmp.*")
	if err != nil {
		return fmt.Errorf("cache slot %s: %w", label, err)
	}
	s.staging = staging
	s.Path = staging
	return nil
}

// StageRefresh allocates a staging directory while preserving the
// current final slot until Commit. It is for interval-based refreshes:
// readers keep a usable old artifact if the fetch fails before commit,
// while a successful Commit replaces the final slot under the held
// slot lock.
func (s *Slot) StageRefresh() error {
	if !s.Exists {
		return s.Stage()
	}
	if s.staging != "" {
		s.Path = s.staging
		return nil
	}
	return s.allocStaging("refresh stage")
}

func (s *Slot) lockFile(ctx context.Context) error {
	s.fileLock = flock.New(s.final+".lock", flock.SetPermissions(0o600))
	locked, err := s.fileLock.TryLockContext(ctx, 100*time.Millisecond)
	if err != nil {
		_ = s.fileLock.Close()
		s.fileLock = nil
		return fmt.Errorf("cache slot lock: %w", err)
	}
	if !locked {
		_ = s.fileLock.Close()
		s.fileLock = nil
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cache slot lock: %w", err)
		}
		return errors.New("cache slot lock: not acquired")
	}
	return nil
}

func (s *Slot) unlock() {
	if s.unlocked {
		return
	}
	s.unlocked = true
	if s.fileLock != nil {
		_ = s.fileLock.Unlock()
		_ = s.fileLock.Close()
		s.fileLock = nil
	}
	s.release()
}

// nonAlnum collapses non-alphanumeric (plus `.-_`) runs into a single
// dash so the resulting slug is fs-safe.
var nonAlnum = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// maxSlugLen caps slug length so cache paths stay below typical
// filesystem name limits and remain greppable.
const maxSlugLen = 50

// slugifyRepo reduces a URL to a short, filesystem-safe identifier
// matching the LAST PATH SEGMENT of the repo URL — not the version.
// Strip OCI version suffixes (`:tag`, `@digest`) and the `.git`
// suffix BEFORE picking the last segment, so:
//
//	oci://ghcr.io/bjw-s-labs/helm/app-template:5.0.1 → "app-template"
//	oci://ghcr.io/foo/bar@sha256:abc...               → "bar"
//	https://github.com/owner/repo.git                 → "repo"
//	git@github.com:owner/repo.git                     → "repo"
//
// Without the version strip, the OCI flow always presents
// versionedURL (URL + ":" + tag) to slugifyRepo, and every OCI
// slot ends up under `&lt;root&gt;/sources/&lt;tag&gt;/&lt;hash&gt;` — e.g. every
// app-template release piles under `5.0.1/`, `5.0.2/`, `5.0.3/`,
// indistinguishable from any other chart pinned to those tags.
// Functionally safe (hash is the real uniqueness key) but
// useless for operators inspecting the cache. The hash component
// of the slot path is unchanged by this function; it remains
// computed from the raw URL + ref upstream.
func slugifyRepo(url string) string {
	// Strip version suffixes added by versionedURL (or hand-typed
	// URLs that include `:tag` / `@digest`). Done in two steps to
	// preserve scheme colons (e.g. `https:`, `oci:`): split on the
	// LAST `@` first (digest), then look for a trailing `:` that
	// follows the last `/` (tag, not port — registry host:port has
	// the colon BEFORE the path's last `/`).
	if i := strings.LastIndex(url, "@"); i >= 0 {
		// Only treat `@` as digest if what follows looks like
		// `<algo>:<hex>` — otherwise it's userinfo
		// (`git@github.com`) and must be preserved for the
		// last-segment split below.
		if rest := url[i+1:]; strings.Contains(rest, ":") && !strings.Contains(rest, "/") {
			url = url[:i]
		}
	}
	if lastSlash := strings.LastIndex(url, "/"); lastSlash >= 0 {
		if colon := strings.IndexByte(url[lastSlash+1:], ':'); colon >= 0 {
			url = url[:lastSlash+1+colon]
		}
	}
	url = strings.TrimSuffix(url, ".git")
	if idx := strings.LastIndexAny(url, "/:"); idx >= 0 && idx < len(url)-1 {
		url = url[idx+1:]
	}
	url = nonAlnum.ReplaceAllString(url, "-")
	url = strings.Trim(url, "-_.")
	if len(url) > maxSlugLen {
		url = url[:maxSlugLen]
	}
	if url == "" {
		return "repo"
	}
	return url
}
