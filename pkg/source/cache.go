package source

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Cache manages a content-addressed on-disk directory for fetched
// sources. Each (url, ref) tuple gets its own slot, so multiple revisions
// of the same upstream coexist without clobbering one another.
//
// The cache is safe for concurrent use. A per-slot mutex serializes the
// full fetch-write-read lifecycle on a single slot — two distinct
// source CRs with the same (url, ref) hash to the same slot, and
// without per-slot locking one would observe the other mid-write
// (e.g. read an empty marker, call Reset, wipe the in-progress clone).
// Different slots proceed in parallel.
type Cache struct {
	root string
	mu   sync.Mutex // guards locks
	// locks holds a sync.Mutex per slot path. Lazily created; never
	// reaped — the slot count is bounded by user-declared sources.
	locks map[string]*sync.Mutex
}

// NewCache constructs a Cache rooted at dir. If dir is empty, a
// flate-cache subdirectory under os.TempDir() is used.
func NewCache(dir string) *Cache {
	return &Cache{root: cmp.Or(dir, filepath.Join(os.TempDir(), "flate-cache"))}
}

// slotMu returns the per-slot mutex for path, creating it on first
// access. Caller must NOT hold c.mu.
func (c *Cache) slotMu(path string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.locks == nil {
		c.locks = make(map[string]*sync.Mutex)
	}
	m, ok := c.locks[path]
	if !ok {
		m = &sync.Mutex{}
		c.locks[path] = m
	}
	return m
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
func (c *Cache) Slot(url, ref, authID string) (*Slot, error) {
	slug := slugifyRepo(url)
	// authID participates in the cache key so two source CRs with the
	// same (URL, ref) but different SecretRef / RegistryConfig don't
	// share a slot — otherwise the first fetch's clone is silently
	// reused for the second auth context, bypassing the access check.
	// Pass "" when the fetcher has no auth bound to this slot.
	h := sha256.Sum256([]byte(url + "@" + ref + "#" + authID))
	hash := hex.EncodeToString(h[:])[:16]
	final := filepath.Join(c.root, "sources", slug, hash)

	m := c.slotMu(final)
	m.Lock()
	s := &Slot{final: final, mu: m}

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
		if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
			s.unlock()
			return nil, fmt.Errorf("cache slot parent: %w", err)
		}
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
	mu        *sync.Mutex
	committed bool
	unlocked  bool
}

// Commit finalizes a successful fetch: atomic-rename the staging dir
// over the final slot. No-op on a cache hit (Exists == true). Safe to
// call multiple times. After a successful commit, Path is updated to
// the final slot so subsequent reads work uniformly.
func (s *Slot) Commit() error {
	if s.Exists || s.committed {
		return nil
	}
	// os.Rename across an existing target is platform-dependent —
	// remove the (definitely empty by our protocol; we checked it on
	// Slot construction) target first so the rename is unambiguous.
	if err := os.RemoveAll(s.final); err != nil {
		return fmt.Errorf("cache commit prep: %w", err)
	}
	if err := os.Rename(s.staging, s.final); err != nil {
		return fmt.Errorf("cache commit: %w", err)
	}
	s.committed = true
	s.staging = ""
	s.Path = s.final
	return nil
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
	staging, err := os.MkdirTemp(filepath.Dir(s.final), filepath.Base(s.final)+".tmp.*")
	if err != nil {
		return fmt.Errorf("cache slot stage: %w", err)
	}
	s.staging = staging
	s.Path = staging
	return nil
}

func (s *Slot) unlock() {
	if s.unlocked {
		return
	}
	s.unlocked = true
	s.mu.Unlock()
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
	return cmp.Or(url, "repo")
}
