// Package git implements the source.Fetcher for KindGitRepository.
//
// File map:
//
//	fetcher.go    — Fetcher type, Fetch entry, fetch + fetchViaMirror, authIdentity
//	remotebase.go — FetchRemoteBase: anonymous kustomize remote git base
//	auth.go       — SecretRef → transport.AuthMethod resolution
//	tls.go        — spec.secretRef.ca.crt → *tls.Config
//	ssh.go        — SSH host-key callbacks (known_hosts / insecure)
//	checkout.go   — checkoutRef + updateSubmodules
//	resolve.go    — ref → commit hash (mirror path)
//	marker.go     — slot revision sidecar (.flate-meta.json) + worktree HEAD lookup
package git

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/git/internal/gittransport"
	"github.com/home-operations/flate/pkg/source/git/mirror"
	"github.com/home-operations/flate/pkg/source/git/verify"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the source.Fetcher implementation for KindGitRepository.
// It owns a shared Cache so multiple GitRepository CRs writing to the
// same cache root serialize on slot allocation correctly. Secrets is
// optional; required when a GitRepository sets spec.secretRef.
//
// Mirrors, when set, switches the default fetch path to an incremental
// bare-mirror-plus-worktree strategy: one bare clone per upstream URL
// (kept warm across runs and across refs), and per-slot worktrees are
// materialized by walking the commit tree out of the mirror. The
// legacy full PlainClone-into-slot path still runs for repos that
// need submodule recursion or sparse checkout — neither feature is
// expressible against a bare mirror without a separate fetch that
// defeats the cache. Leave nil to keep the legacy path everywhere.
type Fetcher struct {
	Cache   *source.Cache
	Secrets source.SecretGetter
	Mirrors *mirror.Cache

	// Depth caps the clone/fetch history depth for both the bare mirror
	// and the legacy clone path. 0 (the zero value) clones full history,
	// so library embedders are unaffected; the CLI defaults it to 1
	// (opt-out via --git-depth=0). Shallow is forced off for commit-pinned
	// refs (see effectiveDepth) and, in the legacy path, for submodule
	// recursion. The worktree materialization only needs the resolved
	// tip's tree, which a shallow clone provides in full.
	Depth int
}

// Fetch implements source.TypedFetcher[*manifest.GitRepository].
// The typed signature is wrapped via source.Wrap at orchestrator
// registration — a payload mismatch returns ErrInput once at the
// adapter site rather than panicking here.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.GitRepository) (*store.SourceArtifact, error) {
	if repo.Provider != "" && repo.Provider != sourcev1.GitProviderGeneric {
		return nil, source.ErrUnsupportedProvider("GitRepository",
			repo.Namespace, repo.Name, repo.Provider, sourcev1.GitProviderGeneric,
			"SecretRef-based credentials")
	}
	auth, proxy, restore, err := f.resolveTransport(repo)
	if err != nil {
		return nil, err
	}
	defer restore()
	return f.fetch(ctx, repo, auth, proxy)
}

// resolveTransport resolves the auth, TLS, and proxy material for repo
// and installs the process-wide HTTPS transport for the fetch. It
// returns the auth method and proxy config the go-git call needs plus a
// restore closure the caller MUST defer to tear the transport back down.
// Shared by Fetch and FetchRemoteBase so both agree on credential resolution.
func (f *Fetcher) resolveTransport(repo *manifest.GitRepository) (transport.AuthMethod, *source.ProxyConfig, func(), error) {
	auth, err := f.resolveAuth(repo)
	if err != nil {
		return nil, nil, nil, err
	}
	tlsCfg, err := f.resolveTLS(repo)
	if err != nil {
		return nil, nil, nil, err
	}
	proxy, err := source.ResolveProxy(f.Secrets, repo.Namespace, "GitRepository",
		repo.Namespace+"/"+repo.Name, repo.ProxySecretRef)
	if err != nil {
		return nil, nil, nil, err
	}
	restore, err := gittransport.InstallHTTPS(tlsCfg, proxy)
	if err != nil {
		return nil, nil, nil, err
	}
	return auth, proxy, restore, nil
}

// fetch clones the GitRepository, then runs verification, ignore, and
// the cache-marker write — ALL inside the per-slot critical section.
// Holding the slot lock across post-clone work prevents another
// fetcher with the same (url, ref) from observing torn state (an
// in-progress clone, a missing marker, an unverified slot) or from
// racing a sibling cache.Reset against an in-flight write.
//
// auth may be nil for anonymous clones; proxy may be nil for direct.
// Supported transports: HTTPS (anonymous, basic, bearer), SSH (key
// from SecretRef or ssh-agent), and file:// URLs.
func (f *Fetcher) fetch(ctx context.Context, repo *manifest.GitRepository, auth transport.AuthMethod, proxy *source.ProxyConfig) (*store.SourceArtifact, error) {
	if err := validateRepo(repo); err != nil {
		return nil, err
	}

	refLabel := "HEAD"
	if repo.Reference != nil {
		refLabel = cmp.Or(gitRefLabel(*repo.Reference), refLabel)
	}
	slotRef := gitCacheKey(repo, refLabel)
	mutableRef := !canUseCachedGitSlot(repo.Reference)

	authID := authIdentity(repo)
	slot, err := f.Cache.Slot(ctx, repo.URL, slotRef, authID)
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", repo.URL, err)
	}
	defer slot.Release()

	if slot.Exists {
		// The flate-revision marker is written AFTER a successful
		// clone+checkout (and committed via atomic rename only after
		// the marker write), so any committed slot will have it. A
		// missing marker means a legacy slot from a pre-marker flate
		// version or a hand-modified cache.
		if rev := readCachedRevision(slot.Path); rev != "" {
			if !mutableRef {
				return gitArtifact(repo.URL, slot.Path, rev), nil
			}
			if rev, ok := cachedRevisionFresh(slot.Path, repo.Interval.Duration); ok {
				return gitArtifact(repo.URL, slot.Path, rev), nil
			}
		}
		if mutableRef {
			if err := slot.StageRefresh(); err != nil {
				return nil, err
			}
		} else {
			// Stale immutable slot — wipe and stage a fresh clone target.
			if err := slot.Refresh(); err != nil {
				return nil, err
			}
		}
	}

	if f.canUseMirror(repo) {
		return f.fetchViaMirror(ctx, repo, refLabel, slot, auth, proxy)
	}

	url := repo.URL
	// file:// URLs are accepted by go-git as bare filesystem paths.
	if rest, ok := strings.CutPrefix(url, "file://"); ok {
		url = rest
	}

	cloneOpts := &git.CloneOptions{URL: url, NoCheckout: true, Auth: auth}
	if proxy != nil {
		cloneOpts.ProxyOptions = proxyOptions(proxy)
	}
	if repo.RecurseSubmodules {
		cloneOpts.RecurseSubmodules = git.DefaultSubmoduleRecursionDepth
	} else {
		// Shallow + submodule recursion is finicky in go-git, so only
		// carry Depth on the non-submodule legacy path. Plain sparse
		// checkout is fine with shallow; commit-pinned refs fall back to
		// a full clone inside effectiveDepth.
		cloneOpts.Depth = effectiveDepth(f.Depth, repo.Reference)
	}
	// go-git's PlainCloneContext refuses to clone into a non-empty
	// directory — but our staging dir IS that empty directory, so
	// pass it directly. On any error, Release wipes staging; the
	// final slot is never touched.
	cloned, err := git.PlainCloneContext(ctx, slot.Path, false, cloneOpts)
	if err != nil {
		return nil, fmt.Errorf("clone %s: %w", url, err)
	}

	var ref manifest.GitRepositoryRef
	if repo.Reference != nil {
		ref = *repo.Reference
	}
	if err := fetchExplicitNamedRef(ctx, cloned, auth, proxy, effectiveNamedRef(ref)); err != nil {
		return nil, err
	}
	if err := checkoutRef(cloned, ref, repo.SparseCheckout); err != nil {
		return nil, fmt.Errorf("checkout %s: %w", refLabel, err)
	}
	if repo.RecurseSubmodules {
		if err := updateSubmodules(cloned, auth); err != nil {
			return nil, fmt.Errorf("submodules: %w", err)
		}
	}

	art := gitArtifact(repo.URL, slot.Path, readResolvedRevision(slot.Path))
	if err := f.finalize(repo, art); err != nil {
		return nil, err
	}
	return commitArtifact(repo, slot, art)
}

// validateRepo guards the preconditions Fetch requires before touching
// the network: a non-nil CR with a URL.
func validateRepo(repo *manifest.GitRepository) error {
	if repo == nil {
		return errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return fmt.Errorf("%w: GitRepository %s missing url", manifest.ErrInput, repo.RepoName())
	}
	return nil
}

func effectiveNamedRef(ref manifest.GitRepositoryRef) string {
	if ref.Commit != "" {
		return ""
	}
	return ref.Name
}

func gitRefLabel(ref manifest.GitRepositoryRef) string {
	if ref.Commit != "" && ref.Branch != "" {
		return "branch:" + ref.Branch + "@commit:" + ref.Commit
	}
	return manifest.GitRefString(ref)
}

func gitCacheKey(repo *manifest.GitRepository, refLabel string) string {
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	payload := struct {
		Ref               string   `json:"ref"`
		Ignore            string   `json:"ignore,omitempty"`
		SparseCheckout    []string `json:"sparseCheckout,omitempty"`
		RecurseSubmodules bool     `json:"recurseSubmodules,omitempty"`
		Verify            string   `json:"verify,omitempty"`
	}{
		Ref:               refLabel,
		Ignore:            ignore,
		SparseCheckout:    slices.Clone(repo.SparseCheckout),
		RecurseSubmodules: repo.RecurseSubmodules,
		Verify:            gitVerifyCacheKey(repo.Namespace, repo.Verification),
	}
	h, _ := source.CacheKeyHash(payload, 8)
	return refLabel + "#opts:" + h
}

func gitVerifyCacheKey(namespace string, v *manifest.GitRepositoryVerify) string {
	if v == nil {
		return ""
	}
	return string(v.GetMode()) + ":" + namespace + "/" + v.SecretRef.Name
}

func canUseCachedGitSlot(ref *manifest.GitRepositoryRef) bool {
	return ref != nil &&
		ref.Commit != "" &&
		ref.Branch == "" &&
		ref.Tag == "" &&
		ref.SemVer == "" &&
		ref.Name == ""
}

func fetchExplicitNamedRef(ctx context.Context, repo *git.Repository, auth transport.AuthMethod, proxy *source.ProxyConfig, name string) error {
	if name == "" {
		return nil
	}
	remote, err := repo.Remote("origin")
	if err != nil {
		return err
	}
	opts := &git.FetchOptions{
		Auth:     auth,
		RefSpecs: []config.RefSpec{config.RefSpec("+" + name + ":" + name)},
	}
	if proxy != nil {
		opts.ProxyOptions = proxyOptions(proxy)
	}
	if err := remote.FetchContext(ctx, opts); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("fetch ref.name %s: %w", name, err)
	}
	return nil
}

// finalize runs PGP verification (when configured), applies the
// source-controller-compatible ignore patterns, and writes the cache-
// hit marker. Returns the first error encountered; the caller is
// expected to Reset the slot on error while still holding the lock.
func (f *Fetcher) finalize(repo *manifest.GitRepository, art *store.SourceArtifact) error {
	if repo.Verification != nil {
		cloned, oerr := git.PlainOpen(art.LocalPath)
		if oerr != nil {
			return fmt.Errorf("verify: reopen %s: %w", art.LocalPath, oerr)
		}
		head, herr := cloned.Head()
		if herr != nil {
			return fmt.Errorf("verify: resolve HEAD: %w", herr)
		}
		if err := f.verifySignatures(repo, cloned, head.Hash()); err != nil {
			return err
		}
	}
	return applyIgnoreAndMark(repo, art)
}

// applyIgnoreAndMark applies the source-controller-compatible ignore
// patterns to the materialized slot, then writes the revision marker.
// Separated from finalize so the mirror path can call it without
// re-opening the local worktree for verification (the mirror path
// verifies against the bare-repo object store instead).
//
// The marker is written AFTER ApplyIgnore so it survives user-supplied
// "exclude all" patterns (common: `/*` + reincludes). Next run's
// cache-hit check avoids the expensive re-clone for big repos whose
// .git/ was wiped by the ignore step.
func applyIgnoreAndMark(repo *manifest.GitRepository, art *store.SourceArtifact) error {
	if err := source.ApplyIgnore(art.LocalPath, repo.Ignore); err != nil {
		return fmt.Errorf("GitRepository %s/%s: %w", repo.Namespace, repo.Name, err)
	}
	if art.Revision != "" {
		_ = writeCachedRevision(art.LocalPath, art.Revision)
	}
	return nil
}

// verifySignatures runs PGP verification of the object at hash within
// gitRepo (a local worktree or a bare mirror — both carry the object
// store verify.Signatures walks). A no-op when repo.Verification is
// unset, so callers can invoke it unconditionally.
func (f *Fetcher) verifySignatures(repo *manifest.GitRepository, gitRepo *git.Repository, hash plumbing.Hash) error {
	if repo.Verification == nil {
		return nil
	}
	tagName := ""
	if repo.Reference != nil {
		tagName = repo.Reference.Tag
	}
	return verify.Signatures(f.Secrets, repo.Namespace, repo.Name,
		repo.Verification.SecretRef.Name, repo.Verification.GetMode(), tagName, gitRepo, hash)
}

// canUseMirror reports whether this Fetcher can take the bare-mirror
// path for repo. The mirror path doesn't support submodule recursion
// or sparse checkout — both require a real worktree go-git can act on.
// Everything else (https, ssh, file://) is mirror-eligible.
func (f *Fetcher) canUseMirror(repo *manifest.GitRepository) bool {
	if f.Mirrors == nil {
		return false
	}
	if repo.RecurseSubmodules {
		return false
	}
	if len(repo.SparseCheckout) > 0 {
		return false
	}
	return true
}

// fetchViaMirror runs the bare-mirror path: open-or-update the
// per-URL mirror, resolve the requested ref to a commit hash, then
// materialize the tree into the slot's staging dir. PGP verification
// runs against the mirror (which has the object store); ApplyIgnore
// and the revision-marker write delegate to applyIgnoreAndMark.
func (f *Fetcher) fetchViaMirror(ctx context.Context, repo *manifest.GitRepository, refStr string, slot *source.Slot, auth transport.AuthMethod, proxy *source.ProxyConfig) (*store.SourceArtifact, error) {
	mirrorRepo, err := f.Mirrors.OpenOrFetch(ctx, repo.URL, auth, proxy, f.mirrorFetchPlan(repo.Reference))
	if err != nil {
		return nil, err
	}
	hash, err := resolveRefHash(mirrorRepo, repo.Reference)
	if err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s ref %q: %w", repo.Namespace, repo.Name, refStr, err)
	}
	// Walk the tree at hash and write every blob into the slot's staging
	// dir via the shared gittree.Materialize helper. Submodule entries
	// are warn-and-skipped: the bare mirror has no nested object stores,
	// so resolving them would require a separate fetch that defeats the
	// point of the mirror cache. Callers that need submodule support fall
	// back to the legacy PlainClone path (Fetcher.fetch decides on
	// Spec.RecurseSubmodules).
	//
	// Symlinks materialize as real OS symlinks rather than being collapsed
	// to text files, so the rendered tree matches what a `git checkout`
	// would produce — important for kustomize bases that follow symlinked
	// component directories.
	if err := gittree.Materialize(ctx, mirrorRepo, hash, slot.Path, gittree.Options{
		OnSubmodule: func(path string) {
			slog.Warn("git mirror: skipping submodule (mirror path does not recurse)", "path", path)
		},
	}); err != nil {
		return nil, fmt.Errorf("materialize %s at %s: %w", hash, refStr, err)
	}
	if err := f.verifySignatures(repo, mirrorRepo, hash); err != nil {
		return nil, err
	}
	art := gitArtifact(repo.URL, slot.Path, hash.String())
	if err := applyIgnoreAndMark(repo, art); err != nil {
		return nil, err
	}
	return commitArtifact(repo, slot, art)
}

// mirrorFetchPlan builds the per-GitRepository mirror update plan: the
// narrow refspecs for ref plus the effective shallow depth, so every
// mirror update for the same CR agrees on depth.
func (f *Fetcher) mirrorFetchPlan(ref *manifest.GitRepositoryRef) mirror.FetchPlan {
	plan := mirrorRefSpecs(ref)
	plan.Depth = effectiveDepth(f.Depth, ref)
	return plan
}

// effectiveDepth returns the clone/fetch depth to use for ref given the
// configured depth. It forces a full clone (0) when ref pins an explicit
// commit: that commit may sit arbitrarily deep behind the tip a shallow
// fetch brings, and validateCommitBranch walks the parent chain via
// IsAncestor, which a truncated history cannot satisfy. Every other ref
// (HEAD, name, semver, tag, branch) resolves to a tip whose tree is
// complete at any depth, so the configured depth passes through.
func effectiveDepth(depth int, ref *manifest.GitRepositoryRef) int {
	if depth > 0 && ref != nil && ref.Commit != "" {
		return 0
	}
	return depth
}

func mirrorRefSpecs(ref *manifest.GitRepositoryRef) mirror.FetchPlan {
	// nil/HEAD and any commit-pinned ref take the broad clone path (empty
	// RefSpecs): HEAD must resolve, and a pinned commit must be findable on
	// any branch for validateCommitBranch's reachability check — a narrow
	// fetch of just the named branch would omit a commit that lives
	// elsewhere and report "not found" instead of "not reachable".
	if ref == nil || ref.Commit != "" {
		return mirror.FetchPlan{}
	}
	switch {
	case ref.Name != "":
		return singleMirrorRefSpec(ref.Name)
	case ref.SemVer != "":
		return singleMirrorRefSpec("refs/tags/*")
	case ref.Tag != "":
		return singleMirrorRefSpec("refs/tags/" + ref.Tag)
	case ref.Branch != "":
		return singleMirrorRefSpec("refs/heads/" + ref.Branch)
	default:
		return mirror.FetchPlan{}
	}
}

// singleMirrorRefSpec builds a FetchPlan carrying one force-update mirror
// refspec (+src:src) — the shape every narrow per-ref fetch above uses.
func singleMirrorRefSpec(src string) mirror.FetchPlan {
	return mirror.FetchPlan{RefSpecs: []config.RefSpec{config.RefSpec("+" + src + ":" + src)}}
}

// authIdentity returns the cache-key auth tag for a GitRepository.
// Combines the SecretRef (HTTPS / SSH creds) and ProxySecretRef the
// fetcher binds. Returns "" for anonymous clones so they share slots
// with the legacy un-auth-keyed layout.
func authIdentity(repo *manifest.GitRepository) string {
	return source.AuthIdentityFromRefs(repo.Namespace,
		repo.SecretRef, repo.ProxySecretRef)
}

// proxyOptions maps a resolved ProxyConfig into go-git transport options,
// shared by the clone and explicit-ref fetch paths.
func proxyOptions(proxy *source.ProxyConfig) transport.ProxyOptions {
	return transport.ProxyOptions{
		URL:      proxy.Address,
		Username: proxy.Username,
		Password: proxy.Password,
	}
}

// commitArtifact atomically commits the staged slot and stamps the
// artifact's LocalPath with the committed slot path. Shared by the
// legacy clone path (fetch) and the bare-mirror path (fetchViaMirror),
// which finalize the slot identically once materialization is done.
func commitArtifact(repo *manifest.GitRepository, slot *source.Slot, art *store.SourceArtifact) (*store.SourceArtifact, error) {
	if err := slot.Commit(); err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s: commit slot: %w", repo.Namespace, repo.Name, err)
	}
	art.LocalPath = slot.Path
	return art, nil
}

// gitArtifact builds the SourceArtifact every git fetch path returns: a
// KindGitRepository pointing at the materialized slot directory.
func gitArtifact(url, localPath, rev string) *store.SourceArtifact {
	return &store.SourceArtifact{
		Kind:      manifest.KindGitRepository,
		URL:       url,
		LocalPath: localPath,
		Revision:  rev,
	}
}
