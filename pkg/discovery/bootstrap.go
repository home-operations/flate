package discovery

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// seedBootstrapSource publishes a synthetic GitRepository pointing at
// the working tree's repo root — the anchor for spec.path resolution
// when a Kustomization carries no explicit sourceRef.
func (d *discoverer) seedBootstrapSource() (string, error) {
	abs, err := ResolveScanPath(d.cfg.Path)
	if err != nil {
		return "", err
	}
	// RepoRoot is the spec.path anchor. When the caller supplies it
	// explicitly (an SDK consumer rendering an extracted tree with no
	// .git), use it verbatim; otherwise fall back to the .git-ancestor
	// walk for a local working tree — byte-identical to prior behavior
	// when RepoRoot is empty.
	root := FindRepoRoot(abs)
	if d.cfg.RepoRoot != "" {
		if r, err := ResolveScanPath(d.cfg.RepoRoot); err == nil {
			root = r
		}
	}
	id := manifest.BootstrapSourceID
	// Always seed the artifact + status: even if the user authored a
	// GitRepository at this id (the `flux bootstrap` pattern), the
	// canonical reference for spec.path resolution is the local
	// working tree, not whatever URL the manifest declares. The
	// artifact is the load-bearing piece — downstream consumers read
	// LocalPath, not spec.url.
	d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  "file://" + root, LocalPath: root,
	})
	d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap")
	// Only publish the synthetic object when no user-authored
	// equivalent exists. If the user has their own
	// GitRepository/flux-system/flux-system in the tree, the loader's
	// first pass (PreferExisting=false at this point) would otherwise
	// overwrite this synthetic immediately and leave object↔artifact
	// out of sync when overrideSelfReferentialGitRepositories doesn't
	// fire (no remote URL match).
	if d.cfg.Store.GetObject(id) == nil {
		repo := &manifest.GitRepository{
			Name: id.Name, Namespace: id.Namespace,
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + root},
		}
		d.cfg.Store.AddObject(repo)
	}
	return root, nil
}

// aliasBootstrapSources resolves sources that real Flux would fetch
// remotely but flate must satisfy from the working tree. Two passes run
// here; a third (overrideRootSourcesToWorkingTree) runs later in
// discovery.Run once the parent index is built, and the caller folds all
// three into one warnIfMultipleBootstrapAliases tally:
//
//  1. aliasMissingKustomizationSources — for every Kustomization
//     whose sourceRef points at a Git/OCIRepository CR that isn't in
//     the tree (the `flux bootstrap` and flux-operator FluxInstance
//     pattern: the cluster's root source is created out-of-band), seed
//     a synthetic CR + artifact so depwait resolves it.
//  2. overrideSelfReferentialGitRepositories — for every in-tree
//     GitRepository whose spec.url matches the working tree's own git
//     remote (the Zariel/home-ops pattern: the cluster pulls itself),
//     override the artifact to the local checkout so the SOPS-decrypted
//     remote fetch is avoided.
//
// All three passes alias to the same working tree, so the caller logs at
// WARN when more than one source is aliased — multiple remote shared-infra
// repos would silently render against the same (wrong) tree. Returns the
// ids aliased here so the caller can fold pass 3's into that tally.
//
// All namespaces are aliased, not just `flux-system` (#199): the
// convention of running Flux in a non-default namespace (e.g.
// `gitops-system`) is widespread and the bootstrap-source-points-at-
// the-local-tree property is identical regardless. A typo'd sourceRef
// silently renders against the working tree instead of failing fast —
// trade-off inherited from the original `flux-system` path.
func (d *discoverer) aliasBootstrapSources(repoRoot string) []manifest.NamedResource {
	aliased := d.aliasMissingKustomizationSources(repoRoot)
	aliased = append(aliased, d.overrideSelfReferentialGitRepositories(repoRoot)...)
	return aliased
}

// overrideRootSourcesToWorkingTree is pass 3, the no-URL-match fallback. When
// passes 1–2 left a *root* Kustomization's in-tree GitRepository source
// un-aliased — because the working tree has no .git remote, the caller supplied
// no SelfURLs, or the declared spec.url simply doesn't normalize-match either —
// alias that source to the working tree anyway. A root KS (one no other KS's
// spec.path covers, i.e. absent from parentOf) pulls the cluster's own repo by
// the flux-bootstrap convention, so its source IS the working tree: flate
// renders the KS's spec.path from the local checkout, and the auth secret real
// Flux needs only to FETCH that repo is irrelevant and must not block the run
// (issue #752). Without this an in-tree bootstrap GitRepository at a non-default
// id whose URL can't be matched falls through to a real fetch, fails on the
// (SOPS-wiped / out-of-band) deploy-key Secret, and cascades every consumer to
// blocked.
//
// Four guards keep this from silently rendering an external private repo
// against the wrong files:
//   - no-URL-signal: this runs ONLY when the working tree exposes no remotes
//     and the caller supplied no SelfURLs. When remotes DO exist but none
//     matched, an unmatched in-tree GitRepository is a genuine external source
//     (shared-infra), not the cluster pulling itself — the URL match is the
//     authoritative signal and we defer to it. Pinned by
//     TestRun_LeavesUnmatchedInTreeGitRepository.
//   - root-only: a source referenced solely by a non-root tenant KS is never
//     touched (parentOf membership ⇒ skip).
//   - already-handled: a source with an artifact (pass 1/2 URL match, or the
//     seeded BootstrapSourceID) is left untouched, so a URL-matched checkout —
//     the common CI case — stays byte-identical.
//   - local-content: the root KS's spec.path must resolve to a directory inside
//     the working tree. A root pointing at a path this tree doesn't contain is
//     an external source flate can't satisfy offline — leave it to fail loud.
//
// Returns the ids aliased so the caller folds them into the multi-alias WARN.
func (d *discoverer) overrideRootSourcesToWorkingTree(repoRoot string, parentOf map[manifest.NamedResource]manifest.NamedResource) []manifest.NamedResource {
	// Only fall back to topology when there's no URL signal at all. With remotes
	// (or SelfURLs) present, pass 2's URL match is authoritative — an unmatched
	// source is external, not self, and must not be aliased to this tree.
	if len(d.selfRemotes(repoRoot)) > 0 {
		return nil
	}
	var overridden []manifest.NamedResource
	seen := map[manifest.NamedResource]struct{}{}
	for _, ks := range store.ListAs[*manifest.Kustomization](d.cfg.Store, manifest.KindKustomization) {
		if _, hasParent := parentOf[ks.Named()]; hasParent {
			continue // not a root — its source resolves through its parent's tree
		}
		src := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
		// GitRepository only. A committed OCIRepository is, by overwhelming
		// convention, an EXTERNAL artifact pull (an app/chart registry), not the
		// cluster's own tree — its missing pull Secret should soft-skip, not be
		// aliased away (locked by TestOrchestrator_AllowMissingSecretsPropagates).
		// A genuine OCI *bootstrap* source (flux-operator publishing the repo as an
		// OCI artifact) is operator-created and absent from the tree, so pass 1
		// already aliases it; there's no safe topological signal that distinguishes
		// a committed self OCI source from an external one (no URL match is possible
		// for oci://), so pass 3 stays Git-only.
		if src.Kind != manifest.KindGitRepository || src.Name == "" {
			continue
		}
		if _, done := seen[src]; done {
			continue
		}
		seen[src] = struct{}{}
		// During discovery only aliasing sets a source artifact, so a non-nil
		// artifact means pass 1/2 (or the seed) already handled this id — never
		// clobber it, keeping the URL-matched path byte-identical.
		if d.cfg.Store.GetArtifact(src) != nil {
			continue
		}
		// Must be a real in-tree GitRepository; a missing source is pass 1's job.
		if _, ok := d.cfg.Store.GetObject(src).(*manifest.GitRepository); !ok {
			continue
		}
		if !rootPathInTree(repoRoot, ks.Path) {
			continue
		}
		d.cfg.Store.SetArtifact(src, &store.SourceArtifact{
			Kind: manifest.KindGitRepository,
			URL:  "file://" + repoRoot, LocalPath: repoRoot,
		})
		d.cfg.Store.UpdateStatus(src, store.StatusReady, "bootstrap alias (root Kustomization source)")
		slog.Debug("discovery: aliased root Kustomization source to working tree (no URL match)",
			"id", src.String(), "rootKS", ks.Named().String(), "localPath", repoRoot)
		overridden = append(overridden, src)
	}
	return overridden
}

// rootPathInTree reports whether a Kustomization spec.path resolves to an
// existing directory inside repoRoot. Empty path or one that escapes the tree
// (`..`) ↦ false: a bare-sourceRef root is anchored on the already-seeded
// BootstrapSourceID, and an escaping path is never a self-pull we can satisfy.
func rootPathInTree(repoRoot, specPath string) bool {
	if specPath == "" {
		return false
	}
	clean := filepath.Join(repoRoot, filepath.FromSlash(strings.TrimPrefix(specPath, "./")))
	rel, err := filepath.Rel(repoRoot, clean)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	info, err := os.Stat(clean)
	return err == nil && info.IsDir()
}

// aliasMissingKustomizationSources is pass 1. It walks every loaded
// Kustomization and, for any unique Git/OCIRepository sourceRef that no
// in-tree CR satisfies, publishes a synthetic CR + working-tree
// artifact. Without this, dependent Kustomizations would fail depwait
// with `dependency not found`. Returns the IDs aliased so the multi-
// source WARN sees them.
func (d *discoverer) aliasMissingKustomizationSources(repoRoot string) []manifest.NamedResource {
	// existing doubles as a dedup set: after a successful publish we add
	// the new id so repeated sourceRefs across KSes are skipped without
	// a second map.
	existing := knownSourceIDs(d.cfg.Store, manifest.KindGitRepository, manifest.KindOCIRepository)
	var aliased []manifest.NamedResource
	for _, ks := range store.ListAs[*manifest.Kustomization](d.cfg.Store, manifest.KindKustomization) {
		id := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
		if _, ok := existing[id]; ok {
			continue
		}
		if !d.publishBootstrapAlias(id, repoRoot) {
			// Unsupported kind for aliasing (anything outside
			// newBootstrapAlias's switch) — silently skip; the
			// downstream depwait failure surfaces a clearer error
			// than a misleading half-publish would.
			continue
		}
		existing[id] = struct{}{}
		aliased = append(aliased, id)
	}
	return aliased
}

// overrideSelfReferentialGitRepositories is pass 2. It rewrites the
// artifact of any file-loaded GitRepository whose spec.url matches the
// working tree's own git remote — the cluster pulling itself. Real
// Flux fetches that URL with a SOPS-decrypted deploy key; flate runs
// offline so we substitute the local checkout. Returns the IDs
// overridden so the multi-alias footgun check sees them.
//
// No alreadyAliased skip-set needed: pass 1 publishes synthetic URLs
// (file:// or oci://flate-bootstrap-alias/...) that normalizeGitURL
// always rejects. A pass-1 alias can never URL-match a working-tree
// remote, so the URL-match filter below is the natural guard.
func (d *discoverer) overrideSelfReferentialGitRepositories(repoRoot string) []manifest.NamedResource {
	remotes := d.selfRemotes(repoRoot)
	debugLogRemotes(remotes)
	if len(remotes) == 0 {
		return nil
	}
	var overridden []manifest.NamedResource
	for _, repo := range store.ListAs[*manifest.GitRepository](d.cfg.Store, manifest.KindGitRepository) {
		id := repo.Named()
		normalized := normalizeGitURL(repo.URL)
		if normalized == "" {
			continue
		}
		if _, match := remotes[normalized]; !match {
			continue
		}
		d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
			Kind: manifest.KindGitRepository,
			URL:  "file://" + repoRoot, LocalPath: repoRoot,
		})
		d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap alias (URL matches working tree)")
		slog.Debug("discovery: aliased in-tree GitRepository to working tree (URL matches working-tree remote)",
			"id", id.String(), "url", repo.URL, "normalizedKey", normalized, "localPath", repoRoot)
		overridden = append(overridden, id)
	}
	return overridden
}

// publishBootstrapAlias inserts a synthetic source CR plus its
// working-tree SourceArtifact under id. Returns false when id.Kind
// isn't a kind aliasing knows how to materialize.
func (d *discoverer) publishBootstrapAlias(id manifest.NamedResource, repoRoot string) bool {
	obj, url, ok := newBootstrapAlias(id, repoRoot)
	if !ok {
		return false
	}
	d.cfg.Store.AddObject(obj)
	d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
		Kind: id.Kind, URL: url, LocalPath: repoRoot,
	})
	d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap alias")
	slog.Debug("discovery: aliased bootstrap source",
		"id", id.String(), "localPath", repoRoot)
	return true
}

// newBootstrapAlias builds the synthetic source manifest for id and
// returns (obj, url, true) for kinds aliasing supports. The URL is
// returned separately so callers can stamp it onto the SourceArtifact
// without re-reading the manifest.
func newBootstrapAlias(id manifest.NamedResource, repoRoot string) (manifest.BaseManifest, string, bool) {
	switch id.Kind {
	case manifest.KindGitRepository:
		url := "file://" + repoRoot
		return &manifest.GitRepository{
			Name: id.Name, Namespace: id.Namespace,
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: url},
		}, url, true
	case manifest.KindOCIRepository:
		// Synthetic oci:// URL — never resolved, only present so the
		// store has something to return for spec.url reads. The
		// SourceArtifact's LocalPath is what downstream consumers
		// actually use. Embed namespace so two distinct-namespace
		// OCIRepositories with the same name don't collide on URL.
		url := "oci://flate-bootstrap-alias/" + id.Namespace + "/" + id.Name
		return &manifest.OCIRepository{
			Name: id.Name, Namespace: id.Namespace,
			OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: url},
		}, url, true
	}
	return nil, "", false
}

// knownSourceIDs returns the IDs of every object currently in s for
// the given kinds. Used by aliasing pass 1 to skip sourceRefs that
// already have a real CR.
func knownSourceIDs(s *store.Store, kinds ...string) map[manifest.NamedResource]struct{} {
	out := make(map[manifest.NamedResource]struct{})
	for _, kind := range kinds {
		for _, obj := range s.ListObjects(kind) {
			out[obj.Named()] = struct{}{}
		}
	}
	return out
}

// warnIfMultipleBootstrapAliases surfaces the cross-repo footgun: when
// multiple sources are aliased to the SAME working tree, a real
// upstream shared-infra repository would render against the wrong
// files without any user-visible signal. The single-source case stays
// silent because that's the intended flux-bootstrap shape.
func warnIfMultipleBootstrapAliases(aliased []manifest.NamedResource, repoRoot string) {
	if len(aliased) <= 1 {
		return
	}
	names := make([]string, len(aliased))
	for i, a := range aliased {
		names[i] = a.String()
	}
	slog.Warn("discovery: aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
		"count", len(aliased), "ids", names, "localPath", repoRoot)
}
