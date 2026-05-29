package discovery

import (
	"log/slog"

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
	root := FindRepoRoot(abs)
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
// remotely but flate must satisfy from the working tree. Two passes:
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
// Both passes alias to the same working tree, so the combined result
// is logged at WARN when more than one source is aliased — multiple
// remote shared-infra repos would silently render against the same
// (wrong) tree.
//
// All namespaces are aliased, not just `flux-system` (#199): the
// convention of running Flux in a non-default namespace (e.g.
// `gitops-system`) is widespread and the bootstrap-source-points-at-
// the-local-tree property is identical regardless. A typo'd sourceRef
// silently renders against the working tree instead of failing fast —
// trade-off inherited from the original `flux-system` path.
func (d *discoverer) aliasBootstrapSources(repoRoot string) {
	aliased := d.aliasMissingKustomizationSources(repoRoot)
	aliased = append(aliased, d.overrideSelfReferentialGitRepositories(repoRoot)...)
	warnIfMultipleBootstrapAliases(aliased, repoRoot)
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
	remotes := readWorkingTreeRemotes(repoRoot)
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
		slog.Info("discovery: aliased in-tree GitRepository to working tree (URL matches working-tree remote)",
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
