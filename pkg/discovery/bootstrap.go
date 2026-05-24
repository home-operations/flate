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

	repo := &manifest.GitRepository{
		Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace,
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + root},
	}
	id := repo.Named()
	d.cfg.Store.AddObject(repo)
	d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: root,
	})
	d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap")
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
	aliased = append(aliased, d.overrideSelfReferentialGitRepositories(repoRoot, aliased)...)
	warnIfMultipleBootstrapAliases(aliased, repoRoot)
}

// aliasMissingKustomizationSources is pass 1. It walks every loaded
// Kustomization and, for any unique Git/OCIRepository sourceRef that no
// in-tree CR satisfies, publishes a synthetic CR + working-tree
// artifact. Without this, dependent Kustomizations would fail depwait
// with `dependency not found`. Returns the IDs aliased so pass 2 can
// skip them.
func (d *discoverer) aliasMissingKustomizationSources(repoRoot string) []manifest.NamedResource {
	existing := d.knownSourceIDs(manifest.KindGitRepository, manifest.KindOCIRepository)
	seen := make(map[manifest.NamedResource]struct{})
	var aliased []manifest.NamedResource
	for _, ks := range store.ListAs[*manifest.Kustomization](d.cfg.Store, manifest.KindKustomization) {
		id := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
		if _, dup := seen[id]; dup {
			continue
		}
		if _, ok := existing[id]; ok {
			continue
		}
		if !d.publishBootstrapAlias(id, repoRoot) {
			// Unsupported kind (HelmRepository, Bucket, etc.) — these
			// can't be aliased to a filesystem path, so we silently
			// skip; the downstream depwait failure surfaces a clearer
			// error than a misleading half-publish would.
			continue
		}
		seen[id] = struct{}{}
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
// Skips IDs in alreadyAliased (defensive — pass 1 publishes synthetic
// URLs that don't match real remotes, but staying explicit prevents
// double-status writes).
func (d *discoverer) overrideSelfReferentialGitRepositories(repoRoot string, alreadyAliased []manifest.NamedResource) []manifest.NamedResource {
	remotes := readWorkingTreeRemotes(repoRoot)
	debugLogRemotes(remotes)
	if len(remotes) == 0 {
		return nil
	}
	skip := namedResourceSet(alreadyAliased)
	var overridden []manifest.NamedResource
	for _, repo := range store.ListAs[*manifest.GitRepository](d.cfg.Store, manifest.KindGitRepository) {
		id := repo.Named()
		if _, ok := skip[id]; ok {
			continue
		}
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
		// actually use.
		url := "oci://flate-bootstrap-alias/" + id.Name
		return &manifest.OCIRepository{
			Name: id.Name, Namespace: id.Namespace,
			OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: url},
		}, url, true
	}
	return nil, "", false
}

// knownSourceIDs returns a set of the IDs of every object currently in
// the store for the given kinds. Used by pass 1 to skip sourceRefs
// that already have a real CR.
func (d *discoverer) knownSourceIDs(kinds ...string) map[manifest.NamedResource]struct{} {
	out := make(map[manifest.NamedResource]struct{})
	for _, kind := range kinds {
		for _, obj := range d.cfg.Store.ListObjects(kind) {
			out[obj.Named()] = struct{}{}
		}
	}
	return out
}

// namedResourceSet builds a set lookup from a slice of NamedResource
// IDs.
func namedResourceSet(ids []manifest.NamedResource) map[manifest.NamedResource]struct{} {
	out := make(map[manifest.NamedResource]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
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
	for i, id := range aliased {
		names[i] = id.String()
	}
	slog.Warn("discovery: aliased multiple GitRepositories to the working tree; cross-repo refs render against the wrong tree",
		"count", len(aliased), "ids", names, "localPath", repoRoot)
}
