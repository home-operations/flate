package discovery

import (
	"context"
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/git"
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
			URL: "file://" + root,
		}
		d.cfg.Store.AddObject(repo)
	}
	return root, nil
}

// aliasMissingKustomizationSources supplies missing Git/OCI sourceRefs because
// Flux bootstrap and flux-operator FluxInstance create the cluster's source
// out of band. Each alias requires a synthetic CR and a working-tree artifact.
// All namespaces must be aliased, not just flux-system (#199): clusters can run
// Flux elsewhere and the bootstrap source's local-tree identity is independent
// of namespace. The accepted trade-off is that a typo'd sourceRef renders
// against the working tree instead of failing fast.
// Returns working-tree alias IDs for the combined multi-source warning.
func (d *discoverer) aliasMissingKustomizationSources(repoRoot string) []manifest.NamedResource {
	// existing doubles as a dedup set: after a successful publish we add
	// the new id so repeated sourceRefs across KSes are skipped without
	// a second map.
	existing := knownSourceIDs(d.cfg.Store, manifest.KindGitRepository, manifest.KindOCIRepository)
	var aliased []manifest.NamedResource
	for _, ks := range d.cfg.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
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

// overrideSelfReferentialGitRepositories satisfies matching file-loaded
// GitRepositories locally: the cluster pulling itself. Real Flux fetches that
// URL with a SOPS-decrypted deploy key; offline rendering must use available
// local trees. No ref or HEAD uses the checkout; another locally available ref
// uses the committed artifact. Unavailable refs or non-HEAD sparse/submodule
// options must warn and fetch normally.
// Returns only working-tree aliases for the combined multi-source warning.
func (d *discoverer) overrideSelfReferentialGitRepositories(ctx context.Context, repoRoot string) ([]manifest.NamedResource, error) {
	remotes := d.selfRemotes(repoRoot)
	debugLogRemotes(remotes)
	if len(remotes) == 0 {
		return nil, nil
	}
	var overridden []manifest.NamedResource
	repositories := d.cfg.Store.ListAs[*manifest.GitRepository](manifest.KindGitRepository)
	var pinned []*manifest.GitRepository
	for _, repo := range repositories {
		if d.resolvedSources[repo.Named()] == repo {
			continue
		}
		if repo.Named() == manifest.BootstrapSourceID || repo.Reference == nil || manifest.GitRefString(*repo.Reference) == "" {
			continue
		}
		if _, match := remotes[normalizeGitURL(repo.URL)]; match {
			pinned = append(pinned, repo)
		}
	}
	artifacts, err := git.ResolveLocal(ctx, repoRoot, pinned, d.cfg.SourceCache)
	if err != nil {
		return nil, err
	}
	for _, repo := range repositories {
		id := repo.Named()
		if d.resolvedSources[id] == repo {
			continue
		}
		normalized := normalizeGitURL(repo.URL)
		if normalized == "" {
			continue
		}
		if _, match := remotes[normalized]; !match {
			continue
		}
		if d.resolvedSources == nil {
			d.resolvedSources = make(map[manifest.NamedResource]*manifest.GitRepository)
		}
		d.resolvedSources[id] = repo
		if id != manifest.BootstrapSourceID && repo.Reference != nil && manifest.GitRefString(*repo.Reference) != "" {
			artifact := artifacts[id]
			if artifact == nil {
				slog.Warn("discovery: declared source ref cannot be satisfied locally; using normal source fetch",
					"id", id.String(), "ref", manifest.GitRefString(*repo.Reference),
					"reason", "ref unavailable in this tree's repository or checkout options require fetching")
				continue
			}
			if artifact.LocalPath != repoRoot {
				d.hasPins = true
				artifact.LocalRoot = repoRoot
				d.cfg.Store.SetArtifact(id, artifact)
				d.cfg.Store.UpdateStatus(id, store.StatusReady, "local committed source artifact")
				continue
			}
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
	return overridden, nil
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
			URL: url,
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
			URL: url,
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

// warnIfMultipleBootstrapAliases must combine only working-tree aliases:
// remote shared-infra repositories would otherwise render against the same
// wrong tree without a diagnostic. Pinned sources retain their own committed
// artifacts and must not contribute. One alias is the Flux bootstrap shape.
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
