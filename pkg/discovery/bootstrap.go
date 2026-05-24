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

// aliasBootstrapSources seeds a working-tree SourceArtifact for every
// GitRepository referenced by a loaded Kustomization whose definition
// isn't in the repo itself. Targets `flux bootstrap` / flux-operator
// FluxInstance patterns: the cluster's root GitRepository is created
// out-of-band (the source that delivers the rest of the manifests
// cannot, by construction, be one of the manifests it delivers), so
// no static manifest exists in the tree to discover. Without aliasing,
// every Kustomization referencing it via `sourceRef` fails depwait
// with "dependency not found" (issue #199).
//
// All namespaces are aliased, not just `flux-system` — the convention
// of running Flux in a non-default namespace (e.g. `gitops-system`)
// is widespread, and the bootstrap-source-points-at-the-local-tree
// property is identical regardless of where the user happens to deploy
// Flux. A typo'd sourceRef name will silently render against the
// working tree instead of failing fast — same trade-off the
// `flux-system` path already accepted.
func (d *discoverer) aliasBootstrapSources(repoRoot string) {
	known := make(map[manifest.NamedResource]struct{})
	for _, obj := range d.cfg.Store.ListObjects(manifest.KindGitRepository) {
		known[obj.Named()] = struct{}{}
	}
	seen := make(map[manifest.NamedResource]struct{})
	var aliased []manifest.NamedResource
	for _, obj := range d.cfg.Store.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok || ks.SourceKind != manifest.KindGitRepository {
			continue
		}
		id := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: ks.SourceNamespace, Name: ks.SourceName}
		if _, ok := known[id]; ok {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		alias := &manifest.GitRepository{
			Name: id.Name, Namespace: id.Namespace,
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + repoRoot},
		}
		d.cfg.Store.AddObject(alias)
		d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
			Kind: manifest.KindGitRepository,
			URL:  alias.URL, LocalPath: repoRoot,
		})
		d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap alias")
		slog.Debug("discovery: aliased bootstrap GitRepository",
			"id", id.String(), "localPath", repoRoot)
		aliased = append(aliased, id)
	}

	// Second pass: a GitRepository CR may be file-loaded AND point
	// at the same remote that flate is being run against (the
	// Zariel/home-ops pattern — the in-tree GitRepository URL is
	// the cluster's own deploy-key SSH URL). Real Flux fetches that
	// URL with a SOPS-decrypted key; flate runs offline, so the
	// fetch either round-trips slowly over the network or fails on
	// the wiped-to-placeholder key. URL-match those against the
	// working tree's git remotes and override the artifact to use
	// the working tree directly. Single explicit warn when this
	// fires so misattributed in-tree GitRepositories (a coincidental
	// URL match on a real shared-infra repo) are spottable in logs.
	remotes := readWorkingTreeRemotes(repoRoot)
	debugLogRemotes(remotes)
	if len(remotes) > 0 {
		for _, obj := range d.cfg.Store.ListObjects(manifest.KindGitRepository) {
			repo, ok := obj.(*manifest.GitRepository)
			if !ok {
				continue
			}
			id := repo.Named()
			if _, alreadyAliased := seen[id]; alreadyAliased {
				continue
			}
			normalized := normalizeGitURL(repo.URL)
			if normalized == "" {
				continue
			}
			if _, match := remotes[normalized]; !match {
				continue
			}
			seen[id] = struct{}{}
			d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
				Kind: manifest.KindGitRepository,
				URL:  "file://" + repoRoot, LocalPath: repoRoot,
			})
			d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap alias (URL matches working tree)")
			slog.Info("discovery: aliased in-tree GitRepository to working tree (URL matches working-tree remote)",
				"id", id.String(), "url", repo.URL, "normalizedKey", normalized, "localPath", repoRoot)
			aliased = append(aliased, id)
		}
	}
	// Multiple unresolved GitRepositories is the cross-repo footgun:
	// each gets aliased to the SAME working tree, so a real upstream
	// shared-infra GitRepository would render against the wrong files
	// without any user-visible signal. Warn so an operator can spot
	// the divergence; the single-source case stays Debug because
	// that's the intended flux-bootstrap shape.
	if len(aliased) > 1 {
		names := make([]string, len(aliased))
		for i, id := range aliased {
			names[i] = id.String()
		}
		slog.Warn("discovery: aliased multiple GitRepositories to the working tree; cross-repo refs render against the wrong tree",
			"count", len(aliased), "ids", names, "localPath", repoRoot)
	}
}
