package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/store"
)

// seedBootstrapSource initializes source identity before discovery reads it.
func (d *discoverer) seedBootstrapSource(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("%w: initialize discovery: %w", manifest.ErrFlux, err)
	}
	abs, err := ResolveScanPath(d.cfg.Path)
	if err != nil {
		return "", err
	}
	root := abs
	if d.cfg.RepoRoot != "" {
		root, err = ResolveScanPath(d.cfg.RepoRoot)
		if err != nil {
			return "", err
		}
	}
	urls := d.cfg.SelfURLs
	if d.cfg.RepoRoot == "" || len(urls) == 0 {
		located, repo, _ := gitrepo.Locate(root)
		if d.cfg.RepoRoot == "" && located != "" {
			root = located
		}
		if len(urls) == 0 && repo != nil {
			urls, _ = gitrepo.RemoteURLs(repo)
		}
	}
	if len(urls) > 0 {
		d.remotes = make(map[string]struct{}, len(urls))
		addNormalizedRemotes(d.remotes, urls...)
	}
	input, err := source.NewLocalFS(ctx, root)
	if err != nil {
		return "", err
	}
	id := manifest.BootstrapSourceID
	// Always seed the artifact + status: even if the user authored a
	// GitRepository at this id (the `flux bootstrap` pattern), the
	// canonical reference for spec.path resolution is the local
	// working tree, not whatever URL the manifest declares. The
	// artifact supplies content before consumers and indexes are built.
	d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  "file://" + root, LocalPath: root, FS: input,
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

// aliasMissingKustomizationSources supplies missing Git sourceRefs because
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
	existing := make(map[manifest.NamedResource]struct{})
	for _, obj := range d.cfg.Store.ListObjects(manifest.KindGitRepository) {
		existing[obj.Named()] = struct{}{}
	}
	var aliased []manifest.NamedResource
	for _, ks := range d.cfg.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
		id := ks.SourceID()
		if _, ok := existing[id]; ok {
			continue
		}
		if !d.publishBootstrapAlias(id, repoRoot) {
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
	remotes := d.remotes
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
	artifacts, err := git.ResolveLocal(ctx, repoRoot, pinned)
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
			FS: d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact).FS,
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
		FS: d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact).FS,
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
	if id.Kind != manifest.KindGitRepository {
		return nil, "", false
	}
	url := "file://" + repoRoot
	return &manifest.GitRepository{
		Name: id.Name, Namespace: id.Namespace,
		URL: url,
	}, url, true
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

// addNormalizedRemotes normalizes each URL and inserts the non-empty
// results into out, skipping URLs that don't describe a remote.
func addNormalizedRemotes(out map[string]struct{}, urls ...string) {
	for _, u := range urls {
		if n := normalizeGitURL(u); n != "" {
			out[n] = struct{}{}
		}
	}
}

// normalizeGitURL reduces git URL variants to a canonical
// host/owner/repo key suitable for cross-syntax equality. Strips
// scheme, userinfo, port, query, fragment, trailing .git/`/`, and
// lowercases the result (GitHub/GitLab paths are case-insensitive
// in practice). IPv6 hosts are returned bare (no brackets).
//
// Examples normalize to "github.com/owner/repo":
//
//	ssh://git@github.com/Owner/Repo.git
//	git@github.com:owner/repo.git
//	https://github.com/owner/repo
//	https://user:pass@github.com/owner/repo.git
//	https://github.com/owner/repo?deploy_key=prod#section
//	https://github.com:443/owner/repo.git/
//
// Returns "" for inputs that don't describe a remote (file://,
// local paths, empty) — those don't participate in bootstrap-alias
// matching.
func normalizeGitURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	host, path, ok := parseGitURL(raw)
	if !ok || host == "" || path == "" {
		return ""
	}
	path = strings.TrimSuffix(path, "/")
	path = strings.TrimSuffix(path, ".git")
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return ""
	}
	return strings.ToLower(host + "/" + path)
}

// parseGitURL extracts the host and path components of a remote git
// URL, handling both URL-form (scheme://...) and scp-style
// (user@host:path). Returns ok=false for non-remote shapes (file://,
// local paths, malformed input). Userinfo, port, query, and fragment
// are dropped — net/url handles the URL-form parsing including
// bracketed IPv6 hosts.
func parseGitURL(raw string) (host, path string, ok bool) {
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", "", false
		}
		switch strings.ToLower(u.Scheme) {
		case "http", "https", "ssh", "git":
			// supported remote schemes
		default:
			// file://, ftp://, anything else — not a remote we
			// can compare against a working-tree clone.
			return "", "", false
		}
		// url.Hostname strips both port and IPv6 brackets correctly.
		return u.Hostname(), u.Path, true
	}
	// scp-style: user@host:owner/repo (or host:owner/repo without
	// user). The first `:` after the first `@` (or after the start
	// when no `@`) separates host from path. Anything that doesn't
	// match this shape is treated as a local path and rejected.
	rest := raw
	if _, after, ok := strings.Cut(rest, "@"); ok {
		rest = after
	}
	host, path, ok = strings.Cut(rest, ":")
	if !ok || host == "" {
		return "", "", false
	}
	if strings.ContainsAny(host, "/\\") {
		// Looks like a relative path containing a colon, not
		// host:path. Reject.
		return "", "", false
	}
	return host, "/" + path, true
}

// debugLogRemotes is a small helper to keep the discovery flow log
// readable when working-tree remote inspection is requested.
func debugLogRemotes(remotes map[string]struct{}) {
	if len(remotes) == 0 {
		return
	}
	// Sorted so repeated runs over the same working tree log the
	// remotes in a stable order.
	keys := slices.Sorted(maps.Keys(remotes))
	slog.Debug("discovery: working tree remotes", "count", len(keys), "remotes", keys)
}
