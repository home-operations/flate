package discovery

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"net/url"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// seedBootstrapSource retains an admitted bootstrap input or admits a standalone
// working tree. LocalPath is the source coordinate, never a fallback disk read.
func (d *discoverer) seedBootstrapSource(ctx context.Context) (string, error) {
	id := manifest.BootstrapSourceID
	admitted, _ := d.cfg.Store.GetArtifact(id).(*store.SourceArtifact)
	root := d.cfg.RepoRoot
	if admitted != nil {
		root = cmp.Or(admitted.LocalRoot, admitted.LocalPath)
	}
	urls := d.cfg.SelfURLs
	if root == "" || len(urls) == 0 {
		abs, err := ResolveScanPath(cmp.Or(root, d.cfg.Path))
		if err != nil {
			return "", err
		}
		located, repo, _ := gitrepo.Locate(abs)
		if root == "" {
			root = cmp.Or(located, abs)
		}
		if len(urls) == 0 && repo != nil {
			urls, _ = gitrepo.RemoteURLs(repo)
		}
	}
	if admitted == nil {
		var err error
		admitted, d.cfg.Path, err = baseline.AdmitDirectory(ctx, d.cfg.Path, root)
		if err != nil {
			return "", err
		}
		d.cfg.Store.SetArtifact(id, admitted)
	}
	if admitted.FS == nil || admitted.LocalPath == "" {
		return "", fmt.Errorf("%w: bootstrap source input is unavailable", manifest.ErrInput)
	}
	if len(urls) > 0 {
		d.remotes = make(map[string]struct{}, len(urls))
		addNormalizedRemotes(d.remotes, urls...)
	}
	d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap")
	if d.cfg.Store.GetObject(id) == nil {
		d.cfg.Store.AddObject(&manifest.GitRepository{Name: id.Name, Namespace: id.Namespace, URL: "file://" + admitted.LocalPath})
	}
	return admitted.LocalPath, nil
}

// aliasMissingKustomizationSources supplies missing Git sourceRefs because
// Flux bootstrap and flux-operator FluxInstance create the cluster's source
// out of band. Each alias requires a synthetic CR and a working-tree artifact.
// All namespaces must be aliased, not just flux-system (#199): clusters can run
// Flux elsewhere and the bootstrap source's local-tree identity is independent
// of namespace. The accepted trade-off is that a typo'd sourceRef renders
// against the working tree instead of failing fast.
// Returns working-tree alias IDs for the combined multi-source warning.
func (d *discoverer) aliasMissingKustomizationSources(ctx context.Context, repoRoot string) ([]manifest.NamedResource, error) {
	// existing doubles as a dedup set: after a successful publish we add
	// the new id so repeated sourceRefs across KSes are skipped without
	// a second map.
	existing := make(map[manifest.NamedResource]struct{})
	for _, obj := range d.cfg.Store.ListObjects(manifest.SourceDomain, manifest.KindGitRepository) {
		existing[obj.Named()] = struct{}{}
	}
	var aliased []manifest.NamedResource
	for _, ks := range d.cfg.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		id := ks.SourceID()
		if _, ok := existing[id]; ok {
			continue
		}
		published, err := d.publishBootstrapAlias(ctx, id, repoRoot)
		if err != nil {
			return nil, err
		}
		if !published {
			continue
		}
		existing[id] = struct{}{}
		aliased = append(aliased, id)
	}
	return aliased, nil
}

// overrideSelfReferentialGitRepositories satisfies matching file-loaded
// GitRepositories locally: the cluster pulling itself. Real Flux fetches that
// URL with a SOPS-decrypted deploy key; offline rendering must use available
// local trees. No ref aliases this side; HEAD aliases only a matching side.
// Other locally available refs use committed artifacts with source shaping.
// Unavailable refs or committed-side submodules must warn and fetch normally.
// Returns only working-tree aliases for the combined multi-source warning.
func (d *discoverer) overrideSelfReferentialGitRepositories(ctx context.Context, repoRoot string) ([]manifest.NamedResource, error) {
	lookupRoot, sideRevision := repoRoot, ""
	if bootstrap, ok := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact); ok {
		sideRevision = bootstrap.Revision
		if bootstrap.LocalRoot != "" {
			lookupRoot = bootstrap.LocalRoot
		}
	}
	remotes := d.remotes
	debugLogRemotes(remotes)
	if len(remotes) == 0 {
		return nil, nil
	}
	var overridden []manifest.NamedResource
	repositories := d.cfg.Store.ListAs[*manifest.GitRepository](manifest.SourceDomain, manifest.KindGitRepository)
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
	artifacts, err := git.ResolveLocal(ctx, lookupRoot, pinned, sideRevision)
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
			if artifact.LocalPath != lookupRoot {
				d.hasPins = true
				artifact.LocalRoot = repoRoot
				d.cfg.Store.SetArtifact(id, artifact)
				d.cfg.Store.UpdateStatus(id, store.StatusReady, "local committed source artifact")
				continue
			}
		}
		if _, err := d.publishBootstrapAlias(ctx, id, repoRoot); err != nil {
			return nil, err
		}
		d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap alias (URL matches working tree)")
		slog.Debug("discovery: aliased in-tree GitRepository to working tree (URL matches working-tree remote)",
			"id", id.String(), "url", repo.URL, "normalizedKey", normalized, "localPath", repoRoot)
		overridden = append(overridden, id)
	}
	return overridden, nil
}

// publishBootstrapAlias gives Flux aliases source-qualified labels while
// LocalRoot retains this side's claim origin. Mutable aliases MUST keep Key
// empty; the bootstrap artifact MUST retain its direct-input semantics.
func (d *discoverer) publishBootstrapAlias(ctx context.Context, id manifest.NamedResource, repoRoot string) (bool, error) {
	obj, url, ok := newBootstrapAlias(id, repoRoot)
	if !ok {
		return false, nil
	}
	if id == manifest.BootstrapSourceID {
		return true, nil
	}
	bootstrap := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	if existing := d.cfg.Store.GetObject(id); existing != nil {
		obj = existing
	}
	repo := obj.(*manifest.GitRepository)
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	viewKey := manifest.Fingerprint(struct {
		Root, Origin, Key, Revision, Digest string
		Ignore                              string
		Sparse                              []string `json:",omitempty"`
		Recurse                             bool
	}{repoRoot, bootstrap.LocalRoot, bootstrap.Key, bootstrap.Revision, bootstrap.Digest, ignore, repo.SparseCheckout, repo.RecurseSubmodules})
	viewRoot := filepath.Join(filepath.Dir(repoRoot), ".flate-sources", viewKey)
	input := d.aliasViews[viewKey]
	var err error
	if input != nil {
		input, _, err = gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: input})
	} else {
		input, _, err = gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: bootstrap.FS, Ignore: ignore, Sparse: repo.SparseCheckout})
	}
	if err != nil {
		return false, err
	}
	if d.aliasViews == nil {
		d.aliasViews = make(map[string]fs.FS)
	}
	d.aliasViews[viewKey] = input
	artifact := *bootstrap
	artifact.Kind, artifact.URL, artifact.FS = id.Kind, url, input
	artifact.InputAlias = true
	artifact.LocalPath = filepath.Join(viewRoot, id.Namespace, id.Name)
	artifact.LocalRoot = repoRoot
	artifact.Key = ""
	d.cfg.Store.AddObject(obj)
	d.cfg.Store.SetArtifact(id, &artifact)
	d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap alias")
	slog.Debug("discovery: aliased bootstrap source", "id", id.String(), "localPath", artifact.LocalPath)
	return true, nil
}

// newBootstrapAlias builds the synthetic source manifest for id and
// returns (obj, url, true) for kinds aliasing supports. The URL is
// returned separately so callers can stamp it onto the SourceArtifact
// without re-reading the manifest.
func newBootstrapAlias(id manifest.NamedResource, repoRoot string) (manifest.BaseManifest, string, bool) {
	if id.Group != manifest.SourceDomain || id.Kind != manifest.KindGitRepository || id.Namespace == "" {
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
