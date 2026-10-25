package discovery

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type entrySource interface {
	BootstrapAlias(string) (manifest.BaseManifest, *manifest.GitRepository)
}

type entryResolution struct {
	object    manifest.BaseManifest
	input     *store.SourceArtifact
	git       *manifest.GitRepository
	subtree   string
	synthetic bool
	pinned    bool
	refs      []manifest.BaseManifest
}

// Admission supplies bytes and remote metadata; only publishEntrySource publishes identities.
func (d *discoverer) admitBootstrapInput(ctx context.Context) (*store.SourceArtifact, error) {
	admitted := d.cfg.Input
	if admitted == nil {
		admitted, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	}
	if admitted == nil {
		var err error
		admitted, d.cfg.Path, err = baseline.AdmitDirectory(ctx, d.cfg.Path, d.cfg.RepoRoot)
		if err != nil {
			return nil, err
		}
	}
	urls := d.cfg.SelfURLs
	if len(urls) == 0 {
		if input, ok := admitted.FS.(*gittree.FS); ok && input.Location() != nil {
			urls = input.Location().URLs
		}
	}
	if admitted != nil {
		for _, warning := range admitted.Warnings {
			d.cfg.Store.AddWarning(manifest.BootstrapSourceID, warning)
		}
	}
	if admitted.FS == nil || admitted.LocalPath == "" {
		return nil, fmt.Errorf("%w: bootstrap source input is unavailable", manifest.ErrInput)
	}
	if d.cfg.Path == "" {
		d.cfg.Path = admitted.LocalPath
	} else {
		scanPath, err := filepath.Abs(d.cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve scan path: %w", manifest.ErrInput, err)
		}
		d.cfg.Path = scanPath
	}
	d.remotes = make(map[string]struct{}, len(urls))
	for _, raw := range urls {
		if normalized := normalizeGitURL(raw); normalized != "" {
			d.remotes[normalized] = struct{}{}
		}
	}
	return admitted, nil
}

// Missing roots freeze at the first no-progress point; new references alone never invalidate them.
func (d *discoverer) resolveEntrySources(ctx context.Context, allowMissing bool) (int, error) {
	bootstrap, _ := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	if d.resolvedSources == nil {
		var err error
		bootstrap, err = d.admitBootstrapInput(ctx)
		if err != nil {
			return 0, err
		}
		d.resolvedSources = map[manifest.NamedResource]entryResolution{}
	}
	if previous, ok := d.resolvedSources[manifest.BootstrapSourceID]; ok && previous.input != bootstrap {
		d.aliasViews = nil
	}
	groups := map[manifest.NamedResource][]manifest.BaseManifest{manifest.BootstrapSourceID: nil}
	for kind := range manifest.Kinds() {
		if kind.Group != manifest.SourceDomain {
			continue
		}
		for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
			if _, ok := obj.(entrySource); ok {
				groups[obj.Named()] = nil
			}
		}
	}
	evidence := map[manifest.NamedResource]store.Child{}
	if d.loader != nil {
		for child := range d.loader.Sources() {
			evidence[child.Obj.Named()] = child
		}
		for _, child := range d.loader.Kustomizations() {
			ref, _, _ := child.Obj.SourcePath()
			if allowMissing || evidence[ref].Obj != nil || d.cfg.Store.GetObject(ref) != nil {
				groups[ref] = append(groups[ref], child.Obj)
			}
		}
	}
	if allowMissing {
		for kind := range manifest.Kinds() {
			if !kind.Runnable {
				continue
			}
			for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
				_, _, hasPath := obj.SourcePath()
				for ref := range manifest.References(obj, manifest.ReferenceRelations) {
					if ref.Group != manifest.SourceDomain || ref.Use != "source" && ref.Use != "artifact" {
						continue
					}
					if hasPath {
						if !slices.Contains(groups[ref.NamedResource], obj) {
							groups[ref.NamedResource] = append(groups[ref.NamedResource], obj)
						}
					} else if _, seen := groups[ref.NamedResource]; !seen {
						groups[ref.NamedResource] = nil
					}
				}
			}
		}
	}

	candidates := map[manifest.NamedResource]entryResolution{}
	var pinned []*manifest.GitRepository
	for _, id := range slices.SortedFunc(maps.Keys(groups), manifest.NamedResource.Compare) {
		obj := d.cfg.Store.GetObject(id)
		if obj == nil {
			obj = evidence[id].Obj
			if obj != nil && slices.ContainsFunc(groups[id], func(witness manifest.BaseManifest) bool {
				return d.cfg.Store.GetObject(witness.Named()) != nil
			}) {
				previous, resolved := d.resolvedSources[id]
				artifact, _ := d.cfg.Store.GetArtifact(id).(*store.SourceArtifact)
				d.cfg.Store.AddObject(obj, evidence[id].Doc)
				if resolved && previous.object == obj && artifact != nil {
					d.publishEntrySource(previous, artifact)
				}
			}
		}
		previous, seen := d.resolvedSources[id]
		if seen && obj == nil && previous.input == bootstrap && slices.Equal(previous.refs, groups[id]) {
			continue
		}
		if seen && obj != nil && previous.object == obj && (d.cfg.Store.GetArtifact(id) != nil || d.cfg.Store.GetObject(id) != nil) {
			art, _ := d.cfg.Store.GetArtifact(id).(*store.SourceArtifact)
			if previous.input == bootstrap || art == nil || !art.InputAlias && id != manifest.BootstrapSourceID {
				continue
			}
			previous.input = bootstrap
			if previous.pinned {
				pinned = append(pinned, previous.git)
			}
			candidates[id] = previous
			continue
		}
		synthetic := obj == nil
		if synthetic {
			obj, _ = id.DecodeIdentity()
		}
		behavior, ok := obj.(entrySource)
		if !ok {
			continue
		}
		alias, descriptor := behavior.BootstrapAlias(bootstrap.LocalPath)
		candidate := entryResolution{object: obj, input: bootstrap, git: descriptor, subtree: ".", synthetic: synthetic}
		candidate.pinned = !synthetic && id != manifest.BootstrapSourceID &&
			descriptor != nil && (*manifest.GitRepositoryRef)(descriptor.Reference).String() != ""
		if id != manifest.BootstrapSourceID {
			if !synthetic {
				if descriptor == nil {
					d.resolvedSources[id] = candidate
					continue
				}
				if _, match := d.remotes[normalizeGitURL(descriptor.URL)]; !match {
					d.resolvedSources[id] = candidate
					continue
				}
				if candidate.pinned {
					pinned = append(pinned, descriptor)
				}
			} else if descriptor == nil {
				slices.SortFunc(groups[id], func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
				var err error
				candidate.subtree, err = d.bootstrapArtifactRoot(ctx, groups[id])
				if err != nil {
					return 0, err
				}
				if candidate.subtree == "" {
					candidate.object, candidate.refs = nil, groups[id]
					d.resolvedSources[id] = candidate
					continue
				}
				alias, _ = behavior.BootstrapAlias(filepath.Join(bootstrap.LocalPath, candidate.subtree))
			}
		}
		if synthetic {
			candidate.object = alias
		}
		candidates[id] = candidate
	}
	lookupRoot := cmp.Or(bootstrap.LocalRoot, bootstrap.LocalPath)
	var location *gitrepo.Location
	if input, ok := bootstrap.FS.(*gittree.FS); ok {
		location = input.Location()
	}
	artifacts, err := git.ResolveLocal(ctx, location, pinned, bootstrap.Revision)
	for id, artifact := range artifacts {
		for _, warning := range artifact.Warnings {
			d.cfg.Store.AddWarning(id, warning)
		}
	}
	if err != nil {
		return 0, err
	}
	published := 0
	for _, id := range slices.SortedFunc(maps.Keys(candidates), manifest.NamedResource.Compare) {
		candidate := candidates[id]
		var artifact *store.SourceArtifact
		if id == manifest.BootstrapSourceID {
			artifact = bootstrap
		} else if candidate.pinned {
			artifact = artifacts[id]
			if artifact == nil {
				d.resolvedSources[id] = candidate
				d.cfg.Store.AddWarning(id, manifest.Warning{Resource: id, Category: manifest.WarnSourceRefUnavailable,
					Message: "declared source ref cannot be satisfied locally; using normal source fetch",
					Detail:  []string{(*manifest.GitRepositoryRef)(candidate.git.Reference).String(), "ref unavailable in this tree's repository or checkout options require fetching"}})
				continue
			}
			if artifact.LocalPath == lookupRoot {
				artifact = nil
			} else {
				d.hasPins = true
				artifact.LocalRoot = bootstrap.LocalPath
			}
		}
		if artifact == nil {
			artifact, err = d.entrySourceView(ctx, candidate)
			if err != nil {
				return 0, err
			}
		}
		d.publishEntrySource(candidate, artifact)
		if id != manifest.BootstrapSourceID {
			published++
		}
	}
	return published, nil
}

func (d *discoverer) bootstrapArtifactRoot(ctx context.Context, witnesses []manifest.BaseManifest) (string, error) {
	bootstrap := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	scan, err := filepath.Rel(bootstrap.LocalPath, d.cfg.Path)
	if err != nil {
		return "", err
	}
	if !safepath.Contains(bootstrap.LocalPath, d.cfg.Path) {
		return "", nil
	}
	var selected string
	for candidate := filepath.ToSlash(scan); ; candidate = path.Dir(candidate) {
		valid := true
		for _, witness := range witnesses {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			_, authoredPath, _ := witness.SourcePath()
			name := path.Join(candidate, manifest.NormalizeClaimBase(authoredPath))
			if !safepath.Contains(filepath.Join(bootstrap.LocalPath, candidate), filepath.Join(bootstrap.LocalPath, name)) {
				valid = false
				break
			}
			info, err := fs.Stat(bootstrap.FS, name)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return "", fmt.Errorf("%w: inspect bootstrap artifact root: %w", manifest.ErrInput, err)
			}
			if err != nil || !info.IsDir() {
				valid = false
				break
			}
		}
		if valid {
			selected = candidate
		}
		if candidate == "." {
			return selected, nil
		}
	}
}

// Mutable aliases keep empty keys and source-qualified mounts; their input is immutable.
func (d *discoverer) entrySourceView(ctx context.Context, candidate entryResolution) (*store.SourceArtifact, error) {
	bootstrap, subtree := candidate.input, candidate.subtree
	repoRoot := bootstrap.LocalPath
	ignore := ""
	var sparse []string
	recurse := false
	if repo := candidate.git; repo != nil {
		if repo.Ignore != nil {
			ignore = *repo.Ignore
		}
		sparse, recurse = repo.SparseCheckout, repo.RecurseSubmodules
	}
	label, viewKey := gittree.Label(gittree.LabelOptions{Root: repoRoot, Mode: gittree.Source, Source: candidate.object.Named(), Content: struct {
		Root, Subtree, Origin, Key, Revision, Digest string
		Ignore                                       string
		Sparse                                       []string `json:",omitempty"`
		Recurse                                      bool
	}{repoRoot, subtree, bootstrap.LocalRoot, bootstrap.Key, bootstrap.Revision, bootstrap.Digest, ignore, sparse, recurse}})
	var input *gittree.FS
	var err error
	if cached := d.aliasViews[viewKey]; cached != nil {
		input, _, err = gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: cached})
	} else {
		current := bootstrap.FS
		if subtree != "." {
			current, err = fs.Sub(current, subtree)
		}
		if err == nil {
			input, _, err = gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: current, Ignore: ignore, Sparse: sparse})
		}
	}
	if err != nil {
		return nil, err
	}
	if d.aliasViews == nil {
		d.aliasViews = map[string]fs.FS{}
	}
	d.aliasViews[viewKey] = input
	id := candidate.object.Named()
	artifact := *bootstrap
	artifact.Kind, artifact.URL, artifact.FS = id.Kind, "file://"+filepath.Join(repoRoot, subtree), input
	artifact.InputAlias, artifact.Key = true, ""
	artifact.Warnings = nil
	artifact.LocalPath = label
	artifact.LocalRoot = filepath.Join(repoRoot, subtree)
	return &artifact, nil
}

func (d *discoverer) publishEntrySource(candidate entryResolution, artifact *store.SourceArtifact) {
	id := candidate.object.Named()
	if candidate.synthetic {
		d.cfg.Store.AddObject(candidate.object)
	}

	d.cfg.Store.SetArtifact(id, artifact)
	d.cfg.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "local entry source"})
	d.resolvedSources[id] = candidate
}

// Only working-input aliases contribute; canonical and independently committed artifacts do not.
func (d *discoverer) warnBootstrapAliases(repoRoot string) {
	var names []string
	for id := range d.resolvedSources {
		artifact, _ := d.cfg.Store.GetArtifact(id).(*store.SourceArtifact)
		if id != manifest.BootstrapSourceID && artifact != nil && artifact.InputAlias {
			names = append(names, id.String())
		}
	}
	if len(names) <= 1 {
		return
	}
	slices.Sort(names)
	d.cfg.Store.AddWarning(manifest.NamedResource{}, manifest.Warning{Category: manifest.WarnBootstrapAliases,
		Message: "aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
		Detail:  append(names, repoRoot)})
}

func normalizeGitURL(raw string) string {
	raw = strings.TrimSpace(raw)
	endpoint, err := transport.NewEndpoint(raw)
	if err != nil {
		return ""
	}
	switch strings.ToLower(endpoint.Protocol) {
	case "http", "https", "ssh", "git":
	default:
		return ""
	}
	host, remotePath := strings.Trim(endpoint.Host, "[]"), endpoint.Path
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return ""
		}
		host, remotePath = parsed.Hostname(), parsed.Path
	}
	if host == "" || strings.ContainsAny(host, "/\\") || remotePath == "" {
		return ""
	}
	remotePath = strings.TrimSuffix(remotePath, "/")
	remotePath = strings.TrimSuffix(remotePath, ".git")
	remotePath = strings.TrimPrefix(remotePath, "/")
	if remotePath == "" {
		return ""
	}
	return strings.ToLower(host + "/" + remotePath)
}
