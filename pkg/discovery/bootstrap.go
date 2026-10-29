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
	"sync/atomic"

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
	artifact  *store.SourceArtifact
	warnings  []manifest.Warning
	viewKey   string
}

// Admission supplies bytes and remote metadata; only publishEntrySource publishes identities.
func (d *discoverer) admitBootstrapInput(ctx context.Context) (*store.SourceArtifact, error) {
	admitted := d.cfg.Input
	if admitted == nil {
		admitted, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
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
			d.cfg.Store.AddWarning(warning)
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
	bootstrap, _ := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
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
	add := func(id manifest.NamedResource, witness manifest.BaseManifest) {
		if _, seen := groups[id]; !seen {
			groups[id] = nil
		}
		if witness != nil && !slices.Contains(groups[id], witness) {
			groups[id] = append(groups[id], witness)
		}
	}
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
		for _, child := range d.loader.PathObjects() {
			if applier, ok := child.Obj.(entryApplication); ok {
				if application, valid := applier.EntryApplication(); valid && application.Declaration != nil {
					continue
				}
			}
			ref, _, _ := child.Obj.SourcePath()
			if allowMissing || evidence[ref].Obj != nil || d.cfg.Store.GetObject(ref) != nil {
				add(ref, child.Obj)
			}
		}
	}
	if allowMissing {
		addReferences := func(obj, witness manifest.BaseManifest) {
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				if ref.Group == manifest.SourceDomain && (ref.Use == "source" || ref.Use == "artifact") {
					add(ref.NamedResource, witness)
				}
			}
		}
		for kind := range manifest.Kinds() {
			if !kind.Runnable {
				continue
			}
			for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
				var witness manifest.BaseManifest
				if _, _, hasPath := obj.SourcePath(); hasPath {
					witness = obj
				}
				addReferences(obj, witness)
			}
		}
		if d.loader != nil {
			for id, record := range d.loader.Index.Files() {
				if !id.KindInfo().Runnable || d.cfg.Store.GetObject(id) != nil {
					continue
				}
				obj := d.loader.IndexedObject(id, record).Obj
				if d.loader.AdmitsRoot(obj, record.Path) {
					addReferences(obj, nil)
				}
			}
		}
	}

	admission := d.admission()
	admission.input = bootstrap
	published := 0
	for _, id := range slices.SortedFunc(maps.Keys(groups), manifest.NamedResource.Compare) {
		obj := d.cfg.Store.GetObject(id)
		if obj == nil {
			obj = evidence[id].Obj
			if obj != nil && slices.ContainsFunc(groups[id], func(witness manifest.BaseManifest) bool { return d.cfg.Store.GetObject(witness.Named()) != nil }) {
				d.cfg.Store.AddObject(obj, evidence[id].Doc)
			}
		}
		previous, seen := d.resolvedSources[id]
		if seen && obj != nil && previous.object == obj && previous.input != bootstrap && previous.artifact != nil && (previous.synthetic || previous.artifact.InputAlias) {
			previous.input = bootstrap
			if id == manifest.BootstrapSourceID {
				previous.artifact = bootstrap
				d.publishEntrySource(previous, bootstrap)
				continue
			}
			artifact, key, err := admission.entrySourceView(ctx, previous)
			if err != nil {
				return 0, err
			}
			previous.artifact, previous.viewKey = artifact, key
			d.cfg.Store.SetArtifact(id, artifact)
			d.cfg.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Message: "local entry source"})
			d.resolvedSources[id] = previous
			if id != manifest.BootstrapSourceID {
				published++
			}
			continue
		}
		if seen && previous.input == bootstrap && (obj == nil && slices.Equal(previous.refs, groups[id]) || obj != nil && previous.object == obj) {
			if previous.artifact != nil && obj != nil && d.cfg.Store.GetArtifact(id, store.ArtifactContent) == nil {
				d.publishEntrySource(previous, previous.artifact)
				if id != manifest.BootstrapSourceID {
					published++
				}
			}
			continue
		}
		candidate, err := admission.resolve(ctx, id, obj, groups[id])
		if err != nil {
			return 0, err
		}
		d.resolvedSources[id] = candidate
		for _, warning := range candidate.warnings {
			d.cfg.Store.AddWarning(warning)
		}
		if candidate.artifact == nil {
			continue
		}
		if candidate.pinned && !candidate.artifact.InputAlias {
			d.hasPins = true
		}
		d.publishEntrySource(candidate, candidate.artifact)
		if candidate.viewKey != "" {
			if d.aliasViews == nil {
				d.aliasViews = map[string]*store.SourceArtifact{}
			}
			if d.aliasViews[candidate.viewKey] == nil {
				d.aliasViews[candidate.viewKey] = candidate.artifact
			}
		}
		if id != manifest.BootstrapSourceID {
			published++
		}
	}
	return published, nil
}

// Admission retains immutable bootstrap policy for early and runtime source decisions.
type Admission struct {
	applications atomic.Pointer[applicationEvidence]
	entryBound   atomic.Bool
	sources      map[manifest.NamedResource]entryResolution
	input        *store.SourceArtifact
	path         string
	remotes      map[string]struct{}
	aliasViews   map[string]*store.SourceArtifact
}

func (d *discoverer) admission() *Admission {
	input, _ := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if input == nil {
		input = d.cfg.Input
	}
	return &Admission{input: input, path: d.cfg.Path, remotes: maps.Clone(d.remotes), aliasViews: maps.Clone(d.aliasViews), sources: maps.Clone(d.resolvedSources)}
}

// Resolve admits a known source or a missing source reference with its requesting witnesses.
// A nil artifact leaves normal acquisition responsible for the source.
func (a *Admission) Resolve(ctx context.Context, id manifest.NamedResource, object manifest.BaseManifest, witnesses []manifest.BaseManifest) (manifest.BaseManifest, *store.SourceArtifact, error) {
	if a == nil {
		return nil, nil, nil
	}
	resolved, err := a.resolve(ctx, id, object, witnesses)
	if resolved.artifact != nil && len(resolved.warnings) != 0 {
		captured := *resolved.artifact
		captured.Warnings = append(slices.Clone(captured.Warnings), resolved.warnings...)
		resolved.artifact = &captured
	}
	return resolved.object, resolved.artifact, err
}

func (a *Admission) resolve(ctx context.Context, id manifest.NamedResource, obj manifest.BaseManifest, witnesses []manifest.BaseManifest) (entryResolution, error) {
	bootstrap := a.input
	candidate := entryResolution{object: obj, input: bootstrap, subtree: ".", synthetic: obj == nil, refs: slices.Clone(witnesses)}
	if obj == nil {
		obj, _ = id.BindLocal(bootstrap.LocalPath)
	}
	behavior, ok := obj.(entrySource)
	if !ok {
		return candidate, nil
	}
	alias, descriptor := behavior.BootstrapAlias(bootstrap.LocalPath)
	candidate.git = descriptor
	candidate.pinned = !candidate.synthetic && id != manifest.BootstrapSourceID && descriptor != nil && (*manifest.GitRepositoryRef)(descriptor.Reference).String() != ""
	if id != manifest.BootstrapSourceID {
		if !candidate.synthetic {
			if descriptor == nil {
				return candidate, nil
			}
			if _, match := a.remotes[normalizeGitURL(descriptor.URL)]; !match {
				return candidate, nil
			}
		} else if descriptor == nil {
			witnesses = slices.Clone(witnesses)
			slices.SortFunc(witnesses, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
			var err error
			candidate.subtree, err = a.bootstrapArtifactRoot(ctx, witnesses)
			if err != nil || candidate.subtree == "" {
				return candidate, err
			}
			alias, _ = behavior.BootstrapAlias(filepath.Join(bootstrap.LocalPath, candidate.subtree))
		}
	}
	if candidate.synthetic {
		candidate.object = alias
	}
	if id == manifest.BootstrapSourceID {
		candidate.artifact = bootstrap
		return candidate, nil
	}
	if candidate.pinned {
		var location *gitrepo.Location
		if input, ok := bootstrap.FS.(*gittree.FS); ok {
			location = input.Location()
		}
		artifacts, err := git.ResolveLocal(ctx, location, []*manifest.GitRepository{descriptor}, bootstrap.Revision)
		if err != nil {
			return candidate, err
		}
		artifact := artifacts[id]
		if artifact == nil {
			candidate.warnings = []manifest.Warning{{Resource: id, Category: manifest.WarnSourceRefUnavailable, Message: "declared source ref cannot be satisfied locally; using normal source fetch", Detail: []string{(*manifest.GitRepositoryRef)(descriptor.Reference).String(), "ref unavailable in this tree's repository or checkout options require fetching"}}}
			return candidate, nil
		}
		candidate.warnings = artifact.Warnings
		if artifact.LocalPath != cmp.Or(bootstrap.LocalRoot, bootstrap.LocalPath) {
			captured := *artifact
			captured.LocalRoot = bootstrap.LocalPath
			candidate.artifact = &captured
			return candidate, nil
		}
	}
	artifact, key, err := a.entrySourceView(ctx, candidate)
	candidate.artifact, candidate.viewKey = artifact, key
	return candidate, err
}

func (a *Admission) bootstrapArtifactRoot(ctx context.Context, witnesses []manifest.BaseManifest) (string, error) {
	bootstrap := a.input
	scan, err := filepath.Rel(bootstrap.LocalPath, a.path)
	if err != nil {
		return "", err
	}
	if !safepath.Contains(bootstrap.LocalPath, a.path) {
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
func (a *Admission) entrySourceView(ctx context.Context, candidate entryResolution) (*store.SourceArtifact, string, error) {
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
		Root, Subtree, Origin, Key, Content, Digest string
		Ignore                                      string
		Sparse                                      []string `json:",omitempty"`
		Recurse                                     bool
	}{repoRoot, subtree, bootstrap.LocalRoot, bootstrap.Key, bootstrap.Content, bootstrap.Digest, ignore, sparse, recurse}})
	var input fs.FS
	var identity string
	var err error
	cached := a.aliasViews[viewKey]
	if cached != nil {
		input, identity, err = cached.FS, cached.Content, ctx.Err()
	} else {
		current := bootstrap.FS
		if subtree != "." {
			current, err = gittree.Sub(current, subtree)
		}
		if err == nil {
			input, identity, err = gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: current, CurrentContent: bootstrap.Content, Content: struct{ Subtree string }{subtree}, Ignore: ignore, Sparse: sparse})
		}
	}
	if err != nil {
		return nil, "", err
	}
	id := candidate.object.Named()
	artifact := *bootstrap
	artifact.Kind, artifact.URL, artifact.FS = id.Kind, "file://"+filepath.Join(repoRoot, subtree), input
	artifact.ContentFingerprint = gittree.ContentFingerprint
	artifact.InputAlias, artifact.Key, artifact.Content = true, "", identity
	artifact.Warnings = nil
	artifact.LocalPath = label
	artifact.LocalRoot = filepath.Join(repoRoot, subtree)
	return &artifact, viewKey, nil
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
		artifact, _ := d.cfg.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		if id != manifest.BootstrapSourceID && artifact != nil && artifact.InputAlias {
			names = append(names, id.String())
		}
	}
	if len(names) <= 1 {
		return
	}
	slices.Sort(names)
	d.cfg.Store.AddWarning(manifest.Warning{Category: manifest.WarnBootstrapAliases,
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
