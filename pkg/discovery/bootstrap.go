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
	BootstrapAlias(string) manifest.BaseManifest
	BootstrapFacts() manifest.BootstrapFacts
	BootstrapInput() any
}

type entryResolution struct {
	object    manifest.BaseManifest
	input     *store.SourceArtifact
	facts     manifest.BootstrapFacts
	subtree   string
	synthetic bool
	pinned    bool
	refs      []manifest.BaseManifest
	artifact  *store.SourceArtifact
	warnings  []manifest.Warning
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
	defer func() { d.aliasViews = admission.aliasViews.Load() }()
	admission.input = bootstrap
	published := 0
	for _, id := range slices.SortedFunc(maps.Keys(groups), manifest.NamedResource.Compare) {
		previous, seen := d.resolvedSources[id]
		obj := d.cfg.Store.GetObject(id)
		if child := evidence[id]; seen && previous.synthetic && obj == previous.object && child.Obj != nil {
			obj = child.Obj
			d.cfg.Store.DeleteObject(id)
			d.cfg.Store.AddObject(obj, child.Doc)
		}
		if obj == nil {
			obj = evidence[id].Obj
			if obj != nil && slices.ContainsFunc(groups[id], func(witness manifest.BaseManifest) bool { return d.cfg.Store.GetObject(witness.Named()) != nil }) {
				d.cfg.Store.AddObject(obj, evidence[id].Doc)
			}
		}
		if seen && obj != nil && previous.object == obj && previous.input != bootstrap && previous.artifact != nil && (previous.synthetic || previous.artifact.InputAlias) {
			previous.input = bootstrap
			if id == manifest.BootstrapSourceID {
				previous.artifact = bootstrap
				d.publishEntrySource(previous, bootstrap)
				continue
			}
			artifact, err := admission.entrySourceView(ctx, previous)
			if err != nil {
				return 0, err
			}
			previous.artifact = artifact
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
		candidate, err := admission.resolve(ctx, nil, ResolveRequest{ID: id, Object: obj, Witnesses: groups[id]})
		if err != nil {
			return 0, err
		}
		d.resolvedSources[id] = candidate
		for _, warning := range candidate.warnings {
			d.cfg.Store.AddWarning(warning)
		}
		if candidate.artifact == nil {
			if seen && previous.artifact != nil {
				d.cfg.Store.SetArtifact(id, nil)
			}
			continue
		}
		if candidate.pinned && !candidate.artifact.InputAlias {
			d.hasPins = true
		}
		d.publishEntrySource(candidate, candidate.artifact)
		if id != manifest.BootstrapSourceID {
			published++
		}
	}
	return published, nil
}

type aliasView struct {
	material   *store.SourceArtifact
	identities map[string]*store.SourceArtifact
}

// Admission retains immutable bootstrap policy for early and runtime source decisions.
type Admission struct {
	store        *store.Store
	applications atomic.Pointer[applicationEvidence]
	entryBound   atomic.Bool
	selected     *applicationEvidence
	declarations []store.Child
	sources      map[manifest.NamedResource]entryResolution
	input        *store.SourceArtifact
	path         string
	remotes      map[string]struct{}
	aliasViews   atomic.Pointer[map[string]aliasView]
}

func (d *discoverer) admission() *Admission {
	input, _ := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if input == nil {
		input = d.cfg.Input
	}
	admission := &Admission{store: d.cfg.Store, input: input, path: d.cfg.Path, remotes: maps.Clone(d.remotes), sources: maps.Clone(d.resolvedSources), selected: d.selection}
	if d.loader != nil {
		admission.declarations = d.applications()
	}
	admission.aliasViews.Store(d.aliasViews)
	return admission
}

// ResolveRequest identifies a source and the declarations requesting its content.
type ResolveRequest struct {
	ID        manifest.NamedResource
	Object    manifest.BaseManifest
	Witnesses []manifest.BaseManifest
}

// Resolve admits a source while retaining its current application authority in reads.
func (a *Admission) Resolve(ctx context.Context, reads *store.ReadSet, request ResolveRequest) (manifest.BaseManifest, *store.SourceArtifact, error) {
	if a == nil {
		return nil, nil, nil
	}
	resolved, err := a.resolve(ctx, reads, request)
	if resolved.artifact != nil && len(resolved.warnings) != 0 {
		captured := *resolved.artifact
		captured.Warnings = append(slices.Clone(captured.Warnings), resolved.warnings...)
		resolved.artifact = &captured
	}
	return resolved.object, resolved.artifact, err
}

func (a *Admission) resolve(ctx context.Context, reads *store.ReadSet, request ResolveRequest) (entryResolution, error) {
	id, obj, witnesses := request.ID, request.Object, request.Witnesses
	bootstrap := a.input
	if reads != nil {
		if declaration := reads.GetDeclaration(id); declaration != nil {
			obj = declaration
		}
	}
	if current, ok := obj.(entrySource); ok && id != manifest.BootstrapSourceID {
		if local, err := id.BindLocal(bootstrap.LocalPath); err == nil && manifest.ValueEqual(current.BootstrapInput(), local.(entrySource).BootstrapInput()) {
			obj = nil
		}
	}
	if previous, ok := a.sources[id]; ok && previous.synthetic && obj != nil {
		current, currentSource := obj.(entrySource)
		original, originalSource := previous.object.(entrySource)
		if currentSource && originalSource && manifest.ValueEqual(current.BootstrapInput(), original.BootstrapInput()) {
			obj = nil
			if len(witnesses) == 0 {
				witnesses = previous.refs
			}
		}
	}
	candidate := entryResolution{object: obj, input: bootstrap, subtree: ".", synthetic: obj == nil, refs: slices.Clone(witnesses)}
	if obj == nil {
		obj, _ = id.BindLocal(bootstrap.LocalPath)
	}
	behavior, ok := obj.(entrySource)
	if !ok {
		return candidate, nil
	}
	candidate.facts = behavior.BootstrapFacts()
	alias := behavior.BootstrapAlias(bootstrap.LocalPath)
	selfEntry := false
	if id != manifest.BootstrapSourceID {
		if candidate.synthetic && candidate.facts.DerivedRoot {
			witnesses = slices.Clone(witnesses)
			slices.SortFunc(witnesses, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
			var err error
			candidate.subtree, err = a.bootstrapArtifactRoot(ctx, witnesses)
			if err != nil || candidate.subtree == "" {
				return candidate, err
			}
			alias = behavior.BootstrapAlias(filepath.Join(bootstrap.LocalPath, candidate.subtree))
		}
		if candidate.facts.RefKind == "" || candidate.facts.RefKind == "branch" {
			applications, _ := a.entryApplications(a.store, witnesses)
			for _, evidence := range applications {
				application := evidence.application
				root := evidence.object
				facts, subtree := candidate.facts, candidate.subtree
				var applicationSource manifest.BaseManifest
				if application.Source != id {
					resolved := a.applicationSource(a.store, application)
					applicationSource = resolved.object
					if resolved.object == nil {
						resolved.object, _ = application.Source.BindLocal(bootstrap.LocalPath)
					}
					source, ok := resolved.object.(entrySource)
					if !ok {
						continue
					}
					facts = source.BootstrapFacts()
					subtree = cmp.Or(a.sources[application.Source].subtree, ".")
				}
				if !a.appliesEntry(application, facts, subtree) {
					continue
				}
				if reads != nil {
					root = reads.GetDeclaration(evidence.object.Named())
					if !manifest.Equal(root, evidence.object) {
						continue
					}
					if application.Source != id && !manifest.Equal(reads.GetDeclaration(application.Source), applicationSource) {
						continue
					}
					if owner := a.store.EmissionOwner(evidence.object.Named()); owner != (manifest.NamedResource{}) {
						_, _, output := reads.GetEmission(owner)
						output.Finish()
						if !output.Complete || !output.OutputReads.Valid() || len(output.Failures) != 0 || !slices.ContainsFunc(output.Children, func(child store.Child) bool {
							return child.Obj != nil && manifest.Equal(child.Obj, root)
						}) {
							continue
						}
					} else {
						_, _, output := reads.GetEmission(evidence.object.Named())
						output.Finish()
						reads.GetArtifact(evidence.object.Named(), store.ArtifactContent)
					}
				}
				selfEntry = application.Source == id
				break
			}
		}
		if !candidate.synthetic && !selfEntry {
			if _, match := a.remotes[normalizeGitURL(candidate.facts.URL)]; candidate.facts.DerivedRoot || !match {
				return candidate, nil
			}
		}
	}
	if candidate.synthetic {
		candidate.object = alias
	}
	if id == manifest.BootstrapSourceID {
		candidate.artifact = bootstrap
		return candidate, nil
	}
	candidate.pinned = !candidate.synthetic && !selfEntry && candidate.facts.Ref != ""
	if candidate.pinned {
		var location *gitrepo.Location
		if input, ok := bootstrap.FS.(*gittree.FS); ok {
			location = input.Location()
		}
		artifacts, err := git.ResolveLocal(ctx, location, []manifest.BaseManifest{obj}, bootstrap.Revision)
		if err != nil {
			return candidate, err
		}
		artifact := artifacts[id]
		if artifact == nil {
			candidate.warnings = []manifest.Warning{{Resource: id, Category: manifest.WarnSourceRefUnavailable, Message: "declared source ref cannot be satisfied locally; using normal source fetch", Detail: []string{candidate.facts.Ref, "ref unavailable in this tree's repository or checkout options require fetching"}}}
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
	artifact, err := a.entrySourceView(ctx, candidate)
	candidate.artifact = artifact
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
func (a *Admission) entrySourceView(ctx context.Context, candidate entryResolution) (*store.SourceArtifact, error) {
	bootstrap, subtree := candidate.input, candidate.subtree
	repoRoot := bootstrap.LocalPath
	ignore, sparse, recurse := candidate.facts.Ignore, candidate.facts.Sparse, candidate.facts.Recurse
	label, viewKey := gittree.Label(gittree.LabelOptions{Root: repoRoot, Mode: gittree.Source, Source: candidate.object.Named(), Content: struct {
		Root, Subtree, Origin, Key, Content, Digest string
		Ignore                                      string
		Sparse                                      []string `json:",omitempty"`
		Recurse                                     bool
	}{repoRoot, subtree, bootstrap.LocalRoot, bootstrap.Key, bootstrap.Content, bootstrap.Digest, ignore, sparse, recurse}})
	var input fs.FS
	var identity string
	var err error
	var material *store.SourceArtifact
	if views := a.aliasViews.Load(); views != nil {
		view := (*views)[viewKey]
		if cached := view.identities[label]; cached != nil {
			if cached.Revision != bootstrap.Revision || !maps.Equal(cached.Metadata, bootstrap.Metadata) {
				current := cached.WithReads(nil)
				current.Revision, current.Metadata = bootstrap.Revision, bootstrap.Metadata
				return current, ctx.Err()
			}
			return cached, ctx.Err()
		}
		material = view.material
	}
	if material != nil {
		input, identity, err = material.FS, material.Content, ctx.Err()
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
		return nil, err
	}
	id := candidate.object.Named()
	artifact := *bootstrap
	artifact.Kind, artifact.URL, artifact.FS = id.Kind, "file://"+filepath.Join(repoRoot, subtree), input
	artifact.ContentFingerprint = gittree.ContentFingerprint
	artifact.InputAlias, artifact.Key, artifact.Content = true, "", identity
	artifact.Warnings = nil
	artifact.LocalPath = label
	artifact.LocalRoot = filepath.Join(repoRoot, subtree)
	for {
		previous := a.aliasViews.Load()
		views := map[string]aliasView{}
		view := aliasView{material: &artifact}
		if previous != nil {
			view = (*previous)[viewKey]
			if cached := view.identities[label]; cached != nil {
				return cached, nil
			}
			maps.Copy(views, *previous)
		}
		if view.material == nil {
			view.material = &artifact
		}
		identities := make(map[string]*store.SourceArtifact, len(view.identities)+1)
		maps.Copy(identities, view.identities)
		identities[label] = &artifact
		view.identities = identities
		views[viewKey] = view
		if a.aliasViews.CompareAndSwap(previous, &views) {
			return &artifact, nil
		}
	}
}

func (d *discoverer) publishEntrySource(candidate entryResolution, artifact *store.SourceArtifact) {
	id := candidate.object.Named()
	if candidate.synthetic {
		d.cfg.Store.AddObject(candidate.object)
		if current := d.cfg.Store.GetObject(id); current != nil {
			candidate.object = current
		}
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
