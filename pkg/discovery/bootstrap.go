package discovery

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

type entrySource interface {
	BootstrapAlias(string) (manifest.BaseManifest, *manifest.GitRepository)
}

type entryResolution struct {
	object    manifest.BaseManifest
	input     *store.SourceArtifact
	git       *manifest.GitRepository
	synthetic bool
	pinned    bool
	artifact  *store.SourceArtifact
	warnings  []manifest.Warning
	viewKey   string
}

// Admission supplies bytes and remote metadata; only publishEntrySource publishes identities.
func (d *Session) admitBootstrapInput(ctx context.Context) (*store.SourceArtifact, error) {
	admitted := d.cfg.Input
	if admitted == nil {
		admitted, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	if admitted == nil {
		var err error
		admitted, d.cfg.Path, err = baseline.AdmitDirectory(ctx, d.cfg.Path, d.cfg.RepoRoot, d.cfg.Work)
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
		for index := range admitted.Warnings {
			warning := admitted.Warnings[index]

			d.cfg.Store.AddWarning(warning)
		}
	}
	if admitted.FS == nil || admitted.LocalPath == "" {
		return nil, manifest.NewError(manifest.ErrInput, nil, "bootstrap source input is unavailable")
	}
	if d.cfg.Path == "" {
		d.cfg.Path = admitted.LocalPath
	} else {
		scanPath, err := filepath.Abs(d.cfg.Path)
		if err != nil {
			return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("resolve scan path: %w", err), "")
		}
		d.cfg.Path = scanPath
	}
	d.remotes = make(map[string]struct{}, len(urls))
	for _, raw := range urls {
		if normalized := normalizeGitURL(raw); normalized != "" {
			d.remotes[normalized] = struct{}{}
		}
	}
	d.cfg.Input = admitted
	return admitted, nil
}

type entrySourceGroup struct {
	scoped   bool
	refs     []manifest.NamedResource
	evidence store.Child
	required bool
}

func (d *Session) resolveEntrySources(ctx context.Context, allowMissing bool) (int, error) {
	bootstrap, err := d.resolveBootstrapInput(ctx)
	if err != nil {
		return 0, err
	}
	groups, err := d.entrySourceGroups(ctx, allowMissing)
	if err != nil {
		return 0, err
	}
	admission := d.admission()
	admission.input = bootstrap
	published := 0
	var sourceIDs [8]manifest.NamedResource
	ids := sourceIDs[:0]
	for id := range groups {
		group := groups[id]
		if group.required {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	for _, id := range ids {
		group := groups[id]
		added, err := d.resolveEntrySource(ctx, admission, id, &group)
		if err != nil {
			return 0, err
		}
		if added {
			published++
		}
	}
	return published, nil
}

func (d *Session) resolveBootstrapInput(ctx context.Context) (*store.SourceArtifact, error) {
	if d.resolvedSources != nil {
		if input, ok := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); ok {
			d.cfg.Input = input
		}
	}
	bootstrap := d.cfg.Input
	if d.resolvedSources == nil {
		admitted, err := d.admitBootstrapInput(ctx)
		if err != nil {
			return nil, err
		}
		bootstrap = admitted
		d.resolvedSources = map[manifest.NamedResource]entryResolution{}
	}
	if previous, ok := d.resolvedSources[manifest.BootstrapSourceID]; ok && previous.input != bootstrap {
		d.aliasViews = nil
	}
	return bootstrap, nil
}

func addEntrySource(groups map[manifest.NamedResource]entrySourceGroup, id, requester manifest.NamedResource) {
	group := groups[id]
	group.required = true
	if requester != (manifest.NamedResource{}) && !slices.Contains(group.refs, requester) {
		group.refs = append(group.refs, requester)
	}
	groups[id] = group
}

func (d *Session) entrySourceGroups(
	ctx context.Context,
	allowMissing bool,
) (map[manifest.NamedResource]entrySourceGroup, error) {
	groups := map[manifest.NamedResource]entrySourceGroup{manifest.BootstrapSourceID: {required: true}}
	for kind := range manifest.Kinds() {
		if kind.Group != manifest.SourceDomain {
			continue
		}
		for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
			if _, ok := obj.(entrySource); ok {
				addEntrySource(groups, obj.Named(), manifest.NamedResource{})
			}
		}
	}
	if err := d.addLoadedSources(ctx, groups, allowMissing); err != nil {
		return nil, err
	}
	if allowMissing {
		d.addMissingSources(groups)
	}
	return groups, nil
}

func (d *Session) addLoadedSources(
	ctx context.Context,
	groups map[manifest.NamedResource]entrySourceGroup,
	allowMissing bool,
) error {
	if d.loader == nil {
		return nil
	}
	restricted := d.loader != nil && d.loader.Options.Session != nil
	for child, err := range d.loader.Sources(ctx) {
		if err != nil {
			return err
		}
		id := child.Obj.Named()
		group := groups[id]
		group.evidence = child
		groups[id] = group
		if restricted {
			if selection.Root(d.loader.RootEvidence(id, nil)).Admitted {
				addEntrySource(groups, id, manifest.NamedResource{})
			}
		}
	}
	{
		childValues := d.loader.PathObjects()
		for childIndex := range childValues {
			child := childValues[childIndex]

			application, valid := manifest.ApplicationFor(d.loader.CatalogObject(child.Identity))
			if valid && application.Declaration != nil {
				continue
			}
			ref, _, _ := child.SourcePath()
			if allowMissing || groups[ref].evidence.Obj != nil || d.cfg.Store.GetObject(ref) != nil {
				addEntrySource(groups, ref, child.Identity)
			}
		}
	}
	return nil
}

func entrySourceReference(ref *manifest.Reference) bool {
	return ref.Group == manifest.SourceDomain && (ref.Use == "source" || ref.Use == "artifact")
}

func (d *Session) addMissingSources(groups map[manifest.NamedResource]entrySourceGroup) {
	for kind := range manifest.Kinds() {
		if !kind.Runnable {
			continue
		}
		for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				if entrySourceReference(&ref) {
					addEntrySource(groups, ref.NamedResource, obj.Named())
				}
			}
		}
	}
	d.addMissingIndexedSources(groups)
}

func (d *Session) addMissingIndexedSources(groups map[manifest.NamedResource]entrySourceGroup) {
	if d.loader == nil {
		return
	}
	for id := range d.loader.Index.Files() {
		if !id.KindInfo().Runnable || d.cfg.Store.GetObject(id) != nil {
			continue
		}
		if !selection.Root(d.loader.RootEvidence(id, nil)).Admitted {
			continue
		}
		for ref := range d.loader.References(id) {
			if entrySourceReference(&ref) {
				addEntrySource(groups, ref.NamedResource, manifest.NamedResource{})
			}
		}
	}
}

//nolint:ireturn // Sources retain their heterogeneous manifest types.
func (d *Session) entrySourceObject(
	id manifest.NamedResource,
	group *entrySourceGroup,
) manifest.BaseManifest {
	if group.scoped {
		if group.evidence.Obj != nil {
			d.cfg.Store.AddObject(group.evidence.Obj, group.evidence.Doc)
		}
		return group.evidence.Obj
	}
	restricted := d.loader != nil && d.loader.Options.Session != nil
	obj := d.cfg.Store.GetObject(id)
	if previous, seen := d.resolvedSources[id]; restricted && seen && previous.synthetic && obj == previous.object &&
		group.evidence.Obj != nil {
		obj = group.evidence.Obj
	}
	if obj != nil {
		return obj
	}
	obj = group.evidence.Obj
	independentlyAdmitted := false
	if restricted && obj != nil {
		independentlyAdmitted = selection.Root(d.loader.RootEvidence(id, nil)).Admitted
	}
	if obj != nil &&
		(independentlyAdmitted || slices.ContainsFunc(group.refs, func(requester manifest.NamedResource) bool { return d.cfg.Store.GetObject(requester) != nil })) {
		d.cfg.Store.AddObject(obj, group.evidence.Doc)
	}
	return obj
}

func (d *Session) resolveEntrySource(
	ctx context.Context,
	admission *Admission,
	id manifest.NamedResource,
	group *entrySourceGroup,
) (bool, error) {
	bootstrap := admission.input
	obj := d.entrySourceObject(id, group)
	previous, seen := d.resolvedSources[id]
	if seen && obj != nil && previous.object == obj && previous.input != bootstrap && previous.artifact != nil &&
		(previous.synthetic || previous.artifact.InputAlias) {
		if err := d.refreshEntrySource(ctx, admission, id, &previous); err != nil {
			return false, err
		}
		return id != manifest.BootstrapSourceID, nil
	}
	if seen && previous.input == bootstrap && previous.object == obj {
		if previous.artifact != nil && obj != nil && d.cfg.Store.GetArtifact(id, store.ArtifactContent) == nil {
			d.publishEntrySource(&previous, previous.artifact)
			return id != manifest.BootstrapSourceID, nil
		}
		return false, nil
	}
	candidate, err := admission.resolve(ctx, id, obj, d.cfg.Work)
	if err != nil {
		return false, err
	}
	d.resolvedSources[id] = candidate
	for i := range candidate.warnings {
		d.cfg.Store.AddWarning(candidate.warnings[i])
	}
	if candidate.artifact == nil {
		return false, nil
	}
	d.publishResolvedSource(&candidate)
	return id != manifest.BootstrapSourceID, nil
}

func (d *Session) refreshEntrySource(
	ctx context.Context,
	admission *Admission,
	id manifest.NamedResource,
	previous *entryResolution,
) error {
	previous.input = admission.input
	if id == manifest.BootstrapSourceID {
		previous.artifact = admission.input
		d.publishEntrySource(previous, admission.input)
		return nil
	}
	artifact, key, err := admission.entrySourceView(ctx, previous)
	if err != nil {
		return err
	}
	previous.artifact, previous.viewKey = artifact, key
	d.cfg.Store.SetArtifact(id, artifact)
	d.cfg.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Message: "local entry source"})
	d.resolvedSources[id] = *previous
	return nil
}

func (d *Session) publishResolvedSource(candidate *entryResolution) {
	if candidate.pinned && !candidate.artifact.InputAlias {
		d.hasPins = true
	}
	d.publishEntrySource(candidate, candidate.artifact)
	if candidate.viewKey == "" {
		return
	}
	if d.aliasViews == nil {
		d.aliasViews = map[string]*store.SourceArtifact{}
	}
	if d.aliasViews[candidate.viewKey] == nil {
		d.aliasViews[candidate.viewKey] = candidate.artifact
	}
}

// Admission retains immutable bootstrap policy for early and runtime source decisions.
type Admission struct {
	entryPreparation kustomize.Preparation
	entryRequest     kustomize.RenderRequest
	requestedEntry   manifest.EntryApplication
	entryEvidence    *kustomize.EntryEvidence
	applications     atomic.Pointer[applicationEvidence]
	entryBound       atomic.Bool
	sources          map[manifest.NamedResource]entryResolution
	input            *store.SourceArtifact
	path             string
	remotes          map[string]struct{}
	aliasViews       map[string]*store.SourceArtifact
	entryDemand      bool
	qualification    *entryQualification
	store            *store.Store
	acceptedEntry    *selection.EntryRoute
	assessment       *store.ReadSet
	loader           *loader.Loader
	trees            *kustomize.TreeCache
	session          *Session
	substitutions    values.DependencySubstitutions
}

func (d *Session) admission() *Admission {
	if d.entryAdmission != nil {
		d.entryAdmission.sources = maps.Clone(d.resolvedSources)
		d.entryAdmission.substitutions = d.substitutions
		d.entryAdmission.entryDemand = d.requiresEntry()
		if d.entryAdmission.entryDemand {
			d.entryAdmission.session = d
		}
		if q := d.entryAdmission.qualification; q != nil && !d.entryAdmission.entryBound.Load() {
			q.closed = !d.entryAdmission.entryDemand
			if d.entryAdmission.entryDemand {
				q.session = d
			}
		}
		return d.entryAdmission
	}
	input, _ := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if input == nil {
		input = d.cfg.Input
	}
	admission := &Admission{
		input:         input,
		path:          d.cfg.Path,
		remotes:       maps.Clone(d.remotes),
		aliasViews:    maps.Clone(d.aliasViews),
		sources:       maps.Clone(d.resolvedSources),
		store:         d.cfg.Store,
		loader:        d.loader,
		trees:         d.cfg.Trees,
		session:       d,
		substitutions: d.substitutions,
	}
	admission.requestedEntry = manifest.EntryApplication{
		Source: manifest.BootstrapSourceID, Path: admission.entryPath(),
	}
	d.entryState = entryQualification{loader: d.loader, trees: d.cfg.Trees, session: d}
	admission.entryDemand = d.requiresEntry()
	d.entryState.closed = !admission.entryDemand
	admission.qualification = &d.entryState
	d.entryAdmission = admission
	admission.publishEntry(nil, manifest.NamedResource{})
	return admission
}

// Resolve admits a known source or a missing Git bootstrap reference.
// A nil artifact leaves normal acquisition responsible for the source.
func (a *Admission) Resolve(
	ctx context.Context,
	id manifest.NamedResource,
	object manifest.BaseManifest,
	work manifest.WorkScope,
) (
	store.Child,
	*store.SourceArtifact,
	error,
) {
	if a == nil {
		return store.Child{}, nil, nil
	}
	resolved, err := a.resolve(ctx, id, object, work)
	if resolved.artifact != nil && len(resolved.warnings) != 0 {
		captured := *resolved.artifact
		captured.Warnings = append(slices.Clone(captured.Warnings), resolved.warnings...)
		resolved.artifact = &captured
	}
	return store.Child{Obj: resolved.object}, resolved.artifact, err
}

func (a *Admission) resolve(
	ctx context.Context,
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	work manifest.WorkScope,
) (entryResolution, error) {
	bootstrap := a.input
	candidate := entryResolution{object: obj, input: bootstrap, synthetic: obj == nil}
	if obj == nil {
		obj, _ = id.BindLocal(bootstrap.LocalPath)
	}
	behavior, ok := obj.(entrySource)
	if !ok {
		return candidate, nil
	}
	alias, descriptor := behavior.BootstrapAlias(bootstrap.LocalPath)
	candidate.git = descriptor
	candidate.pinned = !candidate.synthetic && id != manifest.BootstrapSourceID &&
		descriptor.BootstrapPinned()
	if id != manifest.BootstrapSourceID && !candidate.synthetic {
		if _, match := a.remotes[normalizeGitURL(descriptor.URL)]; !match {
			return candidate, nil
		}
	}
	if candidate.synthetic {
		candidate.object = alias
	}
	if id == manifest.BootstrapSourceID {
		candidate.artifact = bootstrap
		return candidate, nil
	}
	var location *gitrepo.Location
	if candidate.pinned {
		if input, ok := bootstrap.FS.(*gittree.FS); ok {
			location = input.Location()
		}
	}
	artifacts, err := git.ResolveLocal(
		ctx,
		location,
		[]*manifest.GitRepository{descriptor},
		git.LocalOptions{Revision: bootstrap.Revision, Work: work},
	)
	if err != nil {
		return candidate, err
	}
	if candidate.pinned {
		artifact := artifacts[id]
		if artifact == nil {
			candidate.warnings = []manifest.Warning{
				{
					Resource: id,
					Category: manifest.WarnSourceRefUnavailable,
					Message:  "declared source ref cannot be satisfied locally; using normal source fetch",
					Detail: []string{
						(*manifest.GitRepositoryRef)(descriptor.Reference).String(),
						"ref unavailable in this tree's repository or checkout options require fetching",
					},
				},
			}
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
	artifact, key, err := a.entrySourceView(ctx, &candidate)
	candidate.artifact, candidate.viewKey = artifact, key
	return candidate, err
}

// Mutable aliases keep empty keys and source-qualified mounts; their input is immutable.
func (a *Admission) entrySourceView(
	ctx context.Context,
	candidate *entryResolution,
) (*store.SourceArtifact, string, error) {
	bootstrap := candidate.input
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
	label, viewKey := gittree.Label(
		gittree.LabelOptions{Root: repoRoot, Mode: gittree.Source, Source: candidate.object.Named(), Content: struct {
			Root, Origin, Key, Content, Digest string
			Ignore                             string
			Sparse                             []string `json:",omitempty"`
			Recurse                            bool
		}{repoRoot, bootstrap.LocalRoot, bootstrap.Key, bootstrap.Content, bootstrap.Digest, ignore, sparse, recurse}},
	)
	var input fs.FS
	var identity string
	var err error
	cached := a.aliasViews[viewKey]
	if cached != nil {
		input, identity, err = cached.FS, cached.Content, ctx.Err()
	} else {
		input, identity, err = gittree.New(
			ctx,
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{
				Mode:           gittree.Source,
				Current:        bootstrap.FS,
				CurrentContent: bootstrap.Content,
				Ignore:         ignore,
				Sparse:         sparse,
			},
		)
	}
	if err != nil {
		return nil, "", err
	}
	id := candidate.object.Named()
	artifact := *bootstrap
	artifact.Kind, artifact.URL, artifact.FS = id.Kind, "file://"+repoRoot, input
	artifact.ContentFingerprint = gittree.ContentFingerprint
	artifact.InputAlias, artifact.Key, artifact.Content = true, "", identity
	artifact.Warnings = nil
	artifact.LocalPath = label
	artifact.LocalRoot = repoRoot
	return &artifact, viewKey, nil
}

func (d *Session) publishEntrySource(candidate *entryResolution, artifact *store.SourceArtifact) {
	id := candidate.object.Named()
	if candidate.synthetic ||
		d.loader != nil && d.loader.Options.Session != nil && d.cfg.Store.GetObject(id) != nil &&
			d.cfg.Store.GetObject(id) != candidate.object {
		d.cfg.Store.AddObject(candidate.object)
	}

	d.cfg.Store.SetArtifact(id, artifact)
	d.cfg.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "local entry source"})
	d.resolvedSources[id] = *candidate
}

// Only working-input aliases contribute; canonical and independently committed artifacts do not.
func (d *Session) warnBootstrapAliases(repoRoot string) {
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
	d.cfg.Store.AddWarning(manifest.Warning{
		Category: manifest.WarnBootstrapAliases,
		Message:  "aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
		Detail:   append(names, repoRoot),
	})
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
