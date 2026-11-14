package discovery

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

type entrySource interface {
	BootstrapAlias(string) (manifest.BaseManifest, *manifest.GitRepository)
}

type entryResolution struct {
	producers []occurrenceKey
	object    manifest.BaseManifest
	input     *store.SourceArtifact
	git       *manifest.GitRepository
	synthetic bool
	entry     bool
	projected bool
	pinned    bool
	artifact  *store.SourceArtifact
	warnings  []manifest.Warning
	viewKey   string
}

func (r *entryResolution) merge(next *entryResolution) {
	producers := slices.Clone(r.producers)
	for i := range next.producers {
		if !slices.Contains(producers, next.producers[i]) {
			producers = append(producers, next.producers[i])
		}
	}
	replace := r.object == nil && len(r.producers) == 0
	if next.object != nil && !next.synthetic {
		replace = r.object == nil || r.synthetic || r.projected
	}
	if replace {
		*r = *next
	}
	r.producers = producers
}

func (r *entryResolution) reusesSynthetic(candidate *entryResolution) bool {
	return r.synthetic && candidate.synthetic && r.input == candidate.input && r.artifact != nil
}

func (r *entryResolution) ownsDeclaration(object manifest.BaseManifest) bool {
	return r.synthetic && object == r.object
}

type entrySourcePublisher struct {
	store   *store.Store
	root    string
	mutable map[manifest.NamedResource]entryResolution
	current atomic.Pointer[map[manifest.NamedResource]entryResolution]
}

func (p *entrySourcePublisher) snapshot() map[manifest.NamedResource]entryResolution {
	if p == nil {
		return nil
	}
	if current := p.current.Load(); current != nil {
		return *current
	}
	return p.mutable
}

func (p *entrySourcePublisher) record(id manifest.NamedResource, resolution *entryResolution) {
	current := p.current.Load()
	if current == nil {
		p.mutable[id] = *resolution
		return
	}
	for {
		next := maps.Clone(*current)
		next[id] = *resolution
		if p.current.CompareAndSwap(current, &next) {
			return
		}
		current = p.current.Load()
	}
}

// Freeze MUST follow the last construction write and precede runtime admission.
func (p *entrySourcePublisher) freeze() {
	if p.current.Load() != nil {
		return
	}
	current := p.mutable
	p.current.Store(&current)
	p.mutable = nil
}

func (p *entrySourcePublisher) publish(candidate *entryResolution, addObject bool) {
	id := candidate.object.Named()
	current := p.current.Load()
	for current != nil {
		previous := (*current)[id]
		if previous.reusesSynthetic(candidate) {
			*candidate = previous
			break
		}
		next := maps.Clone(*current)
		next[id] = *candidate
		if p.current.CompareAndSwap(current, &next) {
			break
		}
		current = p.current.Load()
	}
	if current == nil {
		p.record(id, candidate)
	}
	if addObject {
		p.store.AddObject(candidate.object)
	}
}

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
	pending := false
	for _, id := range ids {
		group := groups[id]
		added, err := d.resolveEntrySource(ctx, admission, id, group)
		if added {
			published++
		}
		if errors.Is(err, errPendingSources) {
			pending = true
			continue
		}
		if err != nil {
			return published, err
		}
	}
	if pending {
		return published, errPendingSources
	}

	return published, nil
}

func (d *Session) resolveBootstrapInput(ctx context.Context) (*store.SourceArtifact, error) {
	if d.resolvedSources != nil {
		if input, ok := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); ok &&
			!input.SameMaterial(d.cfg.Input) {
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
		d.resolvedSources = &entrySourcePublisher{
			store: d.cfg.Store, root: admitted.LocalPath,
			mutable: map[manifest.NamedResource]entryResolution{},
		}
	}
	if d.cfg.Entry != nil {
		// Generic aliases MUST be admitted from this store's declarations.
		for id, resolution := range d.cfg.Entry.prepared.sources {
			if !resolution.synthetic && (resolution.entry || resolution.projected || len(resolution.producers) != 0) {
				d.resolvedSources.mutable[id] = resolution
			}
		}
		d.cfg.Entry = nil
	}
	if previous, ok := d.resolvedSources.snapshot()[manifest.BootstrapSourceID]; ok && previous.input != bootstrap {
		d.aliasViews = nil
		if d.entryAdmission != nil {
			d.entryAdmission.aliasViews = nil
		}
	}
	d.resolvedSources.root = bootstrap.LocalPath
	return bootstrap, nil
}

//nolint:ireturn // Sources retain their heterogeneous manifest types.
func (d *Session) entrySourceObject(
	id manifest.NamedResource,
	group *entrySourceGroup,
) manifest.BaseManifest {
	// Projections MUST remain classification evidence until source publication accepts them.
	if group.projected {
		return group.object
	}
	if group.scoped {
		if group.object != nil {
			d.cfg.Store.AddObject(group.object, group.document)
			return d.cfg.Store.GetObject(id)
		} else if previous := d.resolvedSources.snapshot()[id]; !previous.ownsDeclaration(d.cfg.Store.GetObject(id)) {
			d.cfg.Store.DeleteObject(id)
		}
		return group.object
	}
	restricted := d.loader != nil && !d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{})
	obj := d.cfg.Store.GetObject(id)
	if previous, seen := d.resolvedSources.snapshot()[id]; seen && previous.synthetic &&
		obj == previous.object &&
		group.object != nil {
		obj = group.object
		d.cfg.Store.AddObject(obj, group.document)
	}
	if obj != nil {
		return obj
	}
	obj = group.object
	independentlyAdmitted := false
	if restricted && obj != nil {
		independentlyAdmitted = selection.Root(d.loader.RootEvidence(id, nil, true)).Admitted
	}
	if obj != nil &&
		(independentlyAdmitted || slices.ContainsFunc(group.refs, func(requester manifest.NamedResource) bool { return d.cfg.Store.GetObject(requester) != nil })) {
		d.cfg.Store.AddObject(obj, group.document)
	}
	return obj
}

func (d *Session) resolveEntrySource(
	ctx context.Context,
	admission *Admission,
	id manifest.NamedResource,
	group *entrySourceGroup,
) (bool, error) {
	obj := d.entrySourceObject(id, group)
	candidate, err := admission.resolve(
		ctx,
		id,
		&sourceRequest{
			object:    obj,
			reference: group.reference,
			work:      d.cfg.Work,
			scoped:    group.scoped,
			producers: group.producers,
			entry:     group.entry,
			projected: group.projected,
		},
	)
	if err != nil {
		return false, err
	}
	for i := range candidate.warnings {
		d.cfg.Store.AddWarning(candidate.warnings[i])
	}
	if candidate.artifact == nil {
		if previous := d.resolvedSources.snapshot()[id]; previous.synthetic && !candidate.synthetic &&
			!previous.ownsDeclaration(candidate.object) &&
			d.cfg.Store.GetDeclaration(id) == previous.object {
			current, _ := d.cfg.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			if !current.SameMaterial(previous.artifact) {
				return false, nil
			}
			d.cfg.Store.DeleteObject(id)
		}
		d.resolvedSources.record(id, &candidate)
		return false, nil
	}
	published := d.publishResolvedSource(&candidate)
	return published, d.requireSource(id)
}

func (d *Session) publishResolvedSource(candidate *entryResolution) bool {
	id := candidate.object.Named()
	current, _ := d.cfg.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	sameMaterial := current.SameMaterial(candidate.artifact) && current.Reads.Valid()
	if facts, ok := candidate.object.(interface {
		Composition() manifest.ArtifactComposition
	}); ok && len(facts.Composition().Includes) != 0 {
		sameMaterial = d.sourceMaterial(id, candidate.artifact) != nil
	}
	previous := d.resolvedSources.snapshot()[id]
	refresh := previous.artifact != nil && previous.input != candidate.input
	object := d.cfg.Store.GetObject(id)
	restricted := d.loader != nil && !d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{})
	replacing := restricted && object != nil && object != candidate.object
	projecting := d.finalized && candidate.projected && d.cfg.Store.GetDeclaration(id) == nil
	d.resolvedSources.publish(candidate, candidate.synthetic || replacing || projecting)
	if candidate.viewKey != "" {
		if d.entryAdmission.aliasViews == nil {
			d.entryAdmission.aliasViews = map[string]*store.SourceArtifact{}
		}
		d.entryAdmission.aliasViews[candidate.viewKey] = candidate.artifact
	}
	published := !sameMaterial && id != manifest.BootstrapSourceID
	if sameMaterial || refresh {
		return published
	}
	if candidate.pinned && !candidate.artifact.InputAlias {
		d.hasPins = true
	}
	if candidate.viewKey == "" {
		return published
	}
	if d.aliasViews == nil {
		d.aliasViews = map[string]*store.SourceArtifact{}
	}
	if d.aliasViews[candidate.viewKey] == nil {
		d.aliasViews[candidate.viewKey] = candidate.artifact
	}
	return published
}

// Admission retains immutable bootstrap policy for early and runtime source decisions.
type Admission struct {
	entryPreparation kustomize.Preparation
	entryRequest     kustomize.RenderRequest
	requestedEntry   manifest.EntryApplication
	entryEvidence    *kustomize.EntryEvidence
	applications     atomic.Pointer[applicationEvidence]
	entryBound       atomic.Bool
	resolvedSources  *entrySourcePublisher
	input            *store.SourceArtifact
	path             string
	remotes          map[string]struct{}
	// Construction MUST finish all view writes before runtime admission.
	aliasViews    map[string]*store.SourceArtifact
	entryDemand   bool
	qualification *entryQualification
	store         *store.Store
	acceptedEntry *selection.EntryRoute
	assessment    *store.ReadSet
	loader        *loader.Loader
	trees         *kustomize.TreeCache
	session       *Session
}

func (d *Session) admission() *Admission {
	if d.entryAdmission != nil {
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
	input := d.cfg.Input
	if input == nil {
		input, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	admission := &Admission{
		input:           input,
		path:            d.cfg.Path,
		remotes:         maps.Clone(d.remotes),
		aliasViews:      maps.Clone(d.aliasViews),
		resolvedSources: d.resolvedSources,
		store:           d.cfg.Store,
		loader:          d.loader,
		trees:           d.cfg.Trees,
		session:         d,
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

// Resolve preserves the accepted classification for a known source.
// A nil artifact leaves normal acquisition responsible for the source.
func (a *Admission) Resolve(
	ctx context.Context,
	id manifest.NamedResource,
	object manifest.BaseManifest,
	reads *store.ReadSet,
) (
	store.Child,
	*store.SourceArtifact,
	error,
) {
	if a == nil {
		return store.Child{}, nil, nil
	}
	var work manifest.WorkScope
	if reads != nil {
		work = reads.WorkScope()
	}
	resolved, err := a.resolve(ctx, id, &sourceRequest{object: object, reads: reads, work: work})
	if resolved.artifact != nil {
		if reads != nil && id != manifest.BootstrapSourceID {
			reads.GetArtifact(
				manifest.BootstrapSourceID,
				store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
			)
		}
		current, _ := a.store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		if current.SameMaterial(resolved.artifact) && current.Reads.Valid() {
			resolved.artifact = current
		}
	}
	if resolved.artifact != nil && len(resolved.warnings) != 0 {
		captured := resolved.artifact.WithReads(resolved.artifact.Reads)
		captured.Warnings = append(slices.Clone(captured.Warnings), resolved.warnings...)
		resolved.artifact = captured
	}
	return store.Child{Obj: resolved.object}, resolved.artifact, err
}

// Publish MUST complete acceptance before admitting a source declaration.
func (a *Admission) Publish(
	ctx context.Context,
	id manifest.NamedResource,
	reads *store.ReadSet,
	accept func(store.Child) (bool, error),
) (bool, error) {
	if a == nil || reads == nil || !reads.Valid() {
		return false, nil
	}
	reference := sourceReference(reads.GetDeclaration(reads.ID()), id)
	resolved, err := a.resolve(ctx, id, &sourceRequest{reference: reference, reads: reads, work: reads.WorkScope()})
	if err != nil || resolved.object == nil {
		return false, nil
	}
	if resolved.artifact == nil && !reference.RequiresDeclaration {
		return false, nil
	}
	accepted, err := accept(store.Child{Obj: resolved.object})
	if err != nil || !accepted {
		return false, err
	}
	if !reads.Valid() {
		previous := a.resolvedSources.snapshot()[id]
		if !previous.synthetic || previous.input != a.input || previous.artifact == nil ||
			a.store.GetDeclaration(id) != previous.object {
			return false, nil
		}
		var current store.ReadSet
		current.Init(a.store, reads.ID(), nil)
		defer current.Retire()
		resolved, err = a.resolve(ctx, id, &sourceRequest{reads: &current, work: reads.WorkScope()})
		if err != nil || !current.Valid() || resolved.artifact != previous.artifact {
			return false, err
		}
	}
	a.resolvedSources.publish(&resolved, true)
	return true, nil
}

func (a *Admission) resolve(
	ctx context.Context,
	id manifest.NamedResource,
	request *sourceRequest,
) (entryResolution, error) {
	candidate := a.classifySource(id, request)
	if candidate.artifact != nil || !candidate.synthetic && !candidate.entry && candidate.git == nil {
		return candidate, nil
	}
	bootstrap := candidate.input
	if candidate.viewKey != "" {
		artifact, key, err := a.entrySourceView(ctx, &candidate)
		candidate.artifact, candidate.viewKey = artifact, key
		return candidate, err
	}
	if candidate.synthetic && candidate.object == nil {
		candidate.object, _ = id.BindLocal(bootstrap.LocalPath)
		if behavior, ok := candidate.object.(entrySource); ok {
			_, candidate.git = behavior.BootstrapAlias(bootstrap.LocalPath)
		}
	}
	descriptor := candidate.git
	if descriptor == nil || candidate.projected {
		candidate.pinned = false
		artifact, key, err := a.entrySourceView(ctx, &candidate)
		candidate.artifact, candidate.viewKey = artifact, key
		return candidate, err
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
		git.LocalOptions{Revision: bootstrap.Revision, Work: request.work},
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
	mode := gittree.Source
	if facts, ok := candidate.object.(interface {
		Composition() manifest.ArtifactComposition
	}); ok && len(facts.Composition().Includes) != 0 {
		mode, ignore = gittree.Checkout, ""
	}
	label, viewKey := gittree.Label(
		gittree.LabelOptions{Root: repoRoot, Mode: mode, Source: candidate.object.Named(), Content: struct {
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
				Mode:           mode,
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
	artifact := bootstrap.WithReads(bootstrap.Reads)
	artifact.Kind, artifact.URL, artifact.FS = id.Kind, "file://"+repoRoot, input
	artifact.ContentFingerprint = gittree.ContentFingerprint
	artifact.InputAlias, artifact.Key, artifact.Content = true, "", identity
	artifact.Warnings = nil
	artifact.LocalPath = label
	artifact.LocalRoot = repoRoot
	return artifact, viewKey, nil
}

// Only working-input aliases contribute; canonical and independently committed artifacts do not.
func (p *entrySourcePublisher) aliasWarning() *manifest.Warning {
	var names []string
	for id := range p.snapshot() {
		artifact, _ := p.store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		if id != manifest.BootstrapSourceID && artifact != nil && artifact.InputAlias {
			names = append(names, id.String())
		}
	}
	if len(names) <= 1 {
		return nil
	}
	slices.Sort(names)
	return &manifest.Warning{
		Category: manifest.WarnBootstrapAliases,
		Message:  "aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
		Detail:   append(names, p.root),
		Count:    1,
	}
}

// Warnings MUST replace construction alias diagnostics after runtime publication drains.
func (a *Admission) Warnings() []manifest.Warning {
	warnings := slices.DeleteFunc(a.store.Warnings(), func(warning manifest.Warning) bool {
		return warning.Category == manifest.WarnBootstrapAliases
	})
	if warning := a.resolvedSources.aliasWarning(); warning != nil {
		warnings = append(warnings, *warning)
	}
	slices.SortFunc(warnings, manifest.CompareWarning)
	return warnings
}
