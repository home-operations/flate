package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/baseline"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/git"
	"github.com/home-operations/flate/internal/engine/source/gitrepo"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type entrySource interface {
	BootstrapAlias(string) (manifest.BaseManifest, *manifest.GitRepository)
}

type entryResolution struct {
	producers   []occurrenceKey
	object      manifest.BaseManifest
	placeholder *manifest.BaseManifest // Synthetic declaration retained until its producer emits.
	input       *store.SourceArtifact
	git         *manifest.GitRepository
	synthetic   bool
	entry       bool
	entryScoped bool // A scoped denial is authoritative for the selected declaration.
	projected   bool
	provider    store.SourceProvider
	artifact    *store.SourceArtifact
	warnings    []manifest.Warning
	viewKey     sourceViewKey
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
		gainsEntry := next.entry && !r.entry
		gainsScope := next.entryScoped && !r.entryScoped
		replace = r.object == nil || r.synthetic || r.projected ||
			r.sameDeclaration(next) && (gainsEntry || gainsScope)
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
	if object == nil {
		return false
	}
	return r.synthetic && object == r.object || r.placeholder != nil && object == *r.placeholder
}

type entrySourcePublisher struct {
	bootstrap manifest.BaseManifest
	store     *store.Store
	root      string
	mutable   map[manifest.NamedResource]entryResolution
	current   atomic.Pointer[map[manifest.NamedResource]entryResolution]
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
		if input, ok := inputprojection.Authored(admitted).FS.(*gittree.FS); ok && input.Location() != nil {
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
	if admitted.Provider != store.ProviderInput {
		captured := *admitted
		captured.Provider = store.ProviderInput
		admitted = &captured
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
	d.remotes = slices.Clone(urls)
	d.cfg.Input = admitted
	if d.loader != nil {
		d.loader.Options.ConfiguredInput = admitted
	}
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
	if d.loader != nil {
		d.loader.Options.ConfiguredInput = bootstrap
	}
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
			d.cfg.Store.AddDeclaration(
				store.Declaration{Obj: group.object, Doc: group.document},
			)
			return d.cfg.Store.GetObject(id)
		}
		object := d.cfg.Store.GetObject(id)
		if previous := d.resolvedSources.snapshot()[id]; object != d.resolvedSources.bootstrap &&
			!previous.ownsDeclaration(object) {
			d.cfg.Store.DeleteObject(id)
		}
		return group.object
	}
	restricted := d.loader != nil && !d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{})
	obj := d.cfg.Store.GetObject(id)
	if previous := d.resolvedSources.snapshot()[id]; group.object != nil && obj != nil &&
		(obj == d.resolvedSources.bootstrap || previous.ownsDeclaration(obj)) {
		obj = group.object
		d.cfg.Store.AddDeclaration(store.Declaration{Obj: obj, Doc: group.document})
	}
	if obj != nil {
		return obj
	}
	obj = group.object
	independentlyAdmitted := false
	if restricted && obj != nil {
		independentlyAdmitted = selection.Root(
			selection.CaptureRoot(d.loader.RootEvidence(id, nil), d.loader.Store, d.loader.Index),
		).Admitted
	}
	if obj != nil &&
		(independentlyAdmitted || slices.ContainsFunc(group.refs, func(requester manifest.NamedResource) bool { return d.cfg.Store.GetObject(requester) != nil })) {
		d.cfg.Store.AddDeclaration(store.Declaration{Obj: obj, Doc: group.document})
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
			if !current.SameMaterial(
				previous.artifact,
				store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
			) {
				return false, nil
			}
			d.cfg.Store.DeleteObject(id)
		}
		d.resolvedSources.record(id, &candidate)
		return false, nil
	}
	published := d.publishResolvedSource(&candidate, group.projectedFromParent)
	return published, d.requireSource(id)
}

func (d *Session) publishResolvedSource(candidate *entryResolution, projectedFromParent bool) bool {
	id := candidate.object.Named()
	current, _ := d.cfg.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	sameMaterial := current.SameMaterial(
		candidate.artifact,
		store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
	) &&
		current.Reads.Valid()
	sameMaterial = sameMaterial || d.sourceMaterial(id, candidate.artifact) != nil
	previous := d.resolvedSources.snapshot()[id]
	refresh := previous.artifact != nil && previous.input != candidate.input
	object := d.cfg.Store.GetObject(id)
	restricted := d.loader != nil && !d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{})
	replacing := restricted && object != nil && object != candidate.object
	// Parent-owned projections MUST wait for the parent emission.
	projecting := d.finalized && candidate.projected && !projectedFromParent && d.cfg.Store.GetDeclaration(id) == nil
	d.resolvedSources.publish(candidate, candidate.synthetic || replacing || projecting)
	if candidate.viewKey.mount != "" {
		if d.entryAdmission.aliasViews == nil {
			d.entryAdmission.aliasViews = sourceViews{}
		}
		d.entryAdmission.aliasViews.remember(candidate.viewKey, candidate.artifact)
	}
	if id == manifest.BootstrapSourceID && candidate.artifact.WorkingTree() &&
		candidate.artifact.LocalPath == candidate.input.LocalPath {
		d.bindBootstrapArtifact(candidate)
		return false
	}
	published := !sameMaterial && candidate.provider != store.ProviderInput
	d.hasPins = d.hasPins || candidate.provider == store.ProviderCommit
	if sameMaterial || refresh {
		return published
	}
	if candidate.viewKey.mount == "" {
		return published
	}
	if d.aliasViews == nil {
		d.aliasViews = sourceViews{}
	}
	if d.aliasViews[candidate.viewKey.mount].artifact == nil {
		d.aliasViews.remember(candidate.viewKey, candidate.artifact)
	}
	return published
}

func (d *Session) bindBootstrapArtifact(candidate *entryResolution) {
	d.cfg.Input = candidate.artifact
	if len(candidate.object.Composition().Includes) == 0 {
		return
	}
	if artifact := d.sourceMaterial(manifest.BootstrapSourceID, candidate.artifact); artifact != nil {
		d.cfg.Input = artifact
	}
}

// Admission retains immutable bootstrap policy for early and runtime source decisions.
type Admission struct {
	generatorPackages  []string
	privateInventories []*declarationRender
	entryPreparation   kustomize.Preparation
	packageRender      packagePreparation
	entryRequest       kustomize.RenderRequest
	requestedEntry     manifest.EntryApplication
	entryEvidence      *kustomize.EntryEvidence
	documents          *entryDocuments
	generation         map[manifest.NamedResource][]retainedGeneration
	applications       atomic.Pointer[applicationEvidence]
	entryBound         atomic.Bool
	resolvedSources    *entrySourcePublisher
	input              *store.SourceArtifact
	path               string
	remotes            []string
	// Construction MUST finish all view writes before runtime admission.
	aliasViews    sourceViews
	entryDemand   bool
	qualification *entryQualification
	store         *store.Store
	acceptedEntry *selection.EntryRoute
	assessment    *store.ReadSet
	loader        *loader.Loader
	trees         *kustomize.TreeCache
	session       *Session
}

// Input returns the admitted checkout view at its original mount.
func (a *Admission) Input() *store.SourceArtifact {
	selected := a.resolvedSources.snapshot()[manifest.BootstrapSourceID]
	if selected.artifact == nil || selected.artifact.LocalPath != a.input.LocalPath {
		return a.input
	}
	if material, ok := a.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); ok &&
		material.Reads.Valid() &&
		(material.SameMaterial(selected.artifact, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin) ||
			len(selected.object.Composition().Includes) != 0) {
		return material
	}
	return selected.artifact
}

func (d *Session) admission() *Admission {
	if admission := d.entryAdmission; admission != nil {
		d.refreshAdmission(admission)
		return admission
	}
	input := d.cfg.Input
	if input == nil {
		input, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	admission := &Admission{
		packageRender:     d.cfg.packagePreparation(),
		generatorPackages: d.generatorPackages,
		input:             input,
		path:              d.cfg.Path,
		remotes:           d.remotes,
		aliasViews:        maps.Clone(d.aliasViews),
		resolvedSources:   d.resolvedSources,
		store:             d.cfg.Store,
		loader:            d.loader,
		trees:             d.cfg.Trees,
		session:           d,
	}
	admission.requestedEntry = manifest.EntryApplication{
		Source: manifest.BootstrapSourceID, Path: admission.entryPath(),
	}
	d.entryState = entryQualification{
		changedOnly: d.cfg.Selection.Changed,
		packages:    d.entryPackagesToQualify,
		loader:      d.loader,
		trees:       d.cfg.Trees,
		session:     d,
	}
	admission.entryDemand = d.requiresEntry()
	d.entryState.closed = !admission.entryDemand
	admission.qualification = &d.entryState
	d.entryAdmission = admission
	if d.loader != nil {
		d.loader.SourceProvider = admission.Provider
	}
	admission.publishEntry(nil, manifest.NamedResource{})
	return admission
}

func (d *Session) refreshAdmission(admission *Admission) {
	admission.resolvedSources = d.resolvedSources
	admission.entryDemand = d.requiresEntry()
	if admission.entryDemand {
		admission.session = d
	}
	if q := admission.qualification; q != nil && !admission.entryBound.Load() {
		q.closed = !admission.entryDemand
		if admission.entryDemand {
			q.session = d
		}
	}
}

// CheckoutEntry reports whether the accepted source supplies its own entry declaration.
func (a *Admission) CheckoutEntry(id manifest.NamedResource) bool {
	if a == nil {
		return false
	}
	resolution := a.resolvedSources.snapshot()[id]
	entry := resolution.entry || resolution.projected
	return resolution.provider == store.ProviderInput || resolution.provider == store.ProviderAlias && entry
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
	if reads != nil && id != manifest.BootstrapSourceID && resolved.artifact.WorkingTree() {
		reads.GetArtifact(
			manifest.BootstrapSourceID,
			store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
		)
	}
	current, _ := a.store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	if current.Available() && resolved.reusesMaterial(current) {
		resolved.artifact = current
	}
	if resolved.artifact != nil && len(resolved.warnings) != 0 {
		captured := resolved.artifact.WithReads(resolved.artifact.Reads)
		captured.Warnings = append(slices.Clone(captured.Warnings), resolved.warnings...)
		resolved.artifact = captured
	}

	return store.Child{Obj: resolved.object}, resolved.artifact, err
}

func (r *entryResolution) reusesMaterial(current *store.SourceArtifact) bool {
	if r.artifact == nil {
		return current.Provider == store.ProviderGenerator
	}
	return current.SameMaterial(r.artifact, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin)
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
	if candidate.provider == store.ProviderInput && candidate.artifact == a.input && needsSourcePolicy(a.input) {
		candidate.artifact = nil
	}
	if candidate.artifact != nil || candidate.provider == store.ProviderFetch ||
		candidate.provider == store.ProviderGenerator || candidate.provider == store.ProviderAbsent {
		return candidate, nil
	}
	bootstrap := candidate.input
	if candidate.synthetic && candidate.object == nil {
		candidate.object, _ = id.BindLocal(bootstrap.LocalPath)
		if source, ok := candidate.object.(entrySource); ok {
			_, candidate.git = source.BootstrapAlias(bootstrap.LocalPath)
		}
	}
	if err := candidate.resolveGit(
		ctx,
		id,
		request.work,
	); err != nil || candidate.provider == store.ProviderFetch ||
		candidate.artifact != nil {
		return candidate, err
	}
	if id == manifest.BootstrapSourceID && candidate.provider == store.ProviderAlias &&
		candidate.usesFullGitCheckout() {
		composition := candidate.object.Composition()
		view := bootstrap
		// Planned projections MUST retain their alias coordinates until runtime captures the declaration.
		if request.reads != nil && needsSourcePolicy(bootstrap) && a.resolvedSources.snapshot()[id].input == bootstrap {
			view = a.Input()
		}
		if view.Available() && gittree.PolicyKey(inputprojection.Authored(view).FS) == gittree.DefaultSourcePolicyKey &&
			composition.Ignore == "" && len(composition.Includes) == 0 {
			candidate.provider, candidate.artifact = store.ProviderInput, view
			return candidate, ctx.Err()
		}
	}
	artifact, key, err := a.entrySourceView(ctx, &candidate)
	candidate.artifact, candidate.viewKey = artifact, key
	return candidate, err
}

func (r *entryResolution) usesFullGitCheckout() bool {
	return r.git != nil && len(r.git.SparseCheckout) == 0 && !r.git.RecurseSubmodules
}

func needsSourcePolicy(input *store.SourceArtifact) bool {
	if input == nil {
		return false
	}
	view, native := input.FS.(*gittree.FS)
	return native && gittree.PolicyKey(view) == ""
}

func (r *entryResolution) resolveGit(ctx context.Context, id manifest.NamedResource, work manifest.WorkScope) error {
	descriptor := r.git
	if descriptor == nil {
		return nil
	}
	ref := (*manifest.GitRepositoryRef)(descriptor.Reference)
	if err := ref.Validate(); err != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("resolve local git: %w", err), "")
	}
	if r.provider != store.ProviderCommit && !descriptor.RecurseSubmodules {
		return nil
	}
	var location *gitrepo.Location
	if input, ok := inputprojection.Authored(r.input).FS.(*gittree.FS); ok {
		location = input.Location()
	}
	artifacts, err := git.ResolveLocal(ctx, location, []*manifest.GitRepository{descriptor},
		git.LocalOptions{Revision: r.input.Revision, Work: work})
	if err != nil {
		return err
	}
	artifact := artifacts[id]
	if artifact == nil || artifact.FS == nil {
		r.provider = store.ProviderFetch
		r.warnings = []manifest.Warning{{
			Resource: id,
			Category: manifest.WarnSourceRefUnavailable,
			Message:  "declared source ref cannot be satisfied locally; using normal source fetch",
			Detail: []string{
				ref.String(),
				"ref unavailable in this tree's repository or checkout options require fetching",
			},
		}}
		return nil
	}
	if r.provider == store.ProviderCommit {
		captured := *artifact
		captured.Provider, captured.LocalRoot, captured.Extent = store.ProviderCommit, r.input.LocalPath, store.ExtentFiltered
		r.artifact, r.warnings = &captured, artifact.Warnings
	}
	return nil
}

type sourceViewKey struct {
	mount string
	input *store.SourceArtifact
	mode  gittree.ViewMode
}

type sourceView struct {
	artifact *store.SourceArtifact
	input    *store.SourceArtifact
	mode     gittree.ViewMode
}

type sourceViews map[string]sourceView

func (v sourceViews) remember(key sourceViewKey, artifact *store.SourceArtifact) {
	v[key.mount] = sourceView{artifact: artifact, input: key.input, mode: key.mode}
}

func (v sourceView) reusable(input *store.SourceArtifact, mode gittree.ViewMode) *store.SourceArtifact {
	if v.input == input && v.mode == mode {
		return v.artifact
	}
	return nil
}

func (a *Admission) entrySourceView(
	ctx context.Context,
	candidate *entryResolution,
) (*store.SourceArtifact, sourceViewKey, error) {
	bootstrap := candidate.input
	repoRoot := bootstrap.LocalPath
	id := candidate.object.Named()
	composition := candidate.object.Composition()
	ignore := composition.Ignore
	var sparse []string
	recurse := false
	if repo := candidate.git; repo != nil {
		sparse, recurse = repo.SparseCheckout, repo.RecurseSubmodules
	}
	mode := gittree.Source
	if len(composition.Includes) != 0 {
		mode, ignore = gittree.Checkout, ""
	}
	provider, otherProvider := store.ProviderAlias, store.ProviderInput
	if candidate.provider == store.ProviderInput {
		provider, otherProvider = otherProvider, provider
	}
	content := struct {
		Root, Origin, Key, Content, Digest string
		Ignore                             string
		Sparse                             []string `json:",omitempty"`
		Recurse                            bool
		Provider                           store.SourceProvider
	}{repoRoot, bootstrap.LocalRoot, bootstrap.Key, bootstrap.Content, bootstrap.Digest, ignore, sparse, recurse, provider}
	label, mount := gittree.Label(gittree.LabelOptions{Root: repoRoot, Mode: mode, Source: id, Content: content})
	key := sourceViewKey{mount: mount, input: bootstrap, mode: mode}
	if artifact := a.aliasViews[mount].reusable(bootstrap, mode); artifact != nil {
		label = a.checkoutMount(candidate, artifact.FS, label)
		if artifact.LocalPath != label {
			artifact = artifact.WithReads(artifact.Reads)
			artifact.LocalPath, artifact.Kind = label, id.Kind
		}
		return artifact, key, ctx.Err()
	}
	var input fs.FS
	var identity string
	content.Provider = otherProvider
	if other := a.aliasViews[manifest.Fingerprint(content)].reusable(bootstrap, mode); other != nil {
		input, identity = other.FS, other.Content
	} else {
		current, observed := a.loader.ProvisionalFS(bootstrap)
		view, fingerprint, err := gittree.New(
			ctx, nil, plumbing.ZeroHash,
			gittree.ViewOptions{
				Mode: mode, Current: current, CurrentObserved: observed,
				CurrentContent: bootstrap.Content, Ignore: ignore, Sparse: sparse,
			},
		)
		if err != nil {
			return nil, sourceViewKey{}, err
		}
		input, identity = view, fingerprint
	}
	label = a.checkoutMount(candidate, input, label)
	var artifact *store.SourceArtifact
	if label == repoRoot {
		artifact = bootstrap.WithSourceView(input)
	} else {
		artifact = bootstrap.WithReads(bootstrap.Reads)
		artifact.Key = ""
	}
	artifact.Kind, artifact.URL, artifact.FS = id.Kind, "file://"+repoRoot, input
	artifact.ContentFingerprint = gittree.ContentFingerprint
	artifact.Provider, artifact.Content = provider, identity
	artifact.Warnings = nil
	artifact.LocalPath, artifact.LocalRoot = label, bootstrap.OriginPath(repoRoot)
	if label == repoRoot && provider == store.ProviderInput {
		artifact.LocalRoot = bootstrap.LocalRoot
	}
	_, sourceIgnoreErr := fs.Stat(bootstrap.FS, ".sourceignore")
	declaredMembership := ignore != "" || len(sparse) != 0 || recurse
	if mode != gittree.Source || declaredMembership || !errors.Is(sourceIgnoreErr, fs.ErrNotExist) {
		artifact.Extent = store.ExtentFiltered
	}
	return artifact, key, ctx.Err()
}

func (a *Admission) checkoutMount(candidate *entryResolution, input fs.FS, alias string) string {
	id := candidate.object.Named()
	if id != manifest.BootstrapSourceID || candidate.projected {
		return alias
	}
	root := candidate.input.LocalPath
	// A view must retain the entry and its authored source declaration to replace the checkout mount.
	if path := a.entryPath(); filepath.IsLocal(path) {
		entry, err := fs.Stat(input, path)
		if err != nil || !entry.IsDir() {
			return alias
		}
	}
	if a.loader != nil {
		if record, _ := a.loader.Index.Lookup(id); record.File != "" {
			file, _ := candidate.input.SourcePath(root, record.File)
			if _, statErr := fs.Stat(input, file); statErr != nil {
				return alias
			}
		}
	}
	return root
}

// Only working-input aliases contribute; canonical and independently committed artifacts do not.
func (p *entrySourcePublisher) aliasWarning() *manifest.Warning {
	var names []string
	for id := range p.snapshot() {
		artifact, _ := p.store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		if id != manifest.BootstrapSourceID && artifact != nil && artifact.Provider == store.ProviderAlias &&
			artifact.Reads.Valid() {
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
func (a *Admission) Warnings(warnings []manifest.Warning) []manifest.Warning {
	warnings = slices.DeleteFunc(warnings, func(warning manifest.Warning) bool {
		return warning.Category == manifest.WarnBootstrapAliases
	})
	if warning := a.resolvedSources.aliasWarning(); warning != nil {
		warnings = append(warnings, *warning)
	}
	slices.SortFunc(warnings, manifest.CompareWarning)
	return warnings
}
