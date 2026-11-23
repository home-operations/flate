package discovery

import (
	"context"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/kustomize/patch"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

type packagePreparation struct {
	trees   *kustomize.TreeCache
	request kustomize.RenderRequestOptions
	retry   cache.RetryConfig
	stamp   func(manifest.BaseManifest, []manifest.DocOccurrence)
}

func (c *Config) packagePreparation() packagePreparation {
	return packagePreparation{
		trees: c.Trees, retry: c.RemoteRetry, stamp: c.PrepareDocuments,
		request: kustomize.RenderRequestOptions{
			SkipKinds: c.SkipKinds, StrictSubstitutions: c.StrictSubstitutions,
		},
	}
}

// Keep each usable child and the identity of each failed document from a successful build.
func (p *packagePreparation) inventory(
	ctx context.Context, frame *declarationRender, inputs []values.Result, work *manifest.RenderWork,
) error {
	options := p.request
	options.Source = frame.source
	request, err := kustomize.NewRenderRequest(frame.owner, inputs, &options)
	if err != nil {
		return err
	}
	request.Work = work
	request.Preparation = &frame.preparation
	docs, receipt, found := p.trees.PreviousRender(ctx, request)
	if !found {
		docs, receipt, err = kustomize.Render(ctx, p.trees, request, p.retry)
		if err != nil {
			return err
		}
	}
	p.stamp(request.Spec, docs)
	shaping := kustomize.PrepareShape(&request, docs)
	content := shaping.Content()
	if err := shaping.Apply(ctx, kustomize.ShapeOptions{
		Strict: request.StrictSubstitutions, Work: request.Work.Substitution(),
	}); err != nil {
		return err
	}
	frame.receipt, frame.content = receipt, content
	parsed := loader.ParseChildren(request.Spec, docs, manifest.ParseDocOptions{}, p.request.SkipKinds)
	frame.children, frame.failedDocuments = parsed.Children, parsed.Failures
	for _, doc := range docs {
		frame.warnings = append(frame.warnings, doc.Warnings...)
	}
	return nil
}

func (d *Session) entryMembership(
	ctx context.Context, frame *declarationPreparation, entry string,
) (map[manifest.NamedResource]bool, error) {
	ordinals := frame.preparation.EntryDocumentMembership(
		ctx, frame.receipt,
		func(files []string) []bool { return loader.EntryContributorSides(frame.source, entry, files) },
	)
	type membership struct{ inside, known bool }
	byChild := make(map[manifest.NamedResource]membership, len(frame.children))
	for i := range frame.children {
		child := &frame.children[i]
		id := child.Obj.Named()
		inside, known := ordinals[child.Ordinal]
		if previous, seen := byChild[id]; seen && (!previous.known || previous.inside != inside) {
			known = false
		}
		byChild[id] = membership{inside: inside, known: known}
	}

	result := make(map[manifest.NamedResource]bool, len(byChild))
	for i := range frame.children {
		id := frame.children[i].Obj.Named()
		placement := byChild[id]
		if !placement.known && d.cfg.Selection.Matches(id) {
			return nil, selection.EntryMembershipFailure(d.cfg.Path, frame.owner.Named(), id)
		}
		if placement.known && placement.inside {
			result[id] = true
		}
	}
	return result, nil
}

func (a *Admission) preparedInventory(
	owner *manifest.Kustomization,
	input *store.SourceArtifact,
) *declarationPreparation {
	var prepared map[manifest.NamedResource]*declarationPreparation
	if a.session != nil {
		prepared = a.session.prepared
	} else if a.qualification != nil {
		prepared = a.qualification.prepared
	}
	frame := prepared[owner.Named()]
	if frame != nil && frame.matches(owner, input, nil) {
		return frame
	}
	return nil
}

func (a *Admission) packageInventory(
	ctx context.Context, owner *manifest.Kustomization, input *store.SourceArtifact,
) (*declarationRender, error) {
	if frame := a.preparedInventory(owner, input); frame != nil {
		return &frame.declarationRender, frame.inventoryError()
	}
	frame := &declarationRender{owner: owner, source: input}
	work := a.loader.Options.Work.Source(owner.EffectiveSourceID()).RenderWork()
	return frame, a.packageRender.inventory(ctx, frame, nil, work)
}

func (p *declarationPreparation) inventoryReady() bool {
	return p.complete && p.inventoryError() == nil
}

func (p *declarationPreparation) inventoryError() error {
	if len(p.failedDocuments) != 0 {
		return nil
	}
	return p.err
}

type retainedGeneration struct {
	object manifest.BaseManifest
	output store.ChildDocuments
	err    error
}

// PreparedGeneration returns immutable documents. Discovery only writes at serial construction or quiescent boundaries.
func (a *Admission) PreparedGeneration(object manifest.BaseManifest) (store.ChildDocuments, bool, error) {
	for _, current := range a.generation[object.Named()] {
		if manifest.Equal(current.object, object) {
			return current.output, true, current.err
		}
	}
	return store.ChildDocuments{}, false, nil
}

func (a *Admission) prepareGeneration(
	object manifest.BaseManifest, work manifest.WorkScope,
) (store.ChildDocuments, error) {
	if object == nil {
		return store.ChildDocuments{}, nil
	}
	if output, ok, err := a.PreparedGeneration(object); ok {
		return output, err
	}
	output, err := patch.PrepareGeneration(object, work)
	if a.generation == nil {
		a.generation = map[manifest.NamedResource][]retainedGeneration{}
	}
	id := object.Named()
	a.generation[id] = append(a.generation[id], retainedGeneration{object: object, output: output, err: err})
	return output, err
}

func (d *Session) canPrepare(ks *manifest.Kustomization) bool {
	if ks.PostBuild == nil || len(ks.PostBuild.SubstituteFrom) == 0 {
		return true
	}
	application := manifest.EntryApplication{Source: ks.EffectiveSourceID(), Path: ks.Path}
	// Entry self-appliers MUST resolve referenced values through ordinary execution.
	if d.admission().reachesEntry(&application, nil) {
		return false
	}
	ready := true
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		if keyed, data := ref.DataInput(); !data || keyed {
			continue
		}
		input := d.cfg.Store.ReferenceFacts(ref.NamedResource, d.loader.Index, ks.Named())
		value := values.Resolve(
			&values.ResolveRequest{Reference: ref, Facts: input, Permission: d.loader.Options.Missing},
		)
		if value.MaterialError() != nil {
			return true
		}
		if !value.CanPrepare() {
			ready = false
		}
	}
	return ready
}

func (d *Session) preparePlannedPackages(ctx context.Context) (bool, error) {
	registered := false
	for _, pkg := range d.entryPackages {
		d.retainPackage(pkg)
		if !pkg.register {
			continue
		}
		if err := d.prepareEntryPackage(ctx, pkg.render.owner, d.cfg.Input, nil); err != nil {
			return false, err
		}
		registered = true
	}
	return registered, nil
}

func (d *Session) retainPackage(pkg *plannedPackage) {
	input, owner := d.cfg.Input, pkg.render.owner
	if pkg.render.source != input || d.prepared[owner.Named()] != nil {
		return
	}
	request, err := kustomize.NewRenderRequest(owner, nil, &kustomize.RenderRequestOptions{
		Source:              input,
		SkipKinds:           d.cfg.SkipKinds,
		StrictSubstitutions: d.cfg.StrictSubstitutions,
	})
	if err != nil || !pkg.render.preparation.Matches(d.cfg.Trees, &request) {
		return
	}
	frame := &declarationPreparation{
		declarationRender: pkg.render,
		err:               manifest.DocumentFailure(pkg.render.failedDocuments),
		complete:          true,
		work:              d.preparationWork(owner, owner.EffectiveSourceID()).RenderWork(),
	}
	frame.captureSupport(d.cfg.Store, owner.EffectiveSourceID())
	if d.prepared == nil {
		d.prepared = map[manifest.NamedResource]*declarationPreparation{}
	}
	d.prepared[owner.Named()] = frame
}

func (q *entryQualification) retireWait() {
	if q.waiting != nil {
		q.waiting.Retire()
		q.waiting.ReleaseCapture()
		q.waiting = nil
	}
}

func (q *entryQualification) close() {
	q.retireWait()
	q.preparation.Retire()
	*q = entryQualification{}
}

func (a *Admission) retireQualification() {
	if q := a.qualification; q != nil {
		q.close()
		a.qualification = nil
	}
	a.applications.Store(nil)
}

// Preparation MUST be accessed only by the matching owner after construction freezes.
func (r *Result) Preparation(id manifest.NamedResource) *kustomize.Preparation {
	if r == nil {
		return nil
	}
	if frame := r.prepared[id]; frame != nil {
		return &frame.preparation
	}
	return nil
}

func (p *declarationPreparation) captureSupport(st *store.Store, source manifest.NamedResource) {
	p.support.Init(st, p.owner.Named(), nil)
	p.support.GetArtifact(source, store.ArtifactContent)
	if st.GetDeclaration(p.owner.Named()) != nil {
		p.support.GetDeclaration(p.owner.Named())
	}
}

func (p *declarationPreparation) retireSupport(st *store.Store) {
	if !p.registered {
		p.support.ReleaseCapture()
	}
	st.Retire(&p.support)
}

func (p *declarationPreparation) retire(st *store.Store) {
	p.retireSupport(st)
	p.declarationRender = declarationRender{}
	p.work = nil
	p.complete, p.registered, p.err = false, false, nil
}

// Close MUST follow task drain and every possible construction consumer.
func (r *Result) Close() {
	if r == nil {
		return
	}
	for id, frame := range r.prepared {
		frame.retire(r.loader.Store)
		delete(r.prepared, id)
	}
	if r.Admission != nil {
		if r.loader != nil {
			r.loader.Options.Session.PublishEntry(nil)
		}
		r.Admission.retireQualification()
		r.Admission.entryPreparation = kustomize.Preparation{}
		r.Admission.entryRequest = kustomize.RenderRequest{}
		r.Admission.acceptedEntry = nil
		r.Admission.generation = nil
	}
	if r.ownsLoader {
		r.loader.Close()
	}
	r.prepared, r.loader, r.ownsLoader = nil, nil, false
}

// Close MUST also cover construction failure before a Result exists.
func (d *Session) Close() {
	if d == nil {
		return
	}
	d.planner = nil
	d.packages = nil
	d.entryPackages = nil
	d.entryPackagesToQualify = nil
	d.generatorPackages = nil
	d.publicOperands = nil
	d.entryBuilderInput = nil
	d.entryBuilderPath = ""
	d.entryBuilder = false
	d.pendingSources = nil
	d.lookupSources = nil
	d.sourceGroups = nil
	if d.result != nil {
		d.result.Close()
		d.prepared = nil
		return
	}
	for id, frame := range d.prepared {
		frame.retire(d.cfg.Store)
		delete(d.prepared, id)
	}
	if d.entryAdmission != nil {
		d.loader.Options.Session.PublishEntry(nil)
		d.entryAdmission.retireQualification()
		d.entryAdmission.entryPreparation = kustomize.Preparation{}
		d.entryAdmission.entryRequest = kustomize.RenderRequest{}
		d.entryAdmission.acceptedEntry = nil
		d.entryAdmission.generation = nil
	}
	if d.ownsLoader {
		d.loader.Close()
	}
	d.prepared = nil
}

func (d *Session) hasEntryOperands() bool {
	return len(d.entryPackagesToQualify) != 0 || len(d.generatorPackages) != 0
}

func (d *Session) finishEntryOperands(ctx context.Context, repoRoot string) error {
	hasPrivate := len(d.entryPackagesToQualify) != 0
	if d.hasEntryOperands() && d.requiresEntry() && !d.admission().entryBound.Load() {
		if _, err := d.bindEntry(ctx, repoRoot); err != nil {
			return err
		}
	}
	l := d.loader
	if l.Options.Session.Snapshot().Output(manifest.NamedResource{}) && (!hasPrivate || d.entryState.operandsSettled) {
		if err := l.PromoteOrphans(ctx); err != nil {
			return err
		}
	}
	for _, id := range d.publicOperands {
		d.cfg.Store.RetainOperand(id)
	}
	d.publicOperands = nil
	return nil
}
