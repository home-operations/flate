package discovery

import (
	"context"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

func (d *Session) canPrepare(ks *manifest.Kustomization) bool {
	if ks.PostBuild == nil || len(ks.PostBuild.SubstituteFrom) == 0 {
		return true
	}
	application := manifest.EntryApplication{Source: ks.EffectiveSourceID(), Path: ks.Path}
	// Entry self-appliers MUST resolve referenced values through ordinary execution.
	if d.admission().appliesEntry(&application) {
		return false
	}
	ready := true
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		if ref.Use != "substituteFrom" {
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
		if err := d.prepareProducer(ctx, pkg.render.owner, d.cfg.Input, nil); err != nil {
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
	request := kustomize.RenderRequest{
		Source: input, Spec: owner, SubPath: owner.Path,
		ApplyIgnore:         kustomize.ApplySourceIgnore(owner, input),
		StrictSubstitutions: d.cfg.StrictSubstitutions, SkipKinds: d.cfg.SkipKinds,
	}
	if !pkg.render.preparation.Matches(d.cfg.Trees, &request) {
		return
	}
	frame := &declarationPreparation{
		declarationRender: pkg.render,
		complete:          true,
		work:              d.preparationWork(owner, owner.EffectiveSourceID()).RenderWork(),
	}
	frame.captureSupport(d.cfg.Store, owner.EffectiveSourceID())
	if d.prepared == nil {
		d.prepared = map[manifest.NamedResource]*declarationPreparation{}
	}
	d.prepared[owner.Named()] = frame
}

func (q *entryQualification) close() {
	q.preparation.Retire()
	*q = entryQualification{}
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
		if q := r.Admission.qualification; q != nil {
			q.close()
		}
		r.Admission.entryPreparation = kustomize.Preparation{}
		r.Admission.entryRequest = kustomize.RenderRequest{}
		r.Admission.acceptedEntry = nil
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
	d.entryBuilderInput = nil
	d.entryBuilderPath = ""
	d.entryBuilder = false
	d.pendingSources = nil
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
		if q := d.entryAdmission.qualification; q != nil {
			q.close()
		}
		d.entryAdmission.entryPreparation = kustomize.Preparation{}
		d.entryAdmission.entryRequest = kustomize.RenderRequest{}
		d.entryAdmission.acceptedEntry = nil
	}
	if d.ownsLoader {
		d.loader.Close()
	}
	d.prepared = nil
}
