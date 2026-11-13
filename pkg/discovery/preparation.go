package discovery

import (
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
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
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		if ref.Use != "substituteFrom" {
			continue
		}
		input := d.cfg.Store.ReferenceFacts(ref.NamedResource, d.loader.Index, ks.Named())
		value, err := values.Classify(&ref, &input)
		if err != nil || value.State() != values.Readable {
			return false
		}
	}
	return true
}

func (q *entryQualification) close() {
	q.preparation.Retire()
	*q = entryQualification{}
}

// TakePreparation MUST run only on the matching owner after construction freezes.
func (r *Result) TakePreparation(
	cache *kustomize.TreeCache,
	request *kustomize.RenderRequest,
	destination *kustomize.Preparation,
) bool {
	if r == nil || request.Spec == nil {
		return false
	}
	id := request.Spec.Named()
	if frame := r.prepared[id]; frame != nil && destination.Take(&frame.preparation, cache, request) {
		return true
	}
	return false
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
	p.preparation.Retire()
	p.retireSupport(st)
	p.work, p.owner, p.source, p.receipt, p.content = nil, nil, nil, nil, nil
	p.children, p.warnings = nil, nil
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
		r.Admission.entryPreparation.Retire()
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
		d.entryAdmission.entryPreparation.Retire()
		d.entryAdmission.entryRequest = kustomize.RenderRequest{}
		d.entryAdmission.acceptedEntry = nil
	}
	if d.ownsLoader {
		d.loader.Close()
	}
	d.prepared = nil
}
