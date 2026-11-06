package discovery

import (
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

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
	p.substitutions = values.DependencySubstitutions{}
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
		r.Admission.entryPreparation.Retire()
		r.Admission.entryRequest = kustomize.RenderRequest{}
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
		d.entryAdmission.entryPreparation.Retire()
		d.entryAdmission.entryRequest = kustomize.RenderRequest{}
	}
	if d.ownsLoader {
		d.loader.Close()
	}
	d.prepared = nil
}
