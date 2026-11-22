package store

import (
	"context"

	"github.com/home-operations/flate/pkg/manifest"
)

type admissionMode uint8

const (
	admitRaw admissionMode = iota
	admitGeneratedOperand
	admitSelectionProposal
)

// Admit binds raw-file admission to the captured source that supplied its bytes.
//
//nolint:gocritic // Admission consumes a stable child snapshot through publication retries.
func (r *ReadSet) Admit(child Child, source, owner manifest.NamedResource) {
	r.admit(child, source, owner, admitRaw)
}

// AdmitSelection MUST recheck production reads after publishing the proposal's absent declarations.
func (r *ReadSet) AdmitSelection(
	ctx context.Context,
	children []Child,
	sources []manifest.NamedResource,
	current func() bool,
) bool {
	complete := true
	for i := range children {
		if ctx.Err() != nil || !r.Valid() || !current() {
			complete = false
			break
		}
		r.admit(children[i], sources[i], manifest.NamedResource{}, admitSelectionProposal)
	}
	for input := r.inputs; input != nil; input = input.next {
		if production := input.production.Load(); production != nil && production.consumed.Load() {
			production.consumed.Store(false)
		}
	}
	return complete && ctx.Err() == nil && r.Valid() && current()
}

// AdmitConstructed retains the source coordinate captured by construction.
func (r *ReadSet) AdmitConstructed(child *Child) {
	source := child.SourceSupport
	for i := range child.Productions {
		if source == (manifest.NamedResource{}) && child.Productions[i].Source != (manifest.NamedResource{}) {
			source = child.Productions[i].Source
		}
	}
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
		if declaration := r.GetDeclaration(r.ID()); declaration != nil {
			if ref, _, applies := declaration.SourcePath(); applies && ref != child.Obj.Named() {
				source = ref
			}
		}
	}
	r.Admit(*child, source, manifest.NamedResource{})
}

// AdmitOperand retains physical support under the captured source while an emission supplies the value.
func (r *ReadSet) AdmitOperand(obj manifest.BaseManifest, source manifest.NamedResource) {
	r.admit(Child{Obj: obj}, source, manifest.NamedResource{}, admitGeneratedOperand)
}

//nolint:gocritic // Admission consumes a stable child snapshot through publication retries.
func (r *ReadSet) admit(child Child, source, owner manifest.NamedResource, mode admissionMode) {
	if child.SourceSupport != (manifest.NamedResource{}) &&
		(child.SourceSupport != source || !r.Valid() || !r.SourceAdmission(child.Obj.Named(), source)) {
		return
	}
	r.GetArtifact(source, ArtifactContent)
	index := r.store.index.Load()
	record, _ := index.Lookup(child.Obj.Named())
	prepared := preparedDeclaration(&record, child.Obj) && !rawDeclarationAuthority(index, child.Obj.Named(), true)
	if prepared {
		owner = record.Parent
	}
	var support *Input
	if owner != (manifest.NamedResource{}) && owner != child.Obj.Named() {
		if prepared {
			r.GetDeclaration(owner)
			r.bindAuthority(r.input(owner), objectFacet, objectFacet)
		}
		support = r.bindAuthority(r.input(owner), producerFacet, producerFacet)
	}
	update := objectUpdate{
		obj: child.Obj, document: child.Doc, explicit: child.Doc != nil,
		binding: r.bindAuthority(r.input(source), contentFacet, originFacet), support: support, reads: r,
		classified: child.SourceSupport != (manifest.NamedResource{}), warnings: child.Warnings,
		operand: mode == admitGeneratedOperand,
	}
	if child.VisibleGeneration {
		update.generated = new(child)
	}
	refresh := r.bindAdmissionTarget(&update, child.Obj.Named(), mode == admitSelectionProposal)
	r.store.addObject(&update)
	if refresh && update.target.consumed.Load() {
		r.invalid.Store(true)
	}
}

func (r *ReadSet) bindAdmissionTarget(update *objectUpdate, id manifest.NamedResource, selection bool) bool {
	refresh := false
	for input := r.inputs; input != nil; input = input.next {
		switch {
		case input.Ref.NamedResource == id && input.object.head != nil && input.view.declaration() == nil:
			update.target = &input.object
			refresh = input.gate
			if selection && update.classified && !refresh {
				update.production = input.production.Load()
			}
		case input.Ref.NamedResource == (manifest.NamedResource{Group: id.Group, Kind: id.Kind}):
			update.collection = input.cell(bucketFacet)
		case input.Ref.NamedResource == (manifest.NamedResource{}):
			details := input.details.Load()
			if details != nil && details.inventory != nil {
				update.inventory = &details.inventory[kindShardIndex(id.Kind)]
			}
		}
	}
	return refresh
}
