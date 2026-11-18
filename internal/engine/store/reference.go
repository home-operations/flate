package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

type referenceResolution struct {
	index     *manifest.DiscoveryIndex
	consumer  manifest.NamedResource
	reads     *ReadSet
	reference *Declaration
}

func (f *ReferenceFacts) resolve(
	id manifest.NamedResource,

	scope *referenceResolution,
) ReferenceFacts {
	if !id.KindInfo().Data {
		return *f
	}
	owner := scope.consumer
	input := *f
	input.Declared = f.Data != nil
	input.Data, input.Original, input.Warnings = nil, nil, nil
	production := trackReferenceProductions(&input, owner)
	scope.resolveData(&input, f, &production)
	input.captureDeclaration(id, scope, production.independent)
	input.selectKeys()
	return input.resolveReference(id, scope)
}

func (r *referenceResolution) resolveData(input, original *ReferenceFacts, production *referenceProductionRoles) {
	allowData := r.consumer == (manifest.NamedResource{}) || original.Accepted || production.independent ||
		!production.materialized
	if allowData {
		input.Data = original.Data
		if input.Data != nil {
			input.Warnings = original.Warnings
		}
		if input.Data == nil {
			for index := range original.ReferenceData {
				candidate := &original.ReferenceData[index]
				if candidate.OfflineData != nil {
					input.Data, input.Offline = candidate.OfflineData, true
					input.Warnings = candidate.Warnings
					break
				}
			}
		}
	}
}

func (f *ReferenceFacts) captureDeclaration(
	id manifest.NamedResource,
	scope *referenceResolution,
	independent bool,
) {
	consumer, known := scope.index.Lookup(scope.consumer)
	f.Prepared = known && consumer.Authored == (manifest.NamedResource{}) &&
		f.Private && !independent
	record, exists := scope.index.Lookup(id)
	declared := exists && record.Path != ""
	if (!exists || record.Path == "") && id.Namespace != "" {
		bare := id
		bare.Namespace = ""
		record, exists = scope.index.Lookup(bare)
	}
	if f.Prepared {
		f.Declared = false
	} else if exists && record.Path != "" &&
		(len(record.Productions) == 0 || slices.ContainsFunc(f.Productions, func(p manifest.Production) bool { return p.Path != "" && p.Declaration == (manifest.NamedResource{}) })) {
		f.Declared = f.Declared || declared
		if data, ok := record.Declaration.(manifest.Data); ok && f.Data == nil && len(f.ReferenceData) == 0 {
			f.Keys = manifest.KeySet{Names: slices.Sorted(data.Keys()), Complete: data.KeysKnown()}
		}
	}
}

func (f *ReferenceFacts) selectKeys() {
	if f.Data == nil && !f.Keys.Complete && len(f.Keys.Names) == 0 {
		for i := range f.Productions {
			production := f.Productions[i]
			selected := false
			for i := range f.ReferenceData {
				candidate := &f.ReferenceData[i]
				if candidate.Production == production {
					f.Keys = candidate.Keys
					selected = true
					break
				}
			}
			if selected {
				break
			}
		}
	}
}

func (f *ReferenceFacts) resolveReference(
	id manifest.NamedResource,

	scope *referenceResolution,
) ReferenceFacts {
	f.Original = f.Data
	scope.applySupplied(id, f)
	for index := range f.Productions {
		production := &f.Productions[index]
		external := production.Owner != (manifest.NamedResource{}) && production.Owner != scope.consumer
		if external && !slices.Contains(f.Materializers, production.Owner) {
			f.Materializers = append(f.Materializers, production.Owner)
		}
	}
	if f.Data != nil {
		if f.Generated && f.Original == nil {
			names := slices.AppendSeq(slices.Clone(f.Keys.Names), f.Data.Keys())
			slices.Sort(names)
			f.Keys.Names = slices.Compact(names)
		} else {
			f.Keys = manifest.KeySet{Names: slices.Sorted(f.Data.Keys()), Complete: f.Data.KeysKnown()}
		}
	}
	return *f
}

type referenceProductionRoles struct {
	independent  bool
	materialized bool
}

func trackReferenceProductions(input *ReferenceFacts, owner manifest.NamedResource) referenceProductionRoles {
	self, independent, materialized := false, false, false
	for index := range input.Productions {
		production := &input.Productions[index]
		self = self || production.Owner == owner && owner != (manifest.NamedResource{})
		materialized = materialized || production.Owner != (manifest.NamedResource{}) && production.Owner != owner
		if production.Declaration == (manifest.NamedResource{}) {
			input.Offline = input.Offline || production.Owner != (manifest.NamedResource{}) || production.Path != ""
			independent = independent || production.Owner == (manifest.NamedResource{}) && production.Path != ""
		} else {
			input.Generated = true
		}
	}
	input.Private = self
	return referenceProductionRoles{independent: independent, materialized: materialized}
}

func (r *referenceResolution) applySupplied(id manifest.NamedResource, f *ReferenceFacts) {
	var supplied manifest.Data
	if r.reference != nil {
		supplied, _ = r.reference.Obj.(manifest.Data)
	}
	if supplied == nil {
		return
	}
	f.Prepared = false
	f.Declared = true
	if len(r.reference.Warnings) != 0 {
		f.Warnings = append(slices.Clone(f.Warnings), r.reference.Warnings...)
	}
	if f.Data == nil {
		f.Data = supplied
	} else {
		var overridden []string
		supply := f.Data.WithSupplied(supplied)
		f.Data, overridden = supply.Data, supply.Overridden
		if len(overridden) > 0 && r.reads != nil {
			r.reads.RecordSuppliedOverride(
				SuppliedOverride{Resource: id, SupplyPath: r.reference.InputPath, Keys: overridden},
			)
		}
	}
}

// ReferenceFacts captures visible data and its accepted source-qualified authority.
func (r *ReadSet) ReferenceFacts(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,

	consumer manifest.NamedResource,
) ReferenceFacts {
	if index == nil {
		index = r.store.index.Load()
	}
	input := r.input(id)
	r.Capture(input)
	var data manifest.Data
	if input.view.child.Origin.Policy(id, nil).Root {
		data, _ = input.view.child.Obj.(manifest.Data)
	}
	input.derived |= capturedProductions
	productions := r.productions(input, index)
	var candidates []manifest.ReferenceData
	if len(productions) != 0 {
		candidates = r.store.referenceData(id, index, productions, r)
	}
	for index := range productions {
		production := &productions[index]
		if production.Source.Name != "" {
			r.GetArtifact(production.Source, ArtifactContent)
		}
	}
	facts := ReferenceFacts{
		Data:          data,
		Warnings:      input.view.child.Warnings,
		Accepted:      input.view.accepted || input.view.owner != nil,
		Productions:   productions,
		ReferenceData: candidates,
	}
	return facts.resolve(
		id,

		&referenceResolution{index: index, consumer: consumer, reads: r, reference: input.view.reference},
	)
}
