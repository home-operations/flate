package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

type referenceResolution struct {
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
	input.Data, input.Original, input.Warnings = nil, nil, nil
	production := trackReferenceProductions(&input, owner)
	allowData := owner == (manifest.NamedResource{}) || f.Accepted || production.independent || !production.materialized
	if allowData {
		input.Data = f.Data
		if input.Data != nil {
			input.Warnings = f.Warnings
		}
		if input.Data == nil {
			for index := range f.ReferenceData {
				candidate := &f.ReferenceData[index]
				if candidate.OfflineData != nil {
					input.Data, input.Offline = candidate.OfflineData, true
					input.Warnings = candidate.Warnings
					break
				}
			}
		}
	}
	if scope.reads != nil {
		for warningIndex := range input.Warnings {
			warning := input.Warnings[warningIndex]
			warning.Resource = id
			scope.reads.AddWarning(warning)
		}
	}
	return input.resolveReference(id, scope)
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

func (sc *referenceResolution) applySupplied(id manifest.NamedResource, f *ReferenceFacts) {
	var supplied manifest.Data
	if sc.reference != nil {
		supplied, _ = sc.reference.Obj.(manifest.Data)
	}
	if supplied == nil {
		return
	}
	if len(sc.reference.Warnings) != 0 {
		f.Warnings = append(slices.Clone(f.Warnings), sc.reference.Warnings...)
	}
	if f.Data == nil {
		f.Data = supplied
	} else {
		var overridden []string
		supply := f.Data.WithSupplied(supplied)
		f.Data, overridden = supply.Data, supply.Overridden
		if len(overridden) > 0 && sc.reads != nil {
			sc.reads.RecordSuppliedOverride(
				SuppliedOverride{Resource: id, SupplyPath: sc.reference.InputPath, Keys: overridden},
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

		&referenceResolution{consumer: consumer, reads: r, reference: input.view.reference},
	)
}
