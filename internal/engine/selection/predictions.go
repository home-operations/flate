package selection

import (
	"cmp"
	"iter"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Predictions retains diagnostic membership without admitting declarations for execution.
func (s *Snapshot) Predictions() iter.Seq2[manifest.NamedResource, iter.Seq[manifest.Reference]] {
	if s.Facts == nil || len(s.Failed) == 0 && (s.Decision == nil || len(s.Decision.orderingDeclarations) == 0) {
		return func(func(manifest.NamedResource, iter.Seq[manifest.Reference]) bool) {}
	}
	return func(yield func(manifest.NamedResource, iter.Seq[manifest.Reference]) bool) {
		var reads store.ReadSet
		reads.Init(s.Store, manifest.BootstrapSourceID, nil)
		defer reads.Retire()
		if !s.yieldOrderingPredictions(&reads, yield) || len(s.Failed) == 0 {
			return
		}
		for id := range s.Index.Files() {
			references := s.predictionReferences(id, &reads)
			if len(references) != 0 && !yield(id, slices.Values(references)) {
				return
			}
		}
	}
}

func (s *Snapshot) yieldOrderingPredictions(
	reads *store.ReadSet, yield func(manifest.NamedResource, iter.Seq[manifest.Reference]) bool,
) bool {
	if s.Decision == nil {
		return true
	}
	for _, id := range slices.SortedFunc(maps.Keys(s.Decision.orderingDeclarations), manifest.NamedResource.Compare) {
		obj := s.Decision.OrderingDeclaration(reads, id, false)
		if obj == nil || obj.Suspended() {
			continue
		}
		if !s.Decision.EvaluationOnly(id) || !s.Decision.reportsFailure(id) || s.Failed[id].Status != "" {
			continue
		}
		refs := func(yield func(manifest.Reference) bool) {
			for ref := range manifest.References(obj, manifest.ReferenceAll) {
				if ref.Ordering && !yield(ref) {
					return
				}
			}
		}
		if !yield(id, refs) {
			return false
		}
	}
	return true
}

func (s *Snapshot) predictionReferences(id manifest.NamedResource, reads *store.ReadSet) []manifest.Reference {
	kind := id.KindInfo()
	if !kind.Discovery && !kind.Render {
		return nil
	}
	if _, known := s.Store.GetStatus(id); known {
		return nil
	}
	evidence := CaptureRoot(s.Facts.RootEvidence(id, reads), reads, s.Index)
	parent, _ := s.Store.Parent(id, s.Index)
	parent = cmp.Or(parent, Root(evidence).Owner)
	if evidence.Scope.NativeOperand {
		parent = cmp.Or(parent, s.Entry.Producer)
	}
	failure, failed := s.Failed[parent]
	hidden := parent.ReportHidden()
	switch {
	case !failed:
		return nil
	case !kind.Discovery && !hidden:
		// Visible producers own diagnostics for their unmaterialized render-only children.
		return nil
	case hidden:
		// Synthetic entry parents have no emitted declaration to require.
	case s.Store.EmissionOwner(parent) == (manifest.NamedResource{}):
		return nil
	case CaptureRoot(s.Facts.RootEvidence(parent, reads), reads, s.Index).Occurrence.Declaration.File == "":
		return nil
	}
	occurrence := evidence.Occurrence
	occurrence.Identity = id
	if occurrence.Object == nil && occurrence.Witness == nil {
		record, _ := s.Index.Lookup(id)
		occurrence.Object = record.Declaration
	}
	optional := hidden || occurrence.Declaration.File == "" ||
		failure.Outcome() != store.OutcomeBlocked
	var references []manifest.Reference
	occurrence.references(manifest.ReferenceAll, func(ref manifest.Reference) bool {
		references = append(references, ref)
		return true
	})
	return append(references, manifest.Reference{
		NamedResource: parent, Use: "materializer", Gate: manifest.GateParents, Optional: optional,
	})
}
