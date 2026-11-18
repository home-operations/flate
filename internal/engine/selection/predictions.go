package selection

import (
	"cmp"
	"iter"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Predictions retains diagnostic membership without admitting declarations for execution.
func (s *Snapshot) Predictions() iter.Seq2[manifest.NamedResource, iter.Seq[manifest.Reference]] {
	if len(s.Failed) == 0 || s.Facts == nil {
		return func(func(manifest.NamedResource, iter.Seq[manifest.Reference]) bool) {}
	}
	return func(yield func(manifest.NamedResource, iter.Seq[manifest.Reference]) bool) {
		var reads store.ReadSet
		reads.Init(s.Store, manifest.BootstrapSourceID, nil)
		defer reads.Retire()
		for id := range s.Index.Files() {
			if !id.KindInfo().Discovery {
				continue
			}
			if _, known := s.Store.GetStatus(id); known {
				continue
			}
			evidence := s.Facts.RootEvidence(id, &reads, true)
			parent, _ := s.parentOf(id)
			parent = cmp.Or(parent, Root(evidence).Owner)
			if evidence.Scope.NativeOperand {
				parent = cmp.Or(parent, s.Entry.Producer)
			}
			failure, failed := s.Failed[parent]
			switch {
			case !failed:
				continue
			case parent.ReportHidden():
				// Synthetic entry parents have no emitted declaration to require.
			case s.Store.EmissionOwner(parent) == (manifest.NamedResource{}):
				continue
			case s.Facts.RootEvidence(parent, &reads, true).Occurrence.Declaration.File == "":
				continue
			}
			occurrence := evidence.Occurrence
			occurrence.Identity = id
			optional := parent.ReportHidden() || occurrence.Declaration.File == "" ||
				failure.Outcome() != store.OutcomeBlocked
			references := append(slices.Collect(occurrence.references()), manifest.Reference{
				NamedResource: parent, Use: "materializer", Gate: manifest.GateParents, Optional: optional,
			})
			if !yield(id, slices.Values(references)) {
				return
			}
		}
	}
}
