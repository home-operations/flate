package depwait

import (
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
)

// SelectGates retains the authored order and applies the checkpoint's content mask.
func (w *Waiter) SelectGates(
	references iter.Seq[manifest.Reference], stage manifest.GateStage, capacity int,
) []manifest.DependencyRef {
	var deps []manifest.DependencyRef
	for ref := range references {
		if ref.Optional {
			continue
		}
		if stage == manifest.GateParents {
			if ref.Use != "materializer" {
				continue
			}
		} else if ref.Gate&stage == 0 {
			continue
		}
		if ref.Ordering && !w.Decision.Ordering(
			ref.NamedResource, w.Store.GetObject(ref.NamedResource) != nil,
			w.Indexed != nil && w.Indexed.IsFileIndexed(ref.NamedResource),
		) {
			continue
		}
		if deps == nil {
			deps = make([]manifest.DependencyRef, 0, capacity)
		}
		dep := ref.DependencyRef
		dep.Artifact = dep.Artifact && ref.ArtifactStages&stage != 0
		deps = append(deps, dep)
	}
	return deps
}

// DataReferences selects authored data inputs, including optional inputs with data policy.
func DataReferences(references iter.Seq[manifest.Reference]) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		for ref := range references {
			if _, data := ref.DataInput(); data && ref.Name != "" && !yield(ref) {
				return
			}
		}
	}
}
