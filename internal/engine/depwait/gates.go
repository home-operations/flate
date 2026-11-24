package depwait

import (
	"iter"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// SelectGates retains the authored order and applies the checkpoint's content mask.
func SelectGates(
	references iter.Seq[manifest.Reference], stage manifest.GateStage, capacity int, reads *store.ReadSet,
) []manifest.DependencyRef {
	if reads != nil && stage == manifest.GateInputs {
		reads.AssessOrdering()
	}
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
		if deps == nil {
			deps = make([]manifest.DependencyRef, 0, capacity)
		}
		if reads != nil && ref.Ordering {
			edge := store.OrderingEdgeFor(&ref.DependencyRef)
			reads.RequireOrdering(&edge)
		}
		dep := ref.DependencyRef
		dep.RequiresDeclaration = dep.RequiresDeclaration || ref.Ordering
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

// OrderingGates selects the readiness edges from an effective declaration.
func OrderingGates(obj manifest.BaseManifest, reads *store.ReadSet) []manifest.DependencyRef {
	references := func(yield func(manifest.Reference) bool) {
		for ref := range manifest.References(obj, manifest.ReferenceAll) {
			if ref.Ordering && !yield(ref) {
				return
			}
		}
	}
	return SelectGates(references, manifest.GateInputs, obj.ReferenceCapacity().References, reads)
}
