package base

import (
	"context"
	"errors"
	"fmt"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// RequireRefresh gates effective inputs against the execution's captured canonical declaration.
// Prepared clones MUST remain distinct from the canonical snapshot that produced them.
func (e *Execution) RequireRefresh[T manifest.BaseManifest](ctx context.Context, effective T, stage manifest.GateStage, level manifest.DrainLevel) (T, error) {
	canonical, _ := e.Reads.Get[T](e.Reads.ID())
	if err := e.checkpoint(); err != nil {
		return canonical, err
	}
	parents := e.parents(e.Reads.ID(), canonical)
	if len(parents) != 0 {
		e.SetPendingUnlessReady(e.Reads.ID(), "waiting for structural parents")
	}
	for _, dep := range parents {
		e.Reads.Require(dep)
	}
	parentErr := e.classify(ctx, level)
	if parentErr != nil {
		if _, failed := errors.AsType[*manifest.DependencyFailedError](parentErr); !failed {
			return canonical, parentErr
		}
	}
	message := "resolving dependencies"
	if stage == manifest.GateArtifacts {
		message = "resolving source artifact"
	}
	e.SetPendingUnlessReady(e.Reads.ID(), message)
	deps := e.dependencies(effective, stage)
	if err := e.Require(ctx, deps, level); err != nil {
		return canonical, err
	}
	return canonical, e.checkpoint()
}

func (e *Execution) parents(id manifest.NamedResource, obj manifest.BaseManifest) []manifest.DependencyRef {
	var deps []manifest.DependencyRef
	productions := e.Reads.Productions(id, e.index)
	structural := manifest.KindOf(obj).Structural
	for _, p := range productions {
		if p.Owner == (manifest.NamedResource{}) || p.Owner == id || structural && p.Path != "" {
			continue
		}
		deps = append(deps, manifest.DependencyRef{NamedResource: p.Owner})
	}
	if structural && len(deps) == 0 {
		if record, _ := e.index.Lookup(id); record.Parent != (manifest.NamedResource{}) && record.Parent != id {
			deps = append(deps, manifest.DependencyRef{NamedResource: record.Parent})
		}
	}
	return deps
}

func (e *Execution) dependencies(obj manifest.BaseManifest, stage manifest.GateStage) []manifest.DependencyRef {
	if stage == manifest.GateParents {
		return nil
	}
	capacity, _ := obj.ReferenceCapacity()
	if capacity == 0 {
		return nil
	}
	var deps []manifest.DependencyRef
	for ref := range manifest.References(obj, manifest.ReferenceAll) {
		if ref.Gate&stage == 0 || ref.Optional || ref.Ordering && !e.keepOrderingDep(ref.NamedResource) {
			continue
		}
		if deps == nil {
			deps = make([]manifest.DependencyRef, 0, capacity)
		}
		deps = append(deps, ref.DependencyRef)
		if ref.Artifact && ref.ArtifactStages&stage == 0 {
			deps[len(deps)-1].Artifact = false
		}
	}
	return deps
}

var errSuspended = errors.New("reconcile suspended")
var errInputsChanged = errors.New("execution inputs changed")

func (e *Execution) checkpoint() error {
	if !e.Reads.Valid() {
		return errInputsChanged
	}
	if obj := e.Reads.GetObject(e.Reads.ID()); obj != nil && e.preGate(obj) {
		return errSuspended
	}
	if message := e.Reads.Preflight(); message != "" {
		return fmt.Errorf("%w: %s", manifest.ErrInput, message)
	}
	return nil
}

type dataLookup interface {
	Lookup(manifest.NamedResource) values.Input
}

// DataInputs captures authored values and substitution inputs through the shared gate.
func (e *Execution) DataInputs(ctx context.Context, obj manifest.BaseManifest, provider dataLookup) []depwait.DataDependency {
	if _, inputs := obj.ReferenceCapacity(); inputs == 0 {
		return nil
	}
	e.SetPendingUnlessReady(e.Reads.ID(), "resolving values inputs")
	var inputs []depwait.DataDependency
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if (ref.Use != "valuesFrom" && ref.Use != "substituteFrom") || ref.Name == "" || !ref.NamedResource.KindInfo().Data {
			continue
		}
		if ref.Use != "substituteFrom" || e.SubstituteFrom[ref.NamedResource] == nil {
			e.PromoteData(ctx, ref.NamedResource)
		}
		input := provider.Lookup(ref.NamedResource)
		allowMissing := e.allowMissingSecrets
		if ref.Use == "substituteFrom" {
			allowMissing = ref.Kind == manifest.KindSecret
			if allowMissing && !ref.Optional && (input.Data == nil || !input.Data.HasKeys()) {
				if e.SubstituteFrom[ref.NamedResource] == nil {
					e.Unreadable = append(e.Unreadable, ref.NamedResource)
				}
				e.unresolved = append(e.unresolved, ref.NamedResource)
			}
		}
		inputs = append(inputs, depwait.DataDependency{DependencyRef: ref.DependencyRef, Input: input, Optional: ref.Optional, AllowMissing: allowMissing})
	}
	return inputs
}
