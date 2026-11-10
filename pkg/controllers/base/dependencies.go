package base

import (
	"context"
	"errors"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

// RequireRefresh gates effective inputs against the execution's captured canonical declaration.
// Prepared clones MUST remain distinct from the canonical snapshot that produced them.
//
//nolint:ireturn // The generic result retains the concrete manifest type supplied by the caller.
func (e *Execution) RequireRefresh[T manifest.BaseManifest](
	ctx context.Context,
	effective T,
	stage manifest.GateStage,
	level manifest.DrainLevel,
) (T, error) {
	canonical, _ := e.Reads.Get[T](e.Reads.ID())
	if err := e.Checkpoint(); err != nil {
		return canonical, err
	}
	deps := e.dependencies(effective, stage)
	for _, dep := range deps {
		e.Reads.Require(dep)
	}
	if err := e.requireDeclaration(ctx, level); err != nil {
		return canonical, err
	}
	if !e.prepared {
		if err := e.prepare(ctx, canonical); err != nil {
			return canonical, err
		}
	}
	if stage != manifest.GateParents && len(deps) != 0 {
		message := "resolving dependencies"
		if stage == manifest.GateArtifacts {
			message = "resolving source artifact"
		}
		e.SetPendingUnlessReady(e.Reads.ID(), message)
	}
	if err := e.Require(ctx, deps, level); err != nil {
		return canonical, err
	}
	return canonical, e.Checkpoint()
}

func (e *Execution) dependencies(obj manifest.BaseManifest, stage manifest.GateStage) []manifest.DependencyRef {
	if stage == manifest.GateParents {
		return nil
	}
	limits := obj.ReferenceCapacity()
	capacity := limits.References
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

var (
	errSuspended         = errors.New("reconcile suspended")
	errInputsChanged     = errors.New("execution inputs changed")
	errIncompleteHistory = errors.New("history requires normal input materialization")
)

// Checkpoint validates captured inputs and admission before execution continues.
func (e *Execution) Checkpoint() error {
	if !e.Reads.Valid() {
		return errInputsChanged
	}
	if obj := e.Reads.GetObject(e.Reads.ID()); obj != nil && e.preGate(obj) {
		return errSuspended
	}
	if message := e.Reads.Preflight(); message != "" {
		return manifest.NewError(manifest.ErrInput, nil, message)
	}
	return nil
}

type dataLookup interface {
	Lookup(manifest.NamedResource) store.ReferenceFacts
}

func (e *Execution) dependencyWaiter() depwait.Waiter {
	w := depwait.Waiter{
		Store: &e.Reads, Parent: e.Reads.ID(), Missing: e.Missing,
	}
	if !e.probe {
		w.Admission = selection.PrepareRequest{
			Reads:       &e.Reads,
			Preparation: &e.AdmissionPreparation,
			Missing:     e.Missing,
		}
		w.Existence = e.existence
	}
	return w
}

// DataInputs MUST assess promotion before capturing values for either render path.
func (e *Execution) DataInputs(
	ctx context.Context,
	obj manifest.BaseManifest,
	provider dataLookup,
) ([]depwait.DataDependency, error) {
	if message := e.Reads.Preflight(); message != "" {
		return nil, manifest.NewError(manifest.ErrInput, nil, message)
	}
	if obj.ReferenceCapacity().DataInputs == 0 {
		return nil, nil
	}
	if !e.probe {
		e.SetPendingUnlessReady(e.Reads.ID(), "resolving values inputs")
	}
	var inputs []depwait.DataDependency
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if (ref.Use != "valuesFrom" && ref.Use != "substituteFrom") || ref.Name == "" ||
			!ref.NamedResource.KindInfo().Data {
			continue
		}
		input, err := e.readDataInput(ctx, ref.NamedResource, provider)
		if err != nil {
			return nil, err
		}
		if _, unreadable, _ := e.Missing.Missing(ref, input); unreadable {
			e.Unreadable = append(e.Unreadable, ref.NamedResource)
			e.unresolved = append(e.unresolved, ref.NamedResource)
		}
		inputs = append(
			inputs,
			depwait.DataDependency{
				Reference: ref,
				Input:     input,
			},
		)
	}
	return inputs, nil
}
