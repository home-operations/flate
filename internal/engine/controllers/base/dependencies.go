package base

import (
	"context"
	"errors"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

// RequireRefresh gates effective inputs against the execution's captured canonical declaration.
// Prepared clones MUST remain distinct from the canonical snapshot that produced them.
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
	var deps []manifest.DependencyRef
	if stage != manifest.GateParents {
		if capacity := effective.ReferenceCapacity().References; capacity != 0 {
			w := e.dependencyWaiter()
			deps = w.SelectGates(manifest.References(effective, manifest.ReferenceAll), stage, capacity)
		}
	}
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
	if len(deps) != 0 {
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
		Decision: e.selection.Snapshot(), Indexed: e.existence,
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

// Data MUST remain bound to this execution's input capture.
func (e *Execution) Data() *values.StoreProvider {
	if e.data == nil {
		e.data = values.NewStoreProvider(&e.Reads)
	}
	return e.data
}

// Probe MUST return its live assessment after capture starts, including on errors.
func (e *Execution) Probe(
	ctx context.Context,
	controller *Controller,
	object manifest.BaseManifest,
	previous schedule.Result,
) (History, []values.Result, error) {
	e.InitProbe(controller, object)
	history := History{Assessment: e}
	e.Take(previous)
	prior, _ := previous.Assessment.(*Execution)
	e.EntryReplay = controller.selection.EntryReplay(object.Named()) || prior != nil && prior.EntryReplay
	inputs, err := e.DataInputs(ctx, object, e.Data())
	if err != nil {
		return history, nil, err
	}
	var results []values.Result
	err = e.AwaitData(ctx, DataWait{
		Inputs: inputs,
		Resume: func(_ context.Context, _ manifest.DrainLevel, inputs []values.Result) error {
			results = inputs
			return nil
		},
	}, manifest.DrainNone)
	return history, results, err
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
	for ref := range depwait.DataReferences(manifest.References(obj, manifest.ReferenceRelations)) {
		input, err := e.readDataInput(ctx, ref.NamedResource, provider)
		if err != nil {
			return nil, err
		}
		assessment := values.Resolve(&values.ResolveRequest{Reference: ref, Facts: input, Permission: e.Missing})
		if assessment.UnreadableInput() {
			e.Unreadable = append(e.Unreadable, assessment.ID())
		}
		inputs = append(
			inputs,
			depwait.DataDependency{
				Reference: ref,
				Input:     input,
				Result:    assessment,
			},
		)
	}
	return inputs, nil
}

// DataWait resumes a captured execution after its values inputs settle.
type DataWait struct {
	Inputs []depwait.DataDependency
	// Resume retains authored positions until consumers filter their declarations.
	Resume func(context.Context, manifest.DrainLevel, []values.Result) error
}

// AwaitData retains availability separately from required authored readiness gates.
func (e *Execution) AwaitData(ctx context.Context, wait DataWait, level manifest.DrainLevel) error {
	return (&pendingDataError{
		execution:  e,
		resume:     wait.Resume,
		assessment: depwait.DataAssessment{Inputs: wait.Inputs},
	}).Settle(ctx, level)
}

type pendingDataError struct {
	execution  *Execution
	resume     func(context.Context, manifest.DrainLevel, []values.Result) error
	assessment depwait.DataAssessment
}

func (p *pendingDataError) Error() string { return "waiting for values inputs" }

func (p *pendingDataError) Unwrap() error {
	return &depwait.BlockedError{Deps: p.assessment.Blocked}
}

func (p *pendingDataError) Settle(ctx context.Context, level manifest.DrainLevel) error {
	e := p.execution
	w := e.dependencyWaiter()
	e.Unreadable = e.Unreadable[:0]
	err := p.assessment.Settle(ctx, &w, &e.Reads, level)
	for i := range p.assessment.Results {
		result := &p.assessment.Results[i]
		if result.UnreadableInput() {
			e.Unreadable = append(e.Unreadable, result.ID())
		}
		if !e.probe {
			result.ReportWarnings(&e.Reads)
		}
	}
	if err != nil {
		return err
	}
	if len(p.assessment.Blocked) != 0 {
		return p
	}
	return p.resume(ctx, level, p.assessment.Results)
}

func (e *Execution) readDataInput(
	ctx context.Context,
	id manifest.NamedResource,
	provider dataLookup,
) (store.ReferenceFacts, error) {
	if !e.probe && !e.selection.Snapshot().Output(manifest.NamedResource{}) {
		var proof store.ReadSet
		proof.Init(e.Store, e.Reads.ID(), nil)
		proof.SetWork(e.Reads.Work())
		defer proof.Retire()
		proof.RequireData(id)
		if _, err := e.KeepEmitted(
			ctx,
			manifest.NamedResource{},
			selection.PrepareRequest{Generation: nil, Reads: &proof, Preparation: &e.AdmissionPreparation},
		); err != nil {
			return store.ReferenceFacts{}, err
		}
	}
	if !e.probe && e.existence != nil {
		if _, err := e.existence.Promote(
			ctx,
			selection.PrepareRequest{Reads: &e.Reads, Preparation: &e.AdmissionPreparation},
			id,
		); err != nil {
			return store.ReferenceFacts{}, err
		}
	}
	input := provider.Lookup(id)
	if !e.probe || e.Reads.GetDeclaration(id) != nil {
		return input, nil
	}
	if e.existence != nil && e.existence.IsFileIndexed(id) {
		return store.ReferenceFacts{}, errIncompleteHistory
	}
	for i := range input.Productions {
		production := &input.Productions[i]
		if production.Declaration == (manifest.NamedResource{}) && production.Path != "" {
			return store.ReferenceFacts{}, errIncompleteHistory
		}
	}
	return input, nil
}
