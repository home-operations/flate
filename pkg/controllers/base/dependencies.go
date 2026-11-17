package base

import (
	"context"
	"errors"
	"slices"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
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
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if (ref.Use != "valuesFrom" && ref.Use != "substituteFrom") || ref.Name == "" {
			continue
		}
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
	return (&pendingDataError{execution: e, wait: wait}).Settle(ctx, level)
}

type pendingDataError struct {
	execution *Execution
	wait      DataWait
	blocked   []manifest.NamedResource
	results   []values.Result
	// failed merges diagnostics; causes preserves original error classifications.
	// A sole non-absence cause MUST retain its error shape.
	// The aggregate MUST precede causes so errors.AsType finds every failed input.
	failed *manifest.DependencyFailedError
	causes []error
}

func (p *pendingDataError) Error() string { return "waiting for values inputs" }
func (p *pendingDataError) Unwrap() error { return &depwait.BlockedError{Deps: p.blocked} }
func (p *pendingDataError) Settle(ctx context.Context, level manifest.DrainLevel) error {
	e := p.execution
	gateErr := e.classify(ctx, level)

	w := e.dependencyWaiter()
	p.results = p.results[:0]
	p.causes = p.causes[:0]
	e.Unreadable = e.Unreadable[:0]
	p.failed, _ = errors.AsType[*manifest.DependencyFailedError](gateErr)
	p.blocked = p.blocked[:0]
	if blocked, ok := errors.AsType[*depwait.BlockedError](gateErr); ok {
		p.blocked = append(p.blocked, blocked.Deps...)
	}
	for i := range p.wait.Inputs {
		if err := p.settleInput(ctx, &w, &p.wait.Inputs[i], level); err != nil {
			return err
		}
	}
	if len(p.blocked) != 0 {
		p.blocked = compactDataIDs(p.blocked)
		return p
	}
	if len(p.causes) == 1 && !errors.Is(p.causes[0], manifest.ErrObjectNotFound) {
		return p.causes[0]
	}
	if p.failed != nil {
		p.failed.Failed = compactDataIDs(p.failed.Failed)
		if len(p.causes) == 0 {
			return p.failed
		}
		return manifest.NewError(manifest.ErrInput, errors.Join(p.failed, errors.Join(p.causes...)), p.failed.Error())
	}
	if gateErr != nil {
		return gateErr
	}
	return p.wait.Resume(ctx, level, p.results)
}

func compactDataIDs(ids []manifest.NamedResource) []manifest.NamedResource {
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return slices.Compact(ids)
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

func (p *pendingDataError) addFailure(
	causes []manifest.NamedResource,
	reason *manifest.DiagnosticReason,
	err error,
) {
	e := p.execution
	if p.failed == nil {
		p.failed = &manifest.DependencyFailedError{
			Parent:  e.Reads.ID(),
			Subject: manifest.DiagnosticFor(e.Reads.ID(), e.Reads.GetObject(e.Reads.ID())),
			Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
		}
	}
	if len(causes) == 0 {
		causes = []manifest.NamedResource{reason.Subject.ID}
	}
	for _, cause := range causes {
		captured := *reason
		if cause != reason.Subject.ID {
			for i := range reason.Causes {
				if reason.Causes[i].Subject.ID == cause {
					captured = reason.Causes[i]
					break
				}
			}
		}
		previous := p.failed.Reasons[cause]
		captured.Causes = append(slices.Clone(previous.Causes), captured.Causes...)
		slices.SortFunc(captured.Causes, manifest.DiagnosticReason.Compare)
		captured.Causes = slices.CompactFunc(captured.Causes, func(a, b manifest.DiagnosticReason) bool {
			return a.Compare(b) == 0
		})
		p.failed.Failed = append(p.failed.Failed, cause)
		p.failed.Reasons[cause] = captured
	}
	p.causes = append(p.causes, err)
}

func (p *pendingDataError) settleInput(
	ctx context.Context,
	w *depwait.Waiter,
	input *depwait.DataDependency,
	level manifest.DrainLevel,
) error {
	e := p.execution
	result := w.Available(ctx, *input, level)
	if result.Err != nil {
		return result.Err
	}
	if result.Result.RequiresData() {
		if !result.Result.Materialized() {
			for _, producer := range input.Input.Materializers {
				e.Reads.RequireData(producer)
			}
		}
		e.Reads.RequireData(input.NamedResource)
	}
	switch result.Kind {
	case schedule.Blocked:
		p.blocked = append(p.blocked, result.Blocked...)
	case schedule.DependencyFailed:
		if err := result.Result.MaterialError(); err != nil {
			return err
		}
		p.addFailure(result.Failed, &result.Reason, result.Result.Err())
	case schedule.Terminal:
		if result.Result.UnreadableInput() {
			e.Unreadable = append(e.Unreadable, result.Result.ID())
		}
		input.Result = result.Result
		if !e.probe {
			result.Result.ReportWarnings(&e.Reads)
		}
		p.results = append(p.results, result.Result)
	}
	return nil
}
