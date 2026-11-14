package base

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
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
		result, err := values.Classify(&ref, &input)
		if err != nil {
			return nil, err
		}
		result.ReportWarnings(&e.Reads)
		disposition := result.Disposition(e.Missing)
		if disposition == values.Unresolved {
			e.Unreadable = append(e.Unreadable, ref.NamedResource)
		}
		if disposition == values.Unresolved ||
			ref.Use == "substituteFrom" && result.State() == values.Encrypted && disposition == values.UseData {
			e.unresolved = append(e.unresolved, depwait.DataDependency{Reference: ref, Input: input, Result: result})
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

// DataWait resumes a captured execution after its values inputs settle.
type DataWait struct {
	Inputs []depwait.DataDependency
	// Resume retains authored positions until consumers filter their declarations.
	Resume func(context.Context, manifest.DrainLevel, []depwait.DataDependency) error
}

// AwaitData retains availability separately from required authored readiness gates.
func (e *Execution) AwaitData(ctx context.Context, wait DataWait, level manifest.DrainLevel) error {
	return (&pendingDataError{execution: e, wait: wait}).Settle(ctx, level)
}

type pendingDataError struct {
	execution *Execution
	wait      DataWait
	blocked   []manifest.NamedResource
	omitted   []depwait.DataDependency
	failed    *manifest.DependencyFailedError
}

func (p *pendingDataError) Error() string { return "waiting for values inputs" }
func (p *pendingDataError) Unwrap() error { return &depwait.ErrBlocked{Deps: p.blocked} }
func (p *pendingDataError) Settle(ctx context.Context, level manifest.DrainLevel) error {
	e := p.execution
	gateErr := e.classify(ctx, level)

	w := e.dependencyWaiter()
	p.omitted = p.omitted[:0]
	p.failed, _ = errors.AsType[*manifest.DependencyFailedError](gateErr)
	p.blocked = p.blocked[:0]
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](gateErr); ok {
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
	if p.failed != nil {
		p.failed.Failed = compactDataIDs(p.failed.Failed)
		return p.failed
	}
	if gateErr != nil {
		return gateErr
	}
	for i := range e.unresolved {
		ref := &e.unresolved[i]
		cause := ""
		name := ref.Name
		var detail []string
		var related []manifest.NamedResource
		if ref.Result.State() != values.Absent {
			cause = ": " + ref.Result.Cause()
		}
		if keys := ref.Result.UnreadableKeys(); len(keys) != 0 {
			name = ref.NamespacedName()
			detail = keys[1:]
			related = []manifest.NamedResource{ref.NamedResource}
		}
		e.Reads.AddWarning(
			manifest.Warning{
				Category: manifest.WarnUnresolvedSubstitution,
				Related:  related,
				Detail:   detail,
				Message: fmt.Sprintf(
					"substituteFrom %s %q could not be read offline; its variables are unavailable, so their ${VAR} references may remain unsubstituted or resolve to defaults or empty values",
					ref.Kind,
					name,
				) + cause,
			},
		)
	}
	slices.SortFunc(p.omitted, func(a, b depwait.DataDependency) int {
		return cmp.Or(
			a.NamedResource.Compare(b.NamedResource),
			strings.Compare(a.Use, b.Use),
			cmp.Compare(a.Position, b.Position),
		)
	})
	return p.wait.Resume(ctx, level, p.omitted)
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
	failed *manifest.DependencyFailedError,
	causes []manifest.NamedResource,
	reason *manifest.DiagnosticReason,
	input *depwait.DataDependency,
) *manifest.DependencyFailedError {
	e := p.execution
	if failed == nil {
		failed = &manifest.DependencyFailedError{
			Parent:  e.Reads.ID(),
			Subject: manifest.DiagnosticFor(e.Reads.ID(), e.Reads.GetObject(e.Reads.ID())),
			Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
		}
	}
	if len(causes) == 0 {
		causes = []manifest.NamedResource{input.NamedResource}
	}
	for _, cause := range causes {
		failed.Failed = append(failed.Failed, cause)
		failed.Reasons[cause] = *reason
	}
	return failed
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
	if !input.Optional && !input.Input.Private && result.Result.State() != values.Readable &&
		result.Result.State() != values.Produced {
		for _, producer := range input.Input.Materializers {
			e.Reads.RequireData(producer)
		}
	}
	if !input.Optional && !input.Input.Private && result.Kind != schedule.Blocked && !result.Omit {
		e.Reads.RequireData(input.NamedResource)
	}
	omit := result.Omit
	switch result.Kind {
	case schedule.Blocked:
		p.blocked = append(p.blocked, result.Blocked...)
	case schedule.DependencyFailed:
		if input.Use == "valuesFrom" && len(result.Failed) == 0 {
			return result.Result.Failure()
		}
		p.failed = p.addFailure(p.failed, result.Failed, &result.Reason, input)
	case schedule.Terminal:
		if omit {
			input.Result = result.Result
			p.omitted = append(p.omitted, *input)
		}
	}
	return nil
}
