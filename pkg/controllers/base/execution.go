package base

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// Execution owns one reconciliation's captured inputs and retained assessment.
type Execution struct {
	*Controller
	Reads      store.ReadSet
	Unreadable []manifest.NamedResource
	Blocked    []manifest.NamedResource
	Reused     bool
	kind       schedule.Kind
	pending    error
	resume     func(context.Context, manifest.DrainLevel) error
	initial    store.StatusInfo
	recovering bool
}

// Init starts capture in caller-owned storage before declaration or dependency assessment.
// The execution MUST remain live until its registrations and output borrowers retire.
func (e *Execution) Init(c *Controller, id manifest.NamedResource) {
	if c.filterActive() {
		c.Store.SetIncluded(id, c.filter.ShouldReconcile(id))
	}
	if c.preflight != nil {
		message, failed := c.PreflightFailure(id)
		if !failed {
			message = ""
		}
		c.Store.SetPreflight(id, message)
	}
	e.Controller = c
	e.Reads.Init(c.Store, id, nil)
}

// Valid reports whether the assessment and its admitted output are current.
func (e *Execution) Valid() bool {
	if !e.Reads.Valid() {
		return false
	}
	if e.Outcome() == schedule.Blocked || !e.Reads.Admitted() {
		return true
	}
	switch output := e.Store.GetArtifact(e.Reads.ID()).(type) {
	case *store.Emission:
		return output.OutputReads.Valid()
	case *store.SourceArtifact:
		return output.Reads.Valid()
	}
	return true
}

// Retire releases superseded assessment cells while retaining borrowed output.
func (e *Execution) Retire() {
	switch output := e.Store.GetArtifact(e.Reads.ID()).(type) {
	case *store.Emission:
		if output.OutputReads == &e.Reads {
			return
		}
	case *store.SourceArtifact:
		if output.Reads == &e.Reads {
			return
		}
	}
	e.Reads.Retire()
}

// Emit publishes the candidate inventory with its consumed provenance.
func (e *Execution) Emit(id manifest.NamedResource, next *store.Emission) {
	e.Reads.Bind(next)
	next.Unreadable = e.Unreadable
	e.Controller.Emit(id, next)
}

// Require assesses and retains readiness gates from the captured inputs.
func (e *Execution) Require(ctx context.Context, deps []manifest.DependencyRef, level manifest.DrainLevel) error {
	for _, dep := range deps {
		e.Reads.Require(dep)
	}
	return e.classify(ctx, level)
}

func (e *Execution) classify(ctx context.Context, level manifest.DrainLevel) error {
	w := depwait.Waiter{Store: &e.Reads, Parent: e.Reads.ID(), Existence: e.existence, AllowMissingCRDs: e.allowMissingCRDs}
	var blocked []manifest.NamedResource
	var failed *manifest.DependencyFailedError
	for dep := range e.Reads.Gates() {
		switch cl := w.Classify(ctx, dep, level); cl.Kind {
		case schedule.DependencyFailed:
			if failed == nil {
				failed = &manifest.DependencyFailedError{Parent: e.Reads.ID(), Reasons: make(map[manifest.NamedResource]string)}
			}
			failed.Failed = append(failed.Failed, dep.NamedResource)
			if prior, exists := failed.Reasons[dep.NamedResource]; !exists || cl.Message < prior {
				failed.Reasons[dep.NamedResource] = cl.Message
			}
		case schedule.Blocked:
			blocked = append(blocked, dep.NamedResource)
		}
	}
	if len(blocked) > 0 {
		slices.SortFunc(blocked, manifest.NamedResource.Compare)
		return &depwait.ErrBlocked{Deps: slices.Compact(blocked)}
	}
	if failed != nil {
		slices.SortFunc(failed.Failed, manifest.NamedResource.Compare)
		failed.Failed = slices.Compact(failed.Failed)
		return failed
	}
	return nil
}

// Settle classifies retained unresolved gates without rendering again.
func (e *Execution) Settle(ctx context.Context, level manifest.DrainLevel) {
	if pending, ok := e.pending.(interface {
		Settle(context.Context, manifest.DrainLevel) error
	}); ok {
		e.finish(pending.Settle(ctx, level))
	} else if e.resume != nil {
		e.finish(e.resume(ctx, level))
	} else {
		e.finish(e.classify(ctx, level))
	}
}

func (e *Execution) finish(err error) {
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
		e.Blocked = blocked.Deps
		e.kind = schedule.Blocked
	}
	if !e.Reads.Valid() {
		return
	}
	defer e.Reads.Accept()
	id := e.Reads.ID()
	e.Blocked = nil
	e.kind = schedule.Terminal
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
		e.pending = err
		e.kind = schedule.Blocked
		e.Blocked = blocked.Deps
		return
	}
	if err != nil {
		if _, failed := errors.AsType[*manifest.DependencyFailedError](err); failed {
			e.kind = schedule.DependencyFailed
		}
		if artifact, ok := e.Store.GetArtifact(id).(*store.SourceArtifact); ok && !artifact.Reads.Valid() {
			e.Store.SetArtifact(id, nil)
		}
		_, next := e.Reads.GetEmission(id)
		if e.Store.HasInventory(id) {
			next.Complete = !next.OutputReads.Valid()
			e.Emit(id, &next)
		}
		next.Finish()
		if errors.Is(err, manifest.ErrSourceSkipped) {
			e.Store.UpdateStatus(id, store.StatusReady, store.ReasonSkipped, strings.TrimSpace(manifest.TrimSentinelPrefix(err.Error())))
		} else {
			e.Store.UpdateFailure(id, err)
		}
		return
	}
	if prior, ok := e.Store.GetStatus(id); ok && (prior.Status == store.StatusFailed && (!e.recovering || prior != e.initial) || prior.Status == store.StatusReady && (prior.Message != "" || prior.Outcome() != store.OutcomePassed)) {
		return
	}
	e.Store.UpdateStatus(id, store.StatusReady, "", "")
}

// Dispatch runs a captured declaration through the common acceptance path.
func (e *Execution) Dispatch[T manifest.BaseManifest](ctx context.Context, level manifest.DrainLevel, reconcile func(context.Context, T, manifest.DrainLevel) error) {
	id := e.Reads.ID()
	defer func() {
		if recovered := recover(); recovered != nil {
			e.Reads.Retire()
			e.log.Error("panic during reconcile", "id", id.String(), "panic", recovered)
			e.Store.UpdateStatus(id, store.StatusFailed, "", fmt.Sprintf("panic: %v", recovered))
			panic(recovered)
		}
	}()
	obj, ok := e.Reads.Get[T](id)
	if !ok {
		if info, exists := e.Store.GetStatus(id); exists && info.Status != store.StatusReady {
			e.Store.UpdateStatus(id, store.StatusReady, store.ReasonSkipped, "object not in store at reconcile time")
		}
		return
	}
	if e.preGate(obj) {
		e.withdrawInvalid()
		e.Reads.Accept()
		return
	}
	if !e.Reads.Admitted() {
		e.Store.UpdateStatus(id, store.StatusReady, store.ReasonUnchanged, "unchanged")
		e.withdrawInvalid()
		e.Reads.Accept()
		return
	}
	if !e.Reads.Valid() {
		return
	}
	if message := e.Reads.Preflight(); message != "" {
		e.finish(fmt.Errorf("%w: %s", manifest.ErrInput, message))
		return
	}
	e.initial, _ = e.Store.GetStatus(id)
	e.recovering = e.initial.IsDependencyFailure() || e.initial.Status == store.StatusFailed && e.Reads.ReplacesInvalid()
	err := reconcile(ctx, obj, level)
	if _, blocked := errors.AsType[*depwait.ErrBlocked](err); blocked {
		e.resume = func(ctx context.Context, level manifest.DrainLevel) error { return reconcile(ctx, obj, level) }
	}
	e.finish(err)
}

// KeepOrderingDep retains changed or unresolved authored prerequisites.
func (e *Execution) KeepOrderingDep(id manifest.NamedResource) bool {
	return !e.filterActive() || e.filter.ShouldReconcile(id) || (e.existence == nil || !e.existence.IsFileIndexed(id)) && e.Reads.GetObject(id) == nil
}

// SourceSkipped propagates a captured source's whole-source skip.
func (e *Execution) SourceSkipped(id manifest.NamedResource, label string) error {
	if info, ok := e.Reads.GetStatus(id); ok && info.Outcome() == store.OutcomeSkipped {
		return fmt.Errorf("%w: %s %s skipped: %s", manifest.ErrSourceSkipped, label, id.String(), info.Message)
	}
	return nil
}

// FingerprintDedup derives the replay key from content and reuses accepted children.
// Nil or unmarshalable content MUST bypass replay; preflight failures still win.
func (e *Execution) FingerprintDedup(id manifest.NamedResource, content any) (string, bool, error) {
	if content == nil {
		return "", false, nil
	}
	fp := manifest.Fingerprint(content)
	if fp == "" {
		return "", false, nil
	}
	current, _ := e.Store.GetArtifact(id).(*store.Emission)
	output, matched := current.Replay(fp)
	var attempt store.Emission
	bound := false
	if !matched {
		_, attempt = e.Reads.GetEmission(id)
		defer attempt.Finish()
		bound = true
		output, matched = attempt.Replay(fp)
	}
	if !matched {
		return fp, false, nil
	}
	if err := e.PreflightError(id); err != nil {
		return fp, true, err
	}
	e.Reused = true
	if slices.Equal(output.Unreadable, e.Unreadable) {
		e.Reads.CarryWarnings(output)
	}
	if !bound && current.Complete && current.Fingerprint == fp && current.OutputReads.Valid() && slices.Equal(output.Unreadable, e.Unreadable) {
		for _, child := range output.Children {
			e.KeepEmitted(id, child.Obj, &e.Reads)
		}
		return fp, true, nil
	}
	if !bound {
		_, attempt = e.Reads.GetEmission(id)
		defer attempt.Finish()
	}
	attempt.Children = output.Children
	attempt.Manifests = output.Manifests
	attempt.Fingerprint = output.Fingerprint
	attempt.Unreadable = e.Unreadable
	attempt.Render = output.Render
	attempt.Complete = true
	for _, child := range output.Children {
		e.KeepEmitted(id, child.Obj, &e.Reads)
	}
	e.Emit(id, &attempt)
	return fp, true, nil
}

// Outcome returns the retained readiness decision without reading the store.
func (e *Execution) Outcome() schedule.Kind { return e.kind }

// Progressed reports an execution that performed work beyond equal-input replay.
func (e *Execution) Progressed() bool { return !e.Reused }

// Close retires all provenance borrowed by the completed invocation.
func (e *Execution) Close() { e.Reads.Close() }

func (e *Execution) withdrawInvalid() {
	_, next := e.Reads.GetEmission(e.Reads.ID())
	if e.Store.HasInventory(e.Reads.ID()) && !next.OutputReads.Valid() {
		next.Complete = true
		e.Emit(e.Reads.ID(), &next)
	}
	next.Finish()
}

// PreflightError reports the execution's captured structural admission decision.
func (e *Execution) PreflightError(_ manifest.NamedResource) error {
	if !e.Reads.Valid() {
		return errors.New("execution inputs changed")
	}
	if message := e.Reads.Preflight(); message != "" {
		return errors.New(message)
	}
	return nil
}

// DataWait resumes a captured execution after its values inputs settle.
type DataWait struct {
	Inputs []depwait.DataDependency
	Resume func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error
}

// AwaitData retains availability separately from required authored readiness gates.
func (e *Execution) AwaitData(ctx context.Context, wait DataWait, level manifest.DrainLevel) error {
	for _, input := range wait.Inputs {
		if !input.Optional && !input.Input.Private {
			for _, producer := range input.Input.Materializers {
				e.Reads.RequireData(producer)
			}
		}
	}
	return (&pendingData{execution: e, wait: wait}).Settle(ctx, level)
}

type pendingData struct {
	execution *Execution
	wait      DataWait
	blocked   []manifest.NamedResource
}

func (p *pendingData) Error() string { return "waiting for values inputs" }
func (p *pendingData) Unwrap() error { return &depwait.ErrBlocked{Deps: p.blocked} }
func (p *pendingData) Settle(ctx context.Context, level manifest.DrainLevel) error {
	e := p.execution
	gateErr := e.classify(ctx, level)

	w := depwait.Waiter{Store: &e.Reads, Parent: e.Reads.ID(), Existence: e.existence, AllowMissingCRDs: e.allowMissingCRDs}
	var omitted []manifest.NamedResource
	failed, _ := errors.AsType[*manifest.DependencyFailedError](gateErr)
	p.blocked = p.blocked[:0]
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](gateErr); ok {
		p.blocked = append(p.blocked, blocked.Deps...)
	}
	for _, input := range p.wait.Inputs {
		result := w.Available(ctx, input, level)
		if !input.Optional && !input.Input.Private && result.Kind != schedule.Blocked && !result.Omit {
			e.Reads.RequireData(input.NamedResource)
		}
		switch result.Kind {
		case schedule.Blocked:
			p.blocked = append(p.blocked, result.Blocked...)
		case schedule.DependencyFailed:
			if failed == nil {
				failed = &manifest.DependencyFailedError{Parent: e.Reads.ID(), Reasons: make(map[manifest.NamedResource]string)}
			}
			causes := result.Failed
			if len(causes) == 0 {
				causes = []manifest.NamedResource{input.NamedResource}
			}
			for _, cause := range causes {
				failed.Failed = append(failed.Failed, cause)
				failed.Reasons[cause] = result.Message
			}
		default:
			if result.Omit && !input.Optional {
				omitted = append(omitted, input.NamedResource)
			}
		}
	}
	if len(p.blocked) != 0 {
		p.blocked = compactDataIDs(p.blocked)
		return p
	}
	if failed != nil {
		failed.Failed = compactDataIDs(failed.Failed)
		return failed
	}
	return p.wait.Resume(ctx, level, compactDataIDs(omitted))
}

func compactDataIDs(ids []manifest.NamedResource) []manifest.NamedResource {
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return slices.Compact(ids)
}

// PromoteData admits a file-indexed input before recording its semantic value.
func (e *Execution) PromoteData(ctx context.Context, id manifest.NamedResource) {
	if e.existence != nil {
		e.existence.Promote(ctx, &e.Reads, id)
	}
}
