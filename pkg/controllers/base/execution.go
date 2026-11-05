package base

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	unresolved []manifest.NamedResource
	recovering bool
	generated  []store.Child
	prepared   bool
}

// Init starts capture in caller-owned storage before declaration or dependency assessment.
// The execution MUST remain live until its registrations and output borrowers retire.
func (e *Execution) Init(c *Controller, id manifest.NamedResource) {
	if c.filterActive() {
		c.Store.SetIncluded(id, c.filter.ShouldReconcile(id))
	}
	if c.preflight != nil {
		message, failed := c.preflight(id)
		if !failed {
			message = ""
		}
		c.Store.SetPreflight(id, message)
	}
	e.Controller = c
	e.Reads.Init(c.Store, id, nil)
	e.Reads.BeginExecution(nil)
}

// Valid reports whether the assessment and its admitted output are current.
func (e *Execution) Valid() bool {
	if !e.Reads.Valid() {
		return false
	}
	if e.Outcome() == schedule.Blocked || !e.Reads.Admitted() {
		return true
	}
	if output, ok := e.Store.GetArtifact(e.Reads.ID(), store.ArtifactContent).(*store.SourceArtifact); ok &&
		!output.Reads.Valid() {
		return false
	}
	_, _, inventory := e.Store.GetEmission(e.Reads.ID())
	defer inventory.Finish()
	return inventory.OutputReads.Valid()
}

// Retire releases superseded assessment cells while retaining borrowed output.
func (e *Execution) Retire() {
	e.closeAcquisition()
	e.Store.Retire(&e.Reads)
}

// Emit publishes the candidate inventory with its consumed provenance.
func (e *Execution) Emit(
	ctx context.Context,
	id manifest.NamedResource,
	next *store.Emission,
	payloads map[manifest.NamedResource]*store.SourceArtifact,
) error {
	if err := e.Checkpoint(); err != nil {
		return err
	}
	return e.publish(ctx, id, next, payloads)
}

func (e *Execution) bindEmission(next *store.Emission) {
	borrowed := next.Reads
	next.Children = e.generatedChildren(next.Children)
	e.Reads.Bind(next)
	if e.Reused && borrowed.Valid() && next.Complete && len(next.Failures) == 0 {
		next.Reads = borrowed
	}
	next.Unreadable = e.Unreadable
}

func (e *Execution) acceptEmission(next *store.Emission) {
	next.Reads = &e.Reads
	if next.Complete && e.Reads.Valid() {
		e.recovering = true
	}
}

func (e *Execution) publish(
	ctx context.Context,
	id manifest.NamedResource,
	next *store.Emission,
	payloads map[manifest.NamedResource]*store.SourceArtifact,
) error {
	e.bindEmission(next)
	if err := e.emit(ctx, id, next, payloads); err != nil {
		return err
	}
	e.acceptEmission(next)
	return manifest.DocumentFailure(next.Failures)
}

// Require assesses and retains readiness gates from the captured inputs.
func (e *Execution) Require(ctx context.Context, deps []manifest.DependencyRef, level manifest.DrainLevel) error {
	admit := false
	for _, dep := range deps {
		e.Reads.Require(dep)
		admit = admit || e.filterActive() && !e.filter.ShouldReconcile(dep.NamedResource)
	}
	if admit {
		if _, err := e.KeepEmitted(ctx, manifest.NamedResource{}, store.Child{}, &e.Reads); err != nil {
			return err
		}
	}
	return e.classify(ctx, level)
}

func (e *Execution) classify(ctx context.Context, level manifest.DrainLevel) error {
	w := depwait.Waiter{
		Store:            &e.Reads,
		Parent:           e.Reads.ID(),
		Existence:        e.existence,
		AllowMissingCRDs: e.allowMissingCRDs,
	}
	var blocked []manifest.NamedResource
	var failed *manifest.DependencyFailedError
	var cause error
	var causeID manifest.NamedResource
	for dep := range e.Reads.Gates() {
		w.Materializer = dep.Materializer
		cl := w.Classify(ctx, dep.DependencyRef, level)
		if cl.Err != nil {
			if cause == nil || dep.Compare(causeID) < 0 {
				cause, causeID = cl.Err, dep.NamedResource
			}
			continue
		}
		switch cl.Kind {
		case schedule.Terminal:
		case schedule.DependencyFailed:
			if failed == nil {
				failed = &manifest.DependencyFailedError{
					Parent:  e.Reads.ID(),
					Subject: manifest.DiagnosticFor(e.Reads.ID(), e.Reads.GetObject(e.Reads.ID())),
					Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
				}
			}
			failed.Failed = append(failed.Failed, dep.NamedResource)
			reason := cl.DiagnosticReason(dep.NamedResource)
			if prior, exists := failed.Reasons[dep.NamedResource]; !exists || reason.Compare(prior) < 0 {
				failed.Reasons[dep.NamedResource] = reason
			}
		case schedule.Blocked:
			blocked = append(blocked, dep.NamedResource)
		}
	}
	if len(blocked) > 0 {
		slices.SortFunc(blocked, manifest.NamedResource.Compare)
		return &depwait.ErrBlocked{Deps: slices.Compact(blocked)}
	}
	if cause != nil {
		return cause
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
	if !e.Reads.Valid() {
		e.closeAcquisition()
		return
	}
	if pending, ok := e.pending.(interface {
		Settle(context.Context, manifest.DrainLevel) error
	}); ok {
		e.finish(ctx, pending.Settle(ctx, level))
	} else if e.resume != nil {
		e.finish(ctx, e.resume(ctx, level))
	} else {
		e.finish(ctx, e.classify(ctx, level))
	}
}

func (e *Execution) finish(ctx context.Context, err error) {
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
		e.Blocked = blocked.Deps
		e.kind = schedule.Blocked
	} else {
		e.pending = nil
		e.resume = nil
	}
	if !e.Reads.Valid() {
		return
	}
	kind := store.AssessmentReconciled
	if e.Reused {
		kind = store.AssessmentReused
	}
	defer e.Reads.Accept(kind)
	id := e.Reads.ID()
	e.Blocked = nil
	e.kind = schedule.Terminal
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
		e.pending = err
		e.kind = schedule.Blocked
		e.Blocked = blocked.Deps
		return
	}
	if errors.Is(err, errSuspended) {
		return
	}
	if err != nil {
		e.finishFailure(ctx, id, err)
		return
	}
	if prior, ok := e.Store.GetStatus(
		id,
	); ok &&
		(prior.Status == store.StatusFailed && (!e.recovering || prior != e.initial) || prior.Status == store.StatusReady && (prior.Message != "" || prior.Outcome() != store.OutcomePassed)) {
		return
	}
	e.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
}

func (e *Execution) finishFailure(ctx context.Context, id manifest.NamedResource, err error) {
	if _, failed := errors.AsType[*manifest.DependencyFailedError](err); failed {
		e.kind = schedule.DependencyFailed
	}
	if artifact, ok := e.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact); ok &&
		(errors.Is(err, manifest.ErrSourceSkipped) || !artifact.Reads.Valid()) {
		e.Store.SetArtifact(id, nil)
	}
	_, _, next := e.Reads.GetEmission(id)
	if e.Store.HasInventory(id) && len(next.Failures) == 0 {
		next.Complete = !next.OutputReads.Valid()
		if publishErr := e.publish(ctx, id, &next, nil); publishErr != nil {
			err = errors.Join(err, publishErr)
		}
	}
	next.Finish()
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(
			ctx,
			"base: resource failed",
			"id",
			manifest.DiagnosticFor(id, e.Reads.GetObject(id)).String(),
			"err",
			manifest.ErrorDetails(err),
		)
	}
	if errors.Is(err, manifest.ErrSourceSkipped) {
		e.Store.UpdateStatus(
			id,
			store.StatusInfo{
				Status:  store.StatusReady,
				Reason:  store.ReasonSkipped,
				Message: strings.TrimSpace(err.Error()),
			},
		)
	} else {
		e.Store.UpdateFailure(id, err)
	}
}

// Dispatch runs a captured declaration through the common acceptance path.
func (e *Execution) Dispatch[T manifest.BaseManifest, S any](
	ctx context.Context,
	level manifest.DrainLevel,
	state S,
	reconcile func(S, context.Context, T, manifest.DrainLevel) error,
) {
	id := e.Reads.ID()
	defer func() {
		if recovered := recover(); recovered != nil {
			subject := manifest.DiagnosticFor(id, e.Reads.GetObject(id))
			e.Reads.Retire()
			slog.Error("base: panic during reconcile", "id", subject.String(), "panic", recovered)
			e.Store.UpdateStatus(
				id,
				store.StatusInfo{Status: store.StatusFailed, Message: fmt.Sprintf("panic: %v", recovered)},
			)
			panic(recovered)
		}
	}()
	obj, ok := e.Reads.Get[T](id)
	policy := e.Reads.Policy(id)
	if ok && e.filterActive() {
		if _, err := e.KeepEmitted(
			ctx,
			manifest.NamedResource{},
			store.Child{Obj: obj, Origin: policy.Origin},
			&e.Reads,
		); err != nil {
			e.finish(ctx, err)
			return
		}
	}

	if ok && (!policy.Root && !policy.Reconcile || e.preGate(obj)) {
		e.acceptReads(ctx)
		return
	}
	if err := e.Checkpoint(); err != nil {
		e.finish(ctx, err)
		return
	}
	if !e.Reads.Admitted() {
		if ok {
			for dep := range manifest.ContentInputs(obj, false) {
				e.Reads.GetObject(dep)
			}
		}
		e.Store.UpdateStatus(
			id,
			store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"},
		)
		e.acceptReads(ctx)
		return
	}
	if !ok {
		e.dispatchMissing(ctx, level)
		return
	}

	e.initial, _ = e.Store.GetStatus(id)
	e.recovering = e.initial.IsDependencyFailure() ||
		e.initial.Status == store.StatusFailed && e.Reads.ReplacesInvalid()
	err := e.reconcile(ctx, obj, level, func(ctx context.Context, obj T, level manifest.DrainLevel) error {
		return reconcile(state, ctx, obj, level)
	})
	if _, blocked := errors.AsType[*depwait.ErrBlocked](err); blocked {
		e.resume = func(ctx context.Context, level manifest.DrainLevel) error {
			return e.reconcile(ctx, obj, level, func(ctx context.Context, obj T, level manifest.DrainLevel) error {
				return reconcile(state, ctx, obj, level)
			})
		}
	}
	e.finish(ctx, err)
}

func (e *Execution) acceptReads(ctx context.Context) {
	if err := e.withdrawInvalid(ctx); err != nil {
		e.finish(ctx, err)
		return
	}
	e.Reads.Accept(store.AssessmentReconciled)
}

func (e *Execution) reconcile[T manifest.BaseManifest](
	ctx context.Context,
	obj T,
	level manifest.DrainLevel,
	reconcile func(context.Context, T, manifest.DrainLevel) error,
) error {
	if !e.Reads.Valid() {
		return nil
	}
	return reconcile(ctx, obj, level)
}

func (e *Execution) prepare(ctx context.Context, obj manifest.BaseManifest) (resultErr error) {
	e.prepared = true
	objects, err := manifest.GeneratedObjects(obj)
	if err == nil && len(objects) == 0 && !e.Store.HasGeneratedObjects(e.Reads.ID()) {
		return nil
	}
	work := e.Reads.Work().Scope().Record(manifest.WorkPreparation, "")
	work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	defer func() { work.Finish(resultErr) }()
	if err != nil {
		return err
	}
	for _, object := range objects {
		e.generated = append(e.generated, store.Child{Obj: object, Generated: true})
	}
	_, _, inventory := e.Reads.GetEmission(e.Reads.ID())
	defer inventory.Finish()
	inventory.Children, inventory.Manifests, inventory.Complete = e.generated, nil, false
	return e.Emit(ctx, e.Reads.ID(), &inventory, nil)
}

func (e *Execution) generatedChildren(children []store.Child) []store.Child {
	if len(children) == 0 {
		return e.generated
	}
	if len(e.generated) == 0 &&
		!slices.ContainsFunc(children, func(child store.Child) bool { return child.Generated }) {
		return children
	}
	current := make([]store.Child, 0, len(children)+len(e.generated))
	for _, child := range children {
		if !child.Generated {
			current = append(current, child)
		}
	}
	return append(current, e.generated...)
}

func (e *Execution) requireDeclaration(ctx context.Context, level manifest.DrainLevel) error {
	id := e.Reads.ID()
	if e.Reads.GetDeclaration(id) != nil {
		return nil
	}
	var owners []manifest.NamedResource
	{
		productionValues := e.Reads.Productions(id, nil)
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]

			if owner := e.Reads.Materializer(id, production); owner != (manifest.NamedResource{}) && owner != id {
				owners = append(owners, owner)
			}
		}
	}
	slices.SortFunc(owners, manifest.NamedResource.Compare)
	owners = slices.Compact(owners)
	if len(owners) == 0 {
		return manifest.NewError(manifest.ErrSourceSkipped, nil, "object not in store at reconcile time")
	}
	for _, owner := range owners {
		e.Reads.RequireMaterializer(owner)
	}
	err := e.classify(ctx, level)
	if _, blocked := errors.AsType[*depwait.ErrBlocked](err); blocked {
		e.SetPendingUnlessReady(id, "waiting for declaration")
		return err
	}
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	if !ok && err != nil {
		return err
	}
	if !ok {
		failed = &manifest.DependencyFailedError{
			Parent:  id,
			Subject: manifest.DiagnosticFor(id, e.Reads.GetObject(id)),
			Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
		}
	}
	failed.Failed = append(failed.Failed, owners...)
	slices.SortFunc(failed.Failed, manifest.NamedResource.Compare)
	failed.Failed = slices.Compact(failed.Failed)
	for _, owner := range owners {
		if _, exists := failed.Reasons[owner]; !exists {
			failed.Reasons[owner] = manifest.DiagnosticReason{
				Subject: manifest.DiagnosticFor(owner, e.Reads.GetObject(owner)),
				Message: "declaration not emitted",
			}
		}
	}
	return failed
}

func (e *Execution) keepOrderingDep(id manifest.NamedResource) bool {
	return !e.filterActive() || e.filter.ShouldReconcile(id) ||
		(e.existence == nil || !e.existence.IsFileIndexed(id)) && e.Reads.GetObject(id) == nil
}

// Deduplication omits its fingerprint when no stable content key exists.
type Deduplication struct {
	Fingerprint string
	Reused      bool
}

// FingerprintDedup derives the replay key from content and reuses accepted children.
// Nil or unmarshalable content MUST bypass replay; preflight failures still win.
func (e *Execution) FingerprintDedup(
	ctx context.Context,
	id manifest.NamedResource,
	content any,
	attempt *store.Emission,
) (Deduplication, error) {
	if err := ctx.Err(); err != nil {
		return Deduplication{Fingerprint: "", Reused: true}, fmt.Errorf("context: %w", err)
	}
	if err := e.Checkpoint(); err != nil {
		return Deduplication{Fingerprint: "", Reused: true}, err
	}
	if e.filter != nil {
		membershipResult1 := e.filter.Membership(id)
		_, selected, dirty, _ := membershipResult1.Needed, membershipResult1.Selected, membershipResult1.Dirty, membershipResult1.Revision
		if selected && !dirty {
			e.Reused = true
			e.Store.UpdateStatus(
				id,
				store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"},
			)
			return Deduplication{Fingerprint: "", Reused: true}, nil
		}
	}
	if content == nil {
		return Deduplication{Fingerprint: "", Reused: false}, e.admitRender()
	}
	fp := manifest.Fingerprint(content)
	e.Reads.Work().Scope().Record(manifest.WorkPreparation, fp).Add(&manifest.WorkCounts{RawKeyPrepares: 1})
	if err := e.Checkpoint(); err != nil {
		return Deduplication{Fingerprint: "", Reused: true}, err
	}
	if fp == "" {
		return Deduplication{Fingerprint: "", Reused: false}, e.admitRender()
	}
	receipt := attempt.Render
	_, _, *attempt = e.Reads.GetEmission(id)
	attempt.Render = receipt
	defer attempt.Finish()
	output, matched := attempt.Replay(fp)
	if !matched {
		return Deduplication{Fingerprint: fp, Reused: false}, e.admitRender()
	}
	if attempt.Render == nil && output.Render != nil && attempt.OutputReads != nil && attempt.OutputReads.Valid() &&
		e.Reads.Matches(attempt.OutputReads) {
		attempt.Render = output.Render
	}
	if err := e.Checkpoint(); err != nil {
		return Deduplication{Fingerprint: fp, Reused: true}, err
	}
	e.Reused = true
	if slices.Equal(output.Unreadable, e.Unreadable) {
		e.Reads.CarryWarnings(output)
	}
	attempt.Children = output.Children
	attempt.Manifests = output.Manifests
	attempt.Fingerprint = output.Fingerprint
	attempt.Unreadable = e.Unreadable
	attempt.Complete = true
	return Deduplication{Fingerprint: fp, Reused: true}, e.Emit(ctx, id, attempt, nil)
}

// Outcome returns the retained readiness decision without reading the store.
func (e *Execution) Outcome() schedule.Kind { return e.kind }

// Progressed reports an execution that performed work beyond equal-input replay.
func (e *Execution) Progressed() bool { return !e.Reused }

// Close retires all provenance borrowed by the completed invocation.
func (e *Execution) Close() {
	e.closeAcquisition()
	e.Reads.Close()
}

func (e *Execution) closeAcquisition() {
	if pending, ok := errors.AsType[*pendingAcquisitionError](e.pending); ok {
		pending.close()
	}
}

func (e *Execution) withdrawInvalid(ctx context.Context) error {
	_, _, next := e.Reads.GetEmission(e.Reads.ID())
	defer next.Finish()
	invalid := !next.OutputReads.Valid()
	generated := slices.ContainsFunc(next.Children, func(child store.Child) bool { return child.Generated })
	if e.Store.HasInventory(e.Reads.ID()) && (invalid || generated) {
		next.Complete = invalid
		return e.publish(ctx, e.Reads.ID(), &next, nil)
	}
	return nil
}

func (e *Execution) admitRender() error {
	if err := e.Checkpoint(); err != nil {
		return err
	}
	id := e.Reads.ID()
	if info, ok := e.Store.GetStatus(id); ok && info.Status == store.StatusPending {
		e.Store.ReportProgress(id, "rendering")
	} else {
		e.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending, Message: "rendering"})
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
	{
		inputValues := wait.Inputs
		for inputIndex := range inputValues {
			input := inputValues[inputIndex]

			if !input.Optional && !input.Input.Private {
				for _, producer := range input.Input.Materializers {
					e.Reads.RequireData(producer)
				}
			}
		}
	}
	return (&pendingDataError{execution: e, wait: wait}).Settle(ctx, level)
}

type pendingDataError struct {
	execution *Execution
	wait      DataWait
	blocked   []manifest.NamedResource
}

func (p *pendingDataError) Error() string { return "waiting for values inputs" }
func (p *pendingDataError) Unwrap() error { return &depwait.ErrBlocked{Deps: p.blocked} }
func (p *pendingDataError) Settle(ctx context.Context, level manifest.DrainLevel) error {
	e := p.execution
	gateErr := e.classify(ctx, level)

	w := depwait.Waiter{
		Store:            &e.Reads,
		Parent:           e.Reads.ID(),
		Existence:        e.existence,
		AllowMissingCRDs: e.allowMissingCRDs,
	}
	var omitted []manifest.NamedResource
	failed, _ := errors.AsType[*manifest.DependencyFailedError](gateErr)
	p.blocked = p.blocked[:0]
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](gateErr); ok {
		p.blocked = append(p.blocked, blocked.Deps...)
	}
	{
		inputValues2 := p.wait.Inputs
		for inputIndex2 := range inputValues2 {
			input := inputValues2[inputIndex2]

			result := w.Available(ctx, input, level)
			if result.Err != nil {
				return result.Err
			}
			if !input.Optional && !input.Input.Private && result.Kind != schedule.Blocked && !result.Omit {
				e.Reads.RequireData(input.NamedResource)
			}
			omit := result.Omit && !input.Optional
			switch result.Kind {
			case schedule.Blocked:
				p.blocked = append(p.blocked, result.Blocked...)
			case schedule.DependencyFailed:
				failed = p.addFailure(failed, result.Failed, &result.Reason, &input)
			case schedule.Terminal:
				if omit {
					omitted = append(omitted, input.NamedResource)
				}
			default:
				if omit {
					omitted = append(omitted, input.NamedResource)
				}
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
	if gateErr != nil {
		return gateErr
	}
	for _, ref := range e.unresolved {
		e.Reads.AddWarning(
			manifest.Warning{
				Category: manifest.WarnUnresolvedSubstitution,
				Message: fmt.Sprintf(
					"substituteFrom Secret %q could not be read offline; its variables are unavailable, so their ${VAR} references may remain unsubstituted or resolve to defaults or empty values",
					ref.Name,
				),
			},
		)
	}
	return p.wait.Resume(ctx, level, compactDataIDs(omitted))
}

func compactDataIDs(ids []manifest.NamedResource) []manifest.NamedResource {
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return slices.Compact(ids)
}

func (e *Execution) promoteData(ctx context.Context, id manifest.NamedResource) error {
	if e.filter.Enabled() {
		var proof store.ReadSet
		proof.Init(e.Store, e.Reads.ID(), nil)
		proof.SetWork(e.Reads.Work())
		defer proof.Retire()
		proof.RequireData(id)
		if _, err := e.KeepEmitted(ctx, manifest.NamedResource{}, store.Child{}, &proof); err != nil {
			return err
		}
	}
	if e.existence != nil {
		_, err := e.existence.Promote(ctx, &e.Reads, id)
		return err
	}
	return nil
}

func (e *Execution) dispatchMissing(ctx context.Context, level manifest.DrainLevel) {
	id := e.Reads.ID()
	e.resume = e.requireDeclaration
	err := e.requireDeclaration(ctx, level)
	if err != nil && !errors.Is(err, manifest.ErrSourceSkipped) {
		e.finish(ctx, err)
	} else {
		if err := e.withdrawInvalid(ctx); err != nil {
			e.finish(ctx, err)
			return
		}
		if info, exists := e.Store.GetStatus(id); exists && info.Status != store.StatusReady {
			e.Store.UpdateStatus(
				id,
				store.StatusInfo{
					Status:  store.StatusReady,
					Reason:  store.ReasonSkipped,
					Message: "object not in store at reconcile time",
				},
			)
		}
		e.Reads.Accept(store.AssessmentReconciled)
	}
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
