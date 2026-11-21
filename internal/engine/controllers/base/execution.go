package base

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

type offlineRenderError struct{ error }

func (e *offlineRenderError) Unwrap() error { return e.error }

// Execution MUST retain its input capture through every output borrower.
type Execution struct {
	*Controller

	feedback              *renderFeedback
	independentRender     bool
	preparation           preparation
	data                  *values.StoreProvider
	AdmissionPreparation  *selection.Preparation
	ProbeLeft, ProbeRight *Execution
	ProbeConsumed         bool
	EntryReplay           bool
	probeStored           bool
	probe                 bool
	declaration           manifest.BaseManifest
	generationKnown       bool
	generationErr         error
	Reads                 store.ReadSet
	Unreadable            []manifest.NamedResource
	Blocked               []manifest.NamedResource
	Disposition           store.AssessmentKind
	kind                  schedule.Kind
	pending               error
	resume                func(context.Context, manifest.DrainLevel) error
	initial               store.StatusInfo
	recovering            bool
	generated             store.ChildDocuments
	childGeneration       map[manifest.NamedResource]preparedGeneration
	prepared              bool
}

// RenderFailure MUST precede publication and never excuses a failed readable document.
func (e *Execution) RenderFailure(
	ctx context.Context,
	err error,
	documents []manifest.DocOccurrence,
	unreadable []manifest.Reference,
) error {
	if err == nil {
		err = manifest.DocumentFailure(documents)
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	renderWide := len(unreadable) != 0
	for _, document := range documents {
		if document.Err == nil {
			continue
		}
		if len(document.ConsumedUnreadable) == 0 && !renderWide {
			return err
		}
		unreadable = append(slices.Clone(unreadable), document.ConsumedUnreadable...)
	}
	if len(unreadable) == 0 {
		return err
	}
	labels := make([]string, 0, len(unreadable))
	for i := range unreadable {
		ref := &unreadable[i]
		labels = append(labels, fmt.Sprintf("%s %s %s/%s key %q (reference %d)", ref.Use, ref.Kind,
			ref.Namespace, ref.Name, ref.Key, ref.Position+1))
	}
	slices.Sort(labels)
	labels = slices.Compact(labels)
	return manifest.NewError(manifest.ErrSourceSkipped, &offlineRenderError{err},
		"cannot render offline: unreadable inputs "+strings.Join(labels, ", ")+": "+err.Error())
}

// Init starts capture in caller-owned storage before declaration or dependency assessment.
// The execution MUST remain live until its registrations and output borrowers retire.
func (e *Execution) Init(c *Controller, id manifest.NamedResource) {
	c.selection.Include(id)
	e.Controller = c
	e.probe = false
	e.Reads.Init(c.Store, id, nil)
	e.Reads.BeginExecution(nil)
}

// InitProbe MUST NOT publish runtime admission or status.
func (e *Execution) InitProbe(c *Controller, object manifest.BaseManifest) {
	e.Controller = c
	e.probe = true
	e.declaration = object
	e.Reads.Init(c.Store, object.Named(), nil)
	_, _, output := e.Reads.GetEmission(object.Named())
	output.Finish()
	e.probeStored = e.Reads.GetDeclaration(object.Named()) != nil
	e.Reads.BeginExecution(object)
}

// PromoteProbe keeps the input capture alive through the original emission.
func (e *Execution) PromoteProbe() {
	e.probe = false
	e.EntryReplay = true
}

func (e *Execution) matchesDeclaration(object manifest.BaseManifest) bool {
	return object != nil && manifest.Equal(e.declaration, object)
}

// ProbeCurrent MUST preserve the declaration authority captured at initialization.
func (e *Execution) ProbeCurrent(indexed manifest.BaseManifest) bool {
	object := e.Store.GetDeclaration(e.Reads.ID())
	if object == nil && !e.probeStored {
		object = indexed
	}
	return e.matchesDeclaration(object)
}

// Take MUST precede retirement of the predecessor's preparation owner.
func (e *Execution) Take(previous schedule.Result) {
	prior, ok := previous.Assessment.(*Execution)
	if !ok || prior == nil || prior.Reads.ID() != e.Reads.ID() {
		return
	}
	if e.declaration == nil {
		e.declaration = e.Reads.GetObject(e.Reads.ID())
	}
	e.AdmissionPreparation, prior.AdmissionPreparation = prior.AdmissionPreparation, nil
	e.feedback, prior.feedback = prior.feedback, nil
	e.childGeneration, prior.childGeneration = prior.childGeneration, nil
	if !prior.matchesDeclaration(e.declaration) {
		return
	}
	e.preparation, prior.preparation = prior.preparation, nil
	e.generated, prior.generated = prior.generated, store.ChildDocuments{}
	e.generationKnown, prior.generationKnown = prior.generationKnown, false
	e.generationErr, prior.generationErr = prior.generationErr, nil
}

// Valid reports whether the assessment and its admitted output are current.
func (e *Execution) Valid() bool {
	if !e.Reads.Valid() {
		return false
	}
	if e.Outcome() == schedule.Blocked || !e.Reads.Admitted() || e.Disposition == store.AssessmentQualificationSettled {
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
	if e.preparation != nil {
		e.preparation.retire()
		e.preparation = nil
	}
	e.feedback = nil
	e.data = nil
	e.AdmissionPreparation.Retire()
	e.AdmissionPreparation = nil
	e.generated, e.childGeneration = store.ChildDocuments{}, nil
	e.generationErr, e.declaration = nil, nil
	e.generationKnown = false
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
	e.Reads.Bind(next)
	if e.Disposition == store.AssessmentReused && borrowed.Valid() && next.Complete && len(next.Failures) == 0 {
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
	if err := e.generatedChildren(ctx, next); err != nil {
		return err
	}
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
		admit = admit || e.selection != nil && !e.selection.Snapshot().Reconcile(dep.NamedResource)
	}
	if admit {
		if _, err := e.KeepEmitted(
			ctx,
			manifest.NamedResource{},
			selection.PrepareRequest{Generation: nil, Reads: &e.Reads, Preparation: &e.AdmissionPreparation},
		); err != nil {
			return err
		}
	}
	return e.classify(ctx, level)
}

func (e *Execution) classify(ctx context.Context, level manifest.DrainLevel) error {
	w := e.dependencyWaiter()
	return w.Assess(ctx, &e.Reads, level)
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
	if err == nil {
		err = ctx.Err()
	}
	if err != nil && e.Disposition == store.AssessmentQualificationSettled {
		e.Disposition = store.AssessmentReconciled
	}
	if blocked, ok := errors.AsType[*depwait.BlockedError](err); ok {
		e.Blocked = blocked.Deps
		e.kind = schedule.Blocked
	} else {
		e.pending = nil
		e.resume = nil
	}
	if !e.Reads.Valid() {
		return
	}
	defer e.Reads.Accept(e.Disposition)
	id := e.Reads.ID()
	e.Blocked = nil
	e.kind = schedule.Terminal
	if blocked, ok := errors.AsType[*depwait.BlockedError](err); ok {
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
	if e.Disposition == store.AssessmentQualificationSettled {
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
	e.withdrawSource(errors.Is(err, manifest.ErrSourceSkipped))
	_, _, next := e.Reads.GetEmission(id)
	_, offline := errors.AsType[*offlineRenderError](err)
	if offline {
		e.generated, e.childGeneration = store.ChildDocuments{}, nil
		e.generationKnown = false
		next.Finish()
		next = store.Emission{Complete: true}
		if publishErr := e.publish(ctx, id, &next, nil); publishErr != nil {
			err = errors.Join(err, publishErr)
		}
	}
	if !offline && e.Store.HasInventory(id) && len(next.Failures) == 0 {
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
	e.declaration = obj
	policy := e.Reads.Policy(id)
	if ok && !e.selection.Snapshot().Output(manifest.NamedResource{}) {
		if _, err := e.KeepEmitted(
			ctx,
			manifest.NamedResource{},
			selection.PrepareRequest{
				Generation:  []store.Child{{Obj: obj, Origin: policy.Origin}},
				Reads:       &e.Reads,
				Preparation: &e.AdmissionPreparation,
			},
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
	if _, blocked := errors.AsType[*depwait.BlockedError](err); blocked {
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
	e.Reads.Accept(e.Disposition)
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
	if _, blocked := errors.AsType[*depwait.BlockedError](err); blocked {
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

// Deduplication omits its fingerprint when no stable content key exists.
type Deduplication struct {
	Fingerprint string
	Reused      bool
}

// AdmitQualification settles a negative precondition only without ordinary demand.
func (e *Execution) AdmitQualification(possible bool) bool {
	if !possible && !e.Reads.OrdinaryDemand(e.Reads.ID()) {
		e.Disposition = store.AssessmentQualificationSettled
		return true
	}
	return false
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
	if e.selection != nil {
		decision := e.selection.Snapshot()
		selected, dirty := decision.Output(id), decision.Changed(id)
		// Unresolved inputs MUST reach content fingerprinting before reuse.
		if selected && !dirty && len(e.Unreadable) == 0 && e.Disposition != store.AssessmentReused {
			e.Disposition = store.AssessmentReused
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
		if e.Disposition == store.AssessmentReused {
			return Deduplication{Fingerprint: fp}, nil
		}
		return Deduplication{Fingerprint: fp, Reused: false}, e.admitRender()
	}
	if attempt.Render == nil && output.Render != nil && attempt.OutputReads != nil && attempt.OutputReads.Valid() &&
		e.Reads.Matches(attempt.OutputReads) {
		attempt.Render = output.Render
	}
	if err := e.Checkpoint(); err != nil {
		return Deduplication{Fingerprint: fp, Reused: true}, err
	}
	e.Disposition = store.AssessmentReused
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
func (e *Execution) Progressed() bool { return e.Disposition == store.AssessmentReconciled }

// Close retires all provenance borrowed by the completed invocation.
func (e *Execution) Close() {
	e.Retire()
	e.Reads.Close()
}

func (e *Execution) closeAcquisition() {
	if pending, ok := errors.AsType[*pendingAcquisitionError](e.pending); ok {
		pending.close()
	}
}

func (e *Execution) withdrawInvalid(ctx context.Context) error {
	e.withdrawSource(false)
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

func (e *Execution) withdrawSource(skipped bool) {
	id := e.Reads.ID()
	if artifact, ok := e.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact); ok &&
		(skipped || !artifact.Reads.Valid()) {
		e.Store.SetArtifact(id, nil)
	}
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
		e.Reads.Accept(e.Disposition)
	}
}
