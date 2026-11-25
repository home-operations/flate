package depwait

import (
	"context"
	"errors"
	"slices"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// AssessmentStage preserves preparation order and distinguishes the dispatch subject from its prerequisites.
type AssessmentStage uint8

// A declaration gate MUST NOT wait for the subject to mark itself Ready.
const (
	AssessGates AssessmentStage = iota
	AssessDeclaration
	AssessDispatch
)

// Assess preserves readiness and dependency provenance without publishing live status.
func (w *Waiter) Assess(
	ctx context.Context,
	reads *store.ReadSet,
	level manifest.DrainLevel,
	stage AssessmentStage,
) error {
	if stage != AssessGates {
		return w.assessDeclaration(ctx, reads, level, stage == AssessDispatch)
	}
	return w.assessGates(ctx, reads, level)
}

func (w *Waiter) assessGates(ctx context.Context, reads *store.ReadSet, level manifest.DrainLevel) error {
	reads.ClearToleratedCRDs()
	var result gateResult
	ordering := orderingAssessment{reads: reads}
	for dep := range reads.Gates() {
		w.Materializer = dep.Materializer
		var evaluation *orderingAssessment
		if dep.RequiresDeclaration && reads.RequiresOrdering(dep.NamedResource) &&
			w.Decision.EvaluationOnly(dep.NamedResource) {
			evaluation = &ordering
		}
		cl, pending := w.classifyCaptured(ctx, &dep.DependencyRef, level, evaluation)
		if cl.ToleratedCRD && !dep.Materializer {
			reads.RecordToleratedCRD(dep.NamedResource)
		}
		if cl.Kind == schedule.Blocked && evaluation == nil {
			pending = unsettledDeclarationProducers(reads, &dep)
		}
		result.add(w, dep.NamedResource, &cl, pending)
	}
	w.Materializer = false
	return result.err()
}

type gateResult struct {
	blocked []manifest.NamedResource
	failed  *manifest.DependencyFailedError
	cause   error
	causeID manifest.NamedResource
}

func (r *gateResult) add(w *Waiter, id manifest.NamedResource, cl *Classification, pending []manifest.NamedResource) {
	switch {
	case cl.Err != nil:
		if r.cause == nil || id.Compare(r.causeID) < 0 {
			r.cause, r.causeID = cl.Err, id
		}
	case cl.Kind == schedule.DependencyFailed:
		if r.failed == nil {
			obj := w.orderingParent
			if obj == nil {
				obj = w.Store.GetObject(w.Parent)
			}
			r.failed = &manifest.DependencyFailedError{
				Parent:  w.Parent,
				Subject: manifest.DiagnosticFor(w.Parent, obj),
				Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
			}
		}
		r.failed.Failed = append(r.failed.Failed, id)
		reason := cl.DiagnosticReason(id)
		if prior, exists := r.failed.Reasons[id]; !exists || reason.Compare(prior) < 0 {
			r.failed.Reasons[id] = reason
		}
	case cl.Kind == schedule.Blocked:
		r.blocked = append(r.blocked, id)
		r.blocked = append(r.blocked, pending...)
	}
}

func (r *gateResult) err() error {
	if len(r.blocked) > 0 {
		slices.SortFunc(r.blocked, manifest.NamedResource.Compare)
		return &BlockedError{Deps: slices.Compact(r.blocked)}
	}
	if r.cause != nil {
		return r.cause
	}
	if r.failed != nil {
		slices.SortFunc(r.failed.Failed, manifest.NamedResource.Compare)
		r.failed.Failed = slices.Compact(r.failed.Failed)
		return r.failed
	}
	return nil
}

func unsettledDeclarationProducers(reads *store.ReadSet, gate *store.Gate) []manifest.NamedResource {
	if !gate.RequiresDeclaration || reads.GetDeclaration(gate.NamedResource) != nil {
		return nil
	}
	return reads.UnsettledProducers(gate.NamedResource)
}

// ErrDeclarationAbsent identifies a dispatch subject that did not materialize.
var ErrDeclarationAbsent = errors.New("object not in store at reconcile time")

func (w *Waiter) assessDeclaration(
	ctx context.Context, reads *store.ReadSet, level manifest.DrainLevel, dispatch bool,
) error {
	id := w.Parent
	if reads.GetDeclaration(id) != nil && !w.IncompatibleDeclaration {
		return nil
	}
	if w.IncompatibleDeclaration {
		return manifest.NewError(manifest.ErrSourceSkipped, ErrDeclarationAbsent, "")
	}
	if message := reads.Preflight(reads.ID()).Message; message != "" {
		return manifest.NewError(manifest.ErrInput, nil, message)
	}
	if dispatch && bootstrapAcquisition(reads, id) {
		return nil
	}
	owners := reads.DeclarationProducers(id)
	for _, owner := range owners {
		reads.RequireMaterializer(owner)
	}
	var failed *manifest.DependencyFailedError
	var pending []manifest.NamedResource
	if len(owners) != 0 {
		err := w.assessGates(ctx, reads, level)
		failed, _ = errors.AsType[*manifest.DependencyFailedError](err)
		if blocked, ok := errors.AsType[*BlockedError](err); ok {
			pending = blocked.Deps
		} else if err != nil && failed == nil {
			return err
		}
	}
	if level == manifest.DrainCascade && len(pending) == 0 && failed == nil {
		pending = reads.UnsettledProducers(id)
	}
	if len(pending) != 0 || len(owners) == 0 && level < manifest.DrainCascade {
		pending = append(pending, id)
		slices.SortFunc(pending, manifest.NamedResource.Compare)
		return &BlockedError{Deps: slices.Compact(pending)}
	}
	if len(owners) == 0 {
		return manifest.NewError(manifest.ErrSourceSkipped, ErrDeclarationAbsent, "")
	}
	return declarationOmission(reads, id, owners, failed)
}

func declarationOmission(
	reads *store.ReadSet,
	id manifest.NamedResource,
	owners []manifest.NamedResource,
	failed *manifest.DependencyFailedError,
) *manifest.DependencyFailedError {
	if failed == nil {
		failed = &manifest.DependencyFailedError{
			Parent: id, Subject: manifest.DiagnosticFor(id, nil),
			Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
		}
	}
	for _, owner := range owners {
		if _, exists := failed.Reasons[owner]; !exists {
			failed.Failed = append(failed.Failed, owner)
			failed.Reasons[owner] = manifest.DiagnosticReason{
				Subject: manifest.DiagnosticFor(owner, reads.GetObject(owner)),
				Message: "declaration not emitted",
			}
		}
	}
	slices.SortFunc(failed.Failed, manifest.NamedResource.Compare)
	return failed
}

// Discovery can bind the original input as a source artifact without publishing its declaration.
func bootstrapAcquisition(reads *store.ReadSet, id manifest.NamedResource) bool {
	if !id.KindInfo().IsSource() || reads.GetObject(id) == nil {
		return false
	}
	artifact, readable := reads.ReadableSourceArtifact(id, store.ArtifactContent)
	if !readable {
		return false
	}
	input, found := reads.ReadableSourceArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	return found && artifact == input && artifact.Reads.Valid()
}
