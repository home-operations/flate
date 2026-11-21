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
	var blocked []manifest.NamedResource
	var failed *manifest.DependencyFailedError
	var cause error
	var causeID manifest.NamedResource
	reads.ClearToleratedCRDs()
	for dep := range reads.Gates() {
		w.Materializer = dep.Materializer
		cl := w.Classify(ctx, dep.DependencyRef, level)
		switch {
		case cl.Err != nil:
			if cause == nil || dep.Compare(causeID) < 0 {
				cause, causeID = cl.Err, dep.NamedResource
			}
		case cl.ToleratedCRD && !dep.Materializer:
			reads.RecordToleratedCRD(dep.NamedResource)
		case cl.Kind == schedule.DependencyFailed:
			if failed == nil {
				failed = &manifest.DependencyFailedError{
					Parent:  w.Parent,
					Subject: manifest.DiagnosticFor(w.Parent, w.Store.GetObject(w.Parent)),
					Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
				}
			}
			failed.Failed = append(failed.Failed, dep.NamedResource)
			reason := cl.DiagnosticReason(dep.NamedResource)
			if prior, exists := failed.Reasons[dep.NamedResource]; !exists || reason.Compare(prior) < 0 {
				failed.Reasons[dep.NamedResource] = reason
			}
		case cl.Kind == schedule.Blocked:
			blocked = append(blocked, dep.NamedResource)
			blocked = append(blocked, unsettledDeclarationProducers(reads, &dep)...)
		}
	}
	w.Materializer = false
	if len(blocked) > 0 {
		slices.SortFunc(blocked, manifest.NamedResource.Compare)
		return &BlockedError{Deps: slices.Compact(blocked)}
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
	if message := reads.Preflight(); message != "" {
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
	artifact, readable := reads.ReadableSourceArtifact(id)
	if !readable {
		return false
	}
	input, found := reads.ReadableSourceArtifact(manifest.BootstrapSourceID)
	return found && artifact == input && artifact.Reads.Valid()
}
