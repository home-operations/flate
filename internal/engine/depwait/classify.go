package depwait

import (
	"context"
	"errors"
	"fmt"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

const dependencyNotFound = "dependency not found"

// Classification retains one dependency's readiness decision.
// Successful decisions MUST not carry an expanded DiagnosticReason.
type Classification struct {
	Kind         schedule.Kind
	ToleratedCRD bool
	Subject      *manifest.DiagnosticIdentity
	Message      string
	Causes       []manifest.DiagnosticReason
	Err          error
}

// DiagnosticReason returns captured failure context for the raw dependency id.
// id MUST be the raw dependency passed to Classify.
func (c Classification) DiagnosticReason(id manifest.NamedResource) manifest.DiagnosticReason {
	if c.Kind != schedule.DependencyFailed {
		return manifest.DiagnosticReason{}
	}
	subject := manifest.DiagnosticFor(id, nil)
	if c.Subject != nil {
		subject = *c.Subject
	}
	return manifest.DiagnosticReason{Subject: subject, Message: c.Message, Causes: c.Causes}
}

func failedClassification(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	message string,
	info *store.Observation,
) Classification {
	reason := manifest.DiagnosticReason{Subject: manifest.DiagnosticFor(id, obj), Message: message}
	if info != nil {
		reason = info.DependencyReason(reason.Subject)
	}
	result := Classification{Kind: schedule.DependencyFailed, Message: reason.Message, Causes: reason.Causes}
	if reason.Subject.Resource != reason.Subject.ID || reason.Subject.Path != "" {
		subject := reason.Subject
		result.Subject = &subject
	}
	return result
}

// Classify resolves readiness and required content from one captured snapshot.
//
//nolint:gocritic // DependencyRef remains a value in the SDK so callers retain ownership.
func (w *Waiter) Classify(ctx context.Context, dep manifest.DependencyRef, level manifest.DrainLevel) Classification {
	cl, _ := w.classifyCaptured(ctx, &dep, level, nil)
	return cl
}

func (w *Waiter) classifyCaptured(
	ctx context.Context, dep *manifest.DependencyRef, level manifest.DrainLevel, ordering *orderingAssessment,
) (Classification, []manifest.NamedResource) {
	if message := w.Store.Preflight(w.Parent).Message; message != "" {
		return Classification{Err: manifest.NewError(manifest.ErrInput, nil, message)}, nil
	}
	result, pending, promoted := w.classify(ctx, dep, level, ordering)
	if !promoted {
		return result, pending
	}
	captured := *w
	captured.Existence = nil
	result, pending, _ = captured.classify(ctx, dep, level, ordering)
	return result, pending
}

func (w *Waiter) classify(
	ctx context.Context,
	dep *manifest.DependencyRef,
	level manifest.DrainLevel,
	ordering *orderingAssessment,
) (Classification, []manifest.NamedResource, bool) {
	id := dep.NamedResource
	hasExpr := dep.ReadyExpr != nil && *dep.ReadyExpr != ""
	if w.artifactOnlyReady(dep, !dep.RequiresDeclaration) {
		return Classification{Kind: schedule.Terminal}, nil, false
	}
	observed := w.observeDependency(dep, ordering, hasExpr)
	if dep.RequiresDeclaration && w.artifactOnlyReady(dep, observed.present) {
		return Classification{Kind: schedule.Terminal}, nil, false
	}
	if !w.Materializer && observed.present && !observed.info.Statusful {
		return w.artifactReady(dep, level), nil, false
	}
	var downstream error
	if ordering != nil {
		ordering.reads.ControllerDemand(id)
		downstream = ordering.projectReadiness(ctx, w, &observed, level)
	}
	var readiness expressionReadiness
	if hasExpr {
		readiness = w.tryReadyExpr(dep, observed.object, observed.info.Conditions)
	}
	if readiness.failure != "" {
		return failedClassification(id, observed.object, readiness.failure, nil), nil, false
	}
	if !observed.present && ordering == nil && w.Existence != nil {
		promoted, err := w.Existence.Promote(ctx, w.Admission, id)
		if promoted || err != nil {
			return Classification{Err: err}, nil, err == nil
		}
	}
	if !observed.present {
		classification, pending := w.classifyAbsent(dep, level, ordering)
		return classification, pending, false
	}
	if readiness.ready {
		return w.artifactReady(dep, level), nil, false
	}
	if downstream != nil {
		if blocked, ok := errors.AsType[*BlockedError](downstream); ok {
			return Classification{Kind: schedule.Blocked}, blocked.Deps, false
		}
		observed.info.StatusInfo = store.FailureStatus(downstream)
	}
	return w.classifyPresent(dep, observed.object, &observed.info, level), nil, false
}

type dependencyObservation struct {
	object  manifest.BaseManifest
	info    store.Observation
	present bool
	hasExpr bool
}

func (w *Waiter) observeDependency(
	dep *manifest.DependencyRef, ordering *orderingAssessment, hasExpr bool,
) dependencyObservation {
	id := dep.NamedResource
	obj, info := w.Store.Snapshot(id, hasExpr || w.Materializer)
	if !dep.RequiresDeclaration {
		return dependencyObservation{
			object:  obj,
			info:    info,
			present: obj != nil || info.Status != "",
			hasExpr: hasExpr,
		}
	}
	if ordering != nil {
		obj = w.Decision.OrderingDeclaration(ordering.reads, id, hasExpr)
	} else {
		obj = w.Store.GetDeclaration(id)
	}
	return dependencyObservation{object: obj, info: info, present: obj != nil, hasExpr: hasExpr}
}

func (w *Waiter) artifactOnlyReady(dep *manifest.DependencyRef, declarationSatisfied bool) bool {
	if !dep.ArtifactOnly || !declarationSatisfied {
		return false
	}
	_, readable := w.Store.ReadableSourceArtifact(dep.NamedResource)
	return readable
}

func (w *Waiter) classifyAbsent(
	dep *manifest.DependencyRef,
	level manifest.DrainLevel,
	ordering *orderingAssessment,
) (Classification, []manifest.NamedResource) {
	id := dep.NamedResource
	if dep.Artifact && !dep.RequiresDeclaration {
		if artifact, _ := w.Store.ReadableSourceArtifact(id); artifact != nil {
			return w.artifactReady(dep, level), nil
		}
	}
	var pending []manifest.NamedResource
	if ordering != nil {
		pending = w.orderingProducers(ordering.reads, id)
	}
	if level < manifest.DrainCascade {
		return Classification{Kind: schedule.Blocked}, pending
	}
	if dep.RequiresDeclaration && level < manifest.DrainForce {
		if ordering != nil && len(pending) != 0 || ordering == nil && len(w.Store.UnsettledProducers(id)) != 0 {
			return Classification{Kind: schedule.Blocked}, pending
		}
	}
	if w.Missing.allowsMissingDeclaration(id) {
		result := w.artifactReady(dep, level)
		result.ToleratedCRD = result.Kind == schedule.Terminal && result.Err == nil
		return result, pending
	}
	return Classification{Kind: schedule.DependencyFailed, Message: dependencyNotFound}, pending
}

func (w *Waiter) classifyPresent(
	dep *manifest.DependencyRef,
	obj manifest.BaseManifest,
	info *store.Observation,
	level manifest.DrainLevel,
) Classification {
	id := dep.NamedResource
	hasExpr := dep.ReadyExpr != nil && *dep.ReadyExpr != ""
	if !hasExpr && info.Status == store.StatusReady {
		return w.artifactReady(dep, level)
	}
	if info.Status == store.StatusFailed {
		if w.Materializer && info.IsDependencyFailure() {
			return failedClassification(id, obj, "declaration not emitted", nil)
		}
		return failedClassification(id, obj, "", info)
	}
	if hasExpr && level >= manifest.DrainCascade {
		return failedClassification(id, obj, "readyExpr timeout: context deadline exceeded", nil)
	}
	if !hasExpr && level >= manifest.DrainForce {
		return failedClassification(id, obj, "not ready", nil)
	}
	return Classification{Kind: schedule.Blocked}
}

type existenceOnly interface {
	ExistenceOnly() bool
}

func (w *Waiter) artifactReady(dep *manifest.DependencyRef, level manifest.DrainLevel) Classification {
	if !dep.Artifact {
		return Classification{Kind: schedule.Terminal}
	}
	id := dep.NamedResource
	obj := w.Store.GetObject(id)
	label := "source"
	if dep.ChartSource {
		label = "chart source"
	}
	if info, ok := w.Store.GetStatus(id); ok && info.Outcome() == store.OutcomeSkipped {
		return Classification{
			Err: manifest.NewError(
				manifest.ErrSourceSkipped,
				nil,
				fmt.Sprintf("%s %s skipped: %s", label, manifest.DiagnosticFor(id, obj), info.String()),
			),
		}
	}
	if descriptor, ok := obj.(existenceOnly); ok && descriptor.ExistenceOnly() && !dep.ArtifactOnly {
		return Classification{Kind: schedule.Terminal}
	}
	artifact, readable := w.Store.ReadableSourceArtifact(id)
	if readable {
		return Classification{Kind: schedule.Terminal}
	}
	if dep.ArtifactOnly {
		return Classification{
			Err: manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf("source %s has no readable content", manifest.DiagnosticFor(id, obj)),
			),
		}
	}
	if level >= manifest.DrainCascade {
		message := fmt.Sprintf("%s %s artifact content unavailable", label, manifest.DiagnosticFor(id, obj))
		if !dep.ChartSource && artifact == nil {
			message = fmt.Sprintf("source %s artifact not found", manifest.DiagnosticFor(id, obj))
		}
		return Classification{Err: manifest.NewError(manifest.ErrObjectNotFound, nil, message)}
	}
	return Classification{Kind: schedule.Blocked}
}
