package depwait

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// Classification retains one dependency's readiness decision.
// Successful decisions MUST not carry an expanded DiagnosticReason.
type Classification struct {
	Kind    schedule.Kind
	Subject *manifest.DiagnosticIdentity
	Message string
	Causes  []manifest.DiagnosticReason
	Err     error
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

func failedClassification(id manifest.NamedResource, obj manifest.BaseManifest, message string, info *store.Observation) Classification {
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
func (w *Waiter) Classify(ctx context.Context, dep manifest.DependencyRef, level manifest.DrainLevel) Classification {
	existence := w.Existence
	reads, _ := w.Store.(*store.ReadSet)
	id := dep.NamedResource
	hasExpr := dep.ReadyExpr != nil && *dep.ReadyExpr != ""
	for {
		if dep.ArtifactOnly {
			if art, ok := w.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact); ok && art.FS != nil {
				return Classification{Kind: schedule.Terminal}
			}
		}
		obj, info := w.Store.Snapshot(id, hasExpr || w.Materializer)
		present := obj != nil || info.Status != ""
		if !w.Materializer && present && !info.Statusful {
			return w.artifactReady(dep, level)
		}
		ready := false
		if hasExpr {
			var failMsg string
			ready, failMsg = w.tryReadyExpr(dep, obj, info.Conditions)
			if failMsg != "" {
				return failedClassification(id, obj, failMsg, nil)
			}
		}
		if !present && existence != nil {
			promoted, err := existence.Promote(ctx, reads, id)
			if err != nil {
				return Classification{Err: err}
			}
			if promoted {
				existence = nil
				continue
			}
		}
		if dep.UnknownArtifactRoot && level >= manifest.DrainCascade && obj == nil && w.Store.GetArtifact(id, store.ArtifactContent) == nil {
			return Classification{Err: fmt.Errorf("%w: source %s has an unknown OCI artifact root", manifest.ErrInput, manifest.DiagnosticFor(id, obj))}
		}
		if present && ready {
			return w.artifactReady(dep, level)
		}
		if !present && dep.Artifact {
			if _, ok := w.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact); ok {
				return w.artifactReady(dep, level)
			}
		}
		if !present {
			if level < manifest.DrainCascade {
				return Classification{Kind: schedule.Blocked}
			}
			if w.AllowMissingCRDs && id.IsCRD() {
				return w.artifactReady(dep, level)
			}
			return Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}
		}
		if !hasExpr && info.Status == store.StatusReady {
			return w.artifactReady(dep, level)
		}
		if info.Status == store.StatusFailed {
			if w.Materializer && info.IsDependencyFailure() {
				return failedClassification(id, obj, "declaration not emitted", nil)
			}
			return failedClassification(id, obj, "", &info)
		}
		if hasExpr && level >= manifest.DrainCascade {
			return failedClassification(id, obj, "readyExpr timeout: context deadline exceeded", nil)
		}
		if !hasExpr && level >= manifest.DrainForce {
			return failedClassification(id, obj, "not ready", nil)
		}
		return Classification{Kind: schedule.Blocked}
	}
}

type existenceOnly interface {
	ExistenceOnly() bool
}

func (w *Waiter) artifactReady(dep manifest.DependencyRef, level manifest.DrainLevel) Classification {
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
		return Classification{Err: fmt.Errorf("%w: %s %s skipped: %s", manifest.ErrSourceSkipped, label, manifest.DiagnosticFor(id, obj), info.String())}
	}
	if descriptor, ok := obj.(existenceOnly); ok && descriptor.ExistenceOnly() && !dep.ArtifactOnly {
		return Classification{Kind: schedule.Terminal}
	}
	art, _ := w.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	if art != nil && art.FS != nil {
		return Classification{Kind: schedule.Terminal}
	}
	if dep.ArtifactOnly {
		return Classification{Err: fmt.Errorf("%w: source %s has no readable content", manifest.ErrInput, manifest.DiagnosticFor(id, obj))}
	}
	if level >= manifest.DrainCascade {
		message := fmt.Sprintf("%s %s artifact content unavailable", label, manifest.DiagnosticFor(id, obj))
		if !dep.ChartSource && art == nil {
			message = fmt.Sprintf("source %s artifact not found", manifest.DiagnosticFor(id, obj))
		}
		return Classification{Err: fmt.Errorf("%w: %s", manifest.ErrObjectNotFound, message)}
	}
	return Classification{Kind: schedule.Blocked}
}
