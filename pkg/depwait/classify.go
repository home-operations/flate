package depwait

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// Classification retains one dependency's readiness decision.
type Classification struct {
	Kind    schedule.Kind
	Message string
	Err     error
}

// Classify resolves readiness and required content from one captured snapshot.
func (w *Waiter) Classify(ctx context.Context, dep manifest.DependencyRef, level manifest.DrainLevel) Classification {
	existence := w.Existence
	reads, _ := w.Store.(*store.ReadSet)
	id := dep.NamedResource
	hasExpr := dep.ReadyExpr != nil && *dep.ReadyExpr != ""
	for {
		obj, info := w.Store.Snapshot(id, hasExpr)
		present := obj != nil || info.Status != ""
		if present && !info.Statusful {
			return w.artifactReady(dep, level)
		}
		ready := false
		if hasExpr {
			var failMsg string
			ready, failMsg = w.tryReadyExpr(dep, obj, info.Conditions)
			if failMsg != "" {
				return Classification{Kind: schedule.DependencyFailed, Message: failMsg}
			}
		}
		if !present && existence != nil && existence.Promote(ctx, reads, id) {
			existence = nil
			continue
		}
		if dep.UnknownArtifactRoot && level >= manifest.DrainCascade && obj == nil && w.Store.GetArtifact(id) == nil {
			return Classification{Err: fmt.Errorf("%w: source %s has an unknown OCI artifact root", manifest.ErrInput, id)}
		}
		if present && ready {
			return w.artifactReady(dep, level)
		}
		if !present && dep.Artifact {
			if _, ok := w.Store.GetArtifact(id).(*store.SourceArtifact); ok {
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
			return Classification{Kind: schedule.DependencyFailed, Message: info.String()}
		}
		if hasExpr && level >= manifest.DrainCascade {
			return Classification{Kind: schedule.DependencyFailed, Message: "readyExpr timeout: context deadline exceeded"}
		}
		if !hasExpr && level >= manifest.DrainForce {
			return Classification{Kind: schedule.DependencyFailed, Message: "not ready"}
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
	label := "source"
	if dep.ChartSource {
		label = "chart source"
	}
	if info, ok := w.Store.GetStatus(id); ok && info.Outcome() == store.OutcomeSkipped {
		return Classification{Err: fmt.Errorf("%w: %s %s skipped: %s", manifest.ErrSourceSkipped, label, id, info.String())}
	}
	if descriptor, ok := w.Store.GetObject(id).(existenceOnly); ok && descriptor.ExistenceOnly() {
		return Classification{Kind: schedule.Terminal}
	}
	art, _ := w.Store.GetArtifact(id).(*store.SourceArtifact)
	if art != nil && art.FS != nil {
		return Classification{Kind: schedule.Terminal}
	}
	if level >= manifest.DrainCascade {
		message := fmt.Sprintf("%s %s artifact content unavailable", label, id)
		if !dep.ChartSource && art == nil {
			message = fmt.Sprintf("source %s artifact not found", id)
		}
		return Classification{Err: fmt.Errorf("%w: %s", manifest.ErrObjectNotFound, message)}
	}
	return Classification{Kind: schedule.Blocked}
}
