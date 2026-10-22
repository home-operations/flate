package depwait

import (
	"context"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// Classification retains one dependency's readiness decision.
type Classification struct {
	Kind    schedule.Kind
	Message string // populated only for schedule.DependencyFailed
}

// Classify resolves one readiness gate from its captured snapshot.
func (w *Waiter) Classify(ctx context.Context, dep manifest.DependencyRef, level manifest.DrainLevel) Classification {
	existence := w.Existence
	reads, _ := w.Store.(*store.ReadSet)
	id := dep.NamedResource
	for {
		obj, info := w.Store.Snapshot(id, dep.ReadyExpr != "")
		present := obj != nil || info.Status != ""
		if present && !info.Statusful {
			return Classification{Kind: schedule.Terminal}
		}
		if dep.ReadyExpr != "" {
			ready, failMsg := w.tryReadyExpr(dep, obj, info.Conditions)
			if failMsg != "" {
				return Classification{Kind: schedule.DependencyFailed, Message: failMsg}
			}
			if present && ready {
				return Classification{Kind: schedule.Terminal}
			}
		}
		if !present {
			if existence != nil && existence.Promote(ctx, reads, id) {
				existence = nil
				continue
			}
			if level < manifest.DrainCascade {
				return Classification{Kind: schedule.Blocked}
			}
			if w.AllowMissingCRDs && id.IsCRD() {
				return Classification{Kind: schedule.Terminal}
			}
			return Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}
		}
		if dep.ReadyExpr == "" && info.Status == store.StatusReady {
			return Classification{Kind: schedule.Terminal}
		}
		if info.Status == store.StatusFailed {
			return Classification{Kind: schedule.DependencyFailed, Message: info.Message}
		}
		if dep.ReadyExpr != "" && level >= manifest.DrainCascade {
			return Classification{Kind: schedule.DependencyFailed, Message: "readyExpr timeout: context deadline exceeded"}
		}
		if dep.ReadyExpr == "" && level >= manifest.DrainForce {
			return Classification{Kind: schedule.DependencyFailed, Message: "not ready"}
		}
		return Classification{Kind: schedule.Blocked}
	}
}
