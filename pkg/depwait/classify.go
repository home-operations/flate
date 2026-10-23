package depwait

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// classification describes one ordinary readiness decision.
type classification struct {
	Kind    schedule.Kind
	Message string // populated only for schedule.DependencyFailed
}

func (w *Waiter) classify(dep manifest.DependencyRef, level manifest.DrainLevel, existence ExistenceLookup) classification {
	id := dep.NamedResource
	for {
		var info store.Observation
		obj := w.Store.Snapshot(&id, dep.ReadyExpr != "", &info)
		present := obj != nil || info.Status != ""
		if present && !info.Statusful {
			return classification{Kind: schedule.Terminal}
		}
		if dep.ReadyExpr != "" {
			ready, failMsg := w.tryReadyExpr(dep, obj, info.Conditions)
			if failMsg != "" {
				return classification{Kind: schedule.DependencyFailed, Message: failMsg}
			}
			if present && ready {
				return classification{Kind: schedule.Terminal}
			}
		}
		if !present {
			if existence != nil && existence.Promote(id) {
				existence = nil
				continue
			}
			if level < manifest.DrainCascade {
				return classification{Kind: schedule.Blocked}
			}
			if w.AllowMissingCRDs && id.IsCRD() {
				return classification{Kind: schedule.Terminal}
			}
			return classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}
		}
		if dep.ReadyExpr == "" && info.Status == store.StatusReady {
			return classification{Kind: schedule.Terminal}
		}
		if info.Status == store.StatusFailed {
			return classification{Kind: schedule.DependencyFailed, Message: info.Message}
		}
		if dep.ReadyExpr != "" && level >= manifest.DrainCascade {
			return classification{Kind: schedule.DependencyFailed, Message: "readyExpr timeout: context deadline exceeded"}
		}
		if dep.ReadyExpr == "" && level >= manifest.DrainForce {
			return classification{Kind: schedule.DependencyFailed, Message: "not ready"}
		}
		return classification{Kind: schedule.Blocked}
	}
}
