package depwait

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// DataDependency retains a values reference and its captured availability policy.
type DataDependency struct {
	manifest.DependencyRef
	Input        values.Input
	Optional     bool
	AllowMissing bool
}

type availability struct {
	Err     error
	Kind    schedule.Kind
	Reason  manifest.DiagnosticReason
	Blocked []manifest.NamedResource
	Failed  []manifest.NamedResource
	Omit    bool
}

// Available waits for offline materializers without making their failure a required cause.
func (w *Waiter) Available(ctx context.Context, dep DataDependency, level manifest.DrainLevel) availability {
	var pending []manifest.NamedResource
	if reads, ok := w.Store.(*store.ReadSet); ok && dep.Input.Data == nil {
		pending = reads.UnsettledProducers(dep.NamedResource)
	}
	var blocked []manifest.NamedResource
	for _, producer := range dep.Input.Materializers {
		if producer == w.Parent {
			continue
		}
		cl := w.Classify(ctx, manifest.DependencyRef{NamedResource: producer}, level)
		if cl.Err != nil {
			return availability{Err: cl.Err}
		}
		if cl.Kind == schedule.Blocked || cl.Kind == schedule.Terminal && level < manifest.DrainForce && slices.Contains(pending, producer) {
			blocked = append(blocked, producer)
		}
		if cl.Kind == schedule.DependencyFailed && !dep.Optional && !dep.AllowMissing && !dep.Input.Private && (!dep.Input.Generated || dep.Input.Offline) {
			if status, _ := w.Store.GetStatus(producer); status.Status != store.StatusFailed || w.blocksParent(producer) {
				if status.Status == store.StatusFailed {
					cl.Message, cl.Causes = "not ready", nil
				}
				return availability{Kind: cl.Kind, Reason: cl.DiagnosticReason(producer), Failed: []manifest.NamedResource{producer}}
			}
		}
	}
	if len(blocked) != 0 {
		return availability{Kind: schedule.Blocked, Blocked: blocked}
	}
	if dep.Input.Data != nil {
		return availability{Kind: schedule.Terminal}
	}
	allowed := dep.Optional || dep.AllowMissing || dep.Input.Private || dep.Input.Generated && !dep.Input.Offline
	if dep.Input.Private || dep.Input.Generated && !dep.Input.Offline || len(dep.Input.Materializers) != 0 {
		if allowed {
			return availability{Kind: schedule.Terminal, Omit: true}
		}
		return availability{Kind: schedule.DependencyFailed, Reason: manifest.DiagnosticReason{Subject: manifest.DiagnosticFor(dep.NamedResource, w.Store.GetObject(dep.NamedResource)), Message: "dependency not found"}}
	}
	if allowed && !dep.Input.Offline {
		return availability{Kind: schedule.Terminal, Omit: true}
	}
	cl := w.Classify(ctx, dep.DependencyRef, level)
	if cl.Err != nil {
		return availability{Err: cl.Err}
	}
	if cl.Kind == schedule.Blocked {
		return availability{Kind: cl.Kind, Reason: cl.DiagnosticReason(dep.NamedResource), Blocked: []manifest.NamedResource{dep.NamedResource}}
	}
	if allowed {
		return availability{Kind: schedule.Terminal, Omit: true}
	}
	if cl.Kind == schedule.Terminal {
		return availability{Kind: schedule.DependencyFailed, Reason: manifest.DiagnosticReason{Subject: manifest.DiagnosticFor(dep.NamedResource, w.Store.GetObject(dep.NamedResource)), Message: "dependency not found"}}
	}
	return availability{Kind: cl.Kind, Reason: cl.DiagnosticReason(dep.NamedResource)}
}

func (w *Waiter) blocksParent(id manifest.NamedResource) bool {
	seen := map[manifest.NamedResource]bool{}
	pending := []manifest.NamedResource{id}
	for len(pending) != 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current == w.Parent {
			return true
		}
		if seen[current] {
			continue
		}
		seen[current] = true
		status, _ := w.Store.GetStatus(current)
		pending = append(pending, status.BlockedBy()...)
	}
	return false
}
