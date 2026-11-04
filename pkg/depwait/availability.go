package depwait

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// DataDependency retains a values reference and its captured availability policy.
type DataDependency struct {
	manifest.DependencyRef

	Input        store.ReferenceFacts
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
//
//nolint:gocritic // The SDK accepts a value snapshot; a pointer would share caller-owned policy fields.
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
		if cl.Kind == schedule.Blocked ||
			cl.Kind == schedule.Terminal && level < manifest.DrainForce && slices.Contains(pending, producer) {
			blocked = append(blocked, producer)
		}
		if failure, failed := w.materializerFailure(&dep, producer, &cl); failed {
			return failure
		}
	}
	if len(blocked) != 0 {
		return availability{Kind: schedule.Blocked, Blocked: blocked}
	}
	if dep.Input.Data != nil {
		return availability{Kind: schedule.Terminal}
	}
	return w.availableAbsent(ctx, &dep, level)
}

func (w *Waiter) materializerFailure(
	dep *DataDependency,
	producer manifest.NamedResource,
	cl *Classification,
) (availability, bool) {
	if cl.Kind != schedule.DependencyFailed || dep.Optional || dep.AllowMissing || dep.Input.AllowsAbsence() {
		return availability{}, false
	}
	status, _ := w.Store.GetStatus(producer)
	if status.Status == store.StatusFailed && !w.blocksParent(producer) {
		return availability{}, false
	}
	if status.Status == store.StatusFailed {
		cl.Message, cl.Causes = "not ready", nil
	}
	return availability{
		Kind:   cl.Kind,
		Reason: cl.DiagnosticReason(producer),
		Failed: []manifest.NamedResource{producer},
	}, true
}

func (w *Waiter) availableAbsent(ctx context.Context, dep *DataDependency, level manifest.DrainLevel) availability {
	allowed := dep.Optional || dep.AllowMissing || dep.Input.AllowsAbsence()
	if dep.Input.AllowsAbsence() || len(dep.Input.Materializers) != 0 {
		if allowed {
			return availability{Kind: schedule.Terminal, Omit: true}
		}
		return availability{
			Kind: schedule.DependencyFailed,
			Reason: manifest.DiagnosticReason{
				Subject: manifest.DiagnosticFor(dep.NamedResource, w.Store.GetObject(dep.NamedResource)),
				Message: dependencyNotFound,
			},
		}
	}
	if allowed && !dep.Input.Offline {
		return availability{Kind: schedule.Terminal, Omit: true}
	}
	cl := w.Classify(ctx, dep.DependencyRef, level)
	if cl.Err != nil {
		return availability{Err: cl.Err}
	}
	if cl.Kind == schedule.Blocked {
		return availability{
			Kind:    cl.Kind,
			Reason:  cl.DiagnosticReason(dep.NamedResource),
			Blocked: []manifest.NamedResource{dep.NamedResource},
		}
	}
	if allowed {
		return availability{Kind: schedule.Terminal, Omit: true}
	}
	if cl.Kind == schedule.Terminal {
		return availability{
			Kind: schedule.DependencyFailed,
			Reason: manifest.DiagnosticReason{
				Subject: manifest.DiagnosticFor(dep.NamedResource, w.Store.GetObject(dep.NamedResource)),
				Message: dependencyNotFound,
			},
		}
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
