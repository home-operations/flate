package orchestrator

import (
	"context"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

func (o *Orchestrator) failDependsOnCycles(ctx context.Context) error {
	o.rebuildDependencyGraphFromStore()
	return o.syncPreflightFailures(ctx)
}

func (o *Orchestrator) updateDependencyGraphFor(ctx context.Context, id manifest.NamedResource) error {
	for {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		obj, policy := o.store.Lookup(id)
		var buf [8]manifest.NamedResource
		var deps []manifest.NamedResource
		if obj != nil && policy.Reconcile {
			deps = sameKindDepTargets(obj, buf[:0])
		}
		o.depGraph.ReplaceEdges(id, deps)
		if err := o.syncPreflightFailures(ctx); err != nil {
			return err
		}
		current, currentPolicy := o.store.Lookup(id)
		if current == obj && currentPolicy == policy {
			return nil
		}
	}
}

//nolint:gocognit // Cancellation MUST share the existing convergence and publication frame.
func (o *Orchestrator) syncPreflightFailures(ctx context.Context) error {
	var previous map[manifest.NamedResource]string
	for {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		paths := o.depGraph.Failures()
		var failures map[manifest.NamedResource]string
		if len(paths) != 0 || o.entryBinding.Failure != nil {
			failures = make(map[manifest.NamedResource]string, len(paths))
		}
		if o.entryBinding.Failure != nil {
			failures[o.entryBinding.Producer] = o.entryBinding.Failure.Error()
		}
		for id, cycle := range paths {
			failures[id] = cycle.diagnostic(o)
		}
		o.preflightMu.Lock()
		cleared := o.replacePreflightFailures(failures)
		o.preflightMu.Unlock()
		unique := make(map[*dependencyCycle]struct{})
		for _, cycle := range paths {
			unique[cycle] = struct{}{}
		}
		for cycle := range unique {
			o.store.AddWarning(
				manifest.Warning{
					Related:  cycle.path,
					Category: manifest.WarnDependencyCycle,
					Message:  "dependency cycle detected",
					Detail:   []string{cycle.diagnostic(o)},
				},
			)
		}

		for id, message := range failures {
			o.store.SetPreflight(id, message)
		}
		for _, id := range cleared {
			o.store.SetPreflight(id, "")
		}
		// A newer publisher can clear the shared map before these stale writes finish.
		for id := range previous {
			o.store.SetPreflight(id, failures[id])
		}
		if o.depGraph.failuresCurrent(paths) {
			return nil
		}
		previous = failures
	}
}

func (o *Orchestrator) rebuildDependencyGraphFromStore() {
	for _, info := range reconcilableKinds {
		for _, obj := range o.store.ListObjects(info.Group, info.Kind) {
			id := obj.Named()
			if !o.store.Policy(id).Reconcile {
				continue
			}
			var buf [8]manifest.NamedResource
			deps := sameKindDepTargets(obj, buf[:0])
			o.depGraph.ReplaceEdges(id, deps)
		}
	}
}

// Flux Kustomization and HelmRelease dependsOn references are kind-homogeneous.
func sameKindDepTargets(obj manifest.BaseManifest, out []manifest.NamedResource) []manifest.NamedResource {
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == "dependsOn" {
			out = append(out, ref.NamedResource)
		}
	}
	return out
}

// formatCyclePath returns the cycle as "A → B → C → A" for log output.
func (o *Orchestrator) formatCyclePath(path []manifest.NamedResource) string {
	parts := make([]string, len(path))
	for i, id := range path {
		parts[i] = o.diagnostic(id).String()
	}
	return strings.Join(parts, " → ")
}
