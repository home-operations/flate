package cli

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/internal/report"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

type failureScope struct {
	named    bool
	failed   map[manifest.NamedResource]store.StatusInfo
	blocked  map[manifest.NamedResource][]manifest.NamedResource
	warnings []manifest.Warning
}

// scopedFailures is the single post-run projection used by errors, reports and
// diff suppression. Namespace selection applies to roots; their prerequisites
// and owned descendants remain in scope across namespaces. An empty name
// preserves namespace scoping.
func scopedFailures(o *orchestrator.Orchestrator, res *orchestrator.Result, c *commonFlags, selected manifest.NamedResource) failureScope {
	if o == nil || res == nil || len(res.Failed) == 0 {
		return failureScope{}
	}
	scope := failureScope{
		named:   selected.Name != "",
		failed:  map[manifest.NamedResource]store.StatusInfo{},
		blocked: map[manifest.NamedResource][]manifest.NamedResource{},
	}
	var required map[manifest.NamedResource]struct{}
	if selected.Name != "" {
		required = requiredClosure(o, res, c, selected)
	}
	unrelated := map[manifest.NamedResource]store.StatusInfo{}
	for id, info := range res.Failed {
		_, needed := required[id]
		visible := c == nil || c.includeNamespace(o.Filter(), id.Namespace)
		if required != nil && !needed {
			if visible {
				unrelated[id] = info
			}
			continue
		}
		if required == nil && !visible {
			continue
		}
		scope.failed[id] = info
		if deps := res.Blocked[id]; len(deps) > 0 {
			scope.blocked[id] = deps
		}
	}
	if len(unrelated) > 0 {
		scope.warnings = unrelatedFailureWarnings(res, unrelated, required)
	}
	return scope
}

func requiredClosure(o *orchestrator.Orchestrator, res *orchestrator.Result, c *commonFlags, selected manifest.NamedResource) map[manifest.NamedResource]struct{} {
	seen := map[manifest.NamedResource]struct{}{}
	var work []manifest.NamedResource
	add := func(id manifest.NamedResource) {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			work = append(work, id)
		}
	}
	seed := func(id manifest.NamedResource) {
		if id.Kind == selected.Kind && id.Name == selected.Name &&
			(c == nil || c.includeNamespace(o.Filter(), id.Namespace)) {
			add(id)
		}
	}
	for _, obj := range o.Store().ListObjects(selected.Kind) {
		seed(obj.Named())
	}
	for id := range res.Failed {
		seed(id)
	}
	// Only selected roots and their owned descendants expand downward;
	// prerequisites must not bring unrelated siblings into the selection.
	children := o.ChildrenByParent()
	for i := 0; i < len(work); i++ { //nolint:intrange // Appending descendants must extend the traversal.
		for _, child := range children[work[i]] {
			add(child)
		}
	}
	for len(work) > 0 {
		id := work[len(work)-1]
		work = work[:len(work)-1]
		for _, dep := range res.DependsOn[id] {
			add(dep)
		}
		for _, dep := range res.Blocked[id] {
			add(dep)
		}
		switch obj := o.Store().GetObject(id).(type) {
		case *manifest.ResourceSet:
			for _, dep := range obj.DependsOn {
				add(manifest.NamedResource{Kind: dep.Kind, Namespace: cmp.Or(dep.Namespace, id.Namespace), Name: dep.Name})
			}
			for _, ref := range obj.InputsFrom {
				add(manifest.NamedResource{Kind: manifest.KindResourceSetInputProvider, Namespace: id.Namespace, Name: ref.Name})
			}
		}
		if id.Kind == manifest.KindHelmRelease || id.Kind == manifest.KindKustomization {
			for _, dep := range o.RequiredDataDependencies(id) {
				add(dep)
			}
		}
	}
	return seen
}

func unrelatedFailureWarnings(res *orchestrator.Result, unrelated map[manifest.NamedResource]store.StatusInfo, required map[manifest.NamedResource]struct{}) []manifest.Warning {
	model := report.Build(res.Failed, res.Blocked, nil, nil)
	covered := map[manifest.NamedResource]bool{}
	var warnings []manifest.Warning
	children := func(ids []manifest.NamedResource) []string {
		var out []string
		for _, id := range ids {
			if _, ok := unrelated[id]; ok {
				out = append(out, id.String())
				covered[id] = true
			}
		}
		return out
	}
	for _, root := range model.Primary {
		if _, ok := unrelated[root.ID]; !ok {
			continue
		}
		covered[root.ID] = true
		warnings = append(warnings, manifest.Warning{Resource: root.ID, Message: root.Msg, Detail: children(root.Blocks)})
	}
	for _, root := range model.Missing {
		if _, needed := required[root.ID]; needed {
			continue
		}
		if detail := children(root.RequiredBy); len(detail) > 0 {
			warnings = append(warnings, manifest.Warning{Resource: root.ID, Message: "not found; required by", Detail: detail})
		}
	}
	// Closed cycles have no report root; shared fatal roots must not be
	// reclassified as warnings. Keep each remaining child's own diagnostic.
	for id, info := range unrelated {
		if covered[id] {
			continue
		}
		deps := slices.Clone(res.Blocked[id])
		slices.SortFunc(deps, manifest.NamedResource.Compare)
		var detail []string
		for _, dep := range slices.Compact(deps) {
			detail = append(detail, "blocked by "+dep.String())
		}
		warnings = append(warnings, manifest.Warning{Resource: id, Message: info.Message, Detail: detail})
	}
	slices.SortFunc(warnings, func(a, b manifest.Warning) int { return a.Resource.Compare(b.Resource) })
	return warnings
}
