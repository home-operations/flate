package orchestrator

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selector"
	"github.com/home-operations/flate/pkg/store"
)

type selectionSide struct {
	run        Rendered
	result     *Result
	namespaces map[string]struct{}
	roots      []manifest.NamedResource
}

// Select projects one completed render, or two ordered orig/current renders,
// onto requested kinds with a common namespace and name. Labels filter output
// objects, while Counts and fatal scope retain the roots matched before labels.
// Borrowed objects, graphs and document batches MUST remain immutable in use.
func Select(runs []Rendered, runErr error, roots []manifest.NamedResource, labels map[string]string) ([]*Result, error) {
	if len(runs) < 1 || len(runs) > 2 {
		return nil, errors.Join(fmt.Errorf("%w: selection requires one or two runs", manifest.ErrInput), runErr)
	}
	for _, run := range runs {
		if run.Result == nil || run.Orchestrator == nil {
			if runErr != nil {
				return nil, runErr
			}
			if run.Err != nil {
				return nil, run.Err
			}
			return nil, fmt.Errorf("%w: selection requires completed runs", manifest.ErrInput)
		}
	}
	if len(roots) == 0 {
		return nil, errors.Join(fmt.Errorf("%w: selection requires requested kinds", manifest.ErrInput), runErr)
	}
	kinds := make(map[string]bool, len(roots))
	for _, root := range roots {
		if root.Kind == "" || kinds[root.Kind] || root.Namespace != roots[0].Namespace || root.Name != roots[0].Name {
			return nil, errors.Join(fmt.Errorf("%w: selection kinds must be distinct with a common namespace and name", manifest.ErrInput), runErr)
		}
		kinds[root.Kind] = true
	}
	sides := make([]selectionSide, len(runs))
	selected := make([]*Result, len(runs))
	for i, run := range runs {
		side := &sides[i]
		side.run = run
		if namespace := roots[0].Namespace; namespace != "" {
			side.namespaces = map[string]struct{}{namespace: {}}
		} else if run.filter.Enabled() {
			side.namespaces = run.filter.KeepNamespaces()
			if len(side.namespaces) == 0 {
				side.namespaces = nil
			}
		}
		side.result = &Result{Counts: make(map[string]int, len(roots)), DependsOn: run.Result.DependsOn, Orphans: run.Result.Orphans}
		selected[i] = side.result
		metadata := selector.Metadata{Labels: labels}
		for _, root := range roots {
			for _, obj := range run.store.ListObjects(root.Kind) {
				id := obj.Named()
				if len(runs) == 2 {
					if ks, ok := obj.(*manifest.Kustomization); ok {
						side.prepareExternal(root, ks)
					}
				}
				if (root.Name != "" && id.Name != root.Name) || !side.includes(id.Namespace) {
					continue
				}
				side.result.Counts[root.Kind]++
				side.roots = append(side.roots, id)
				if !metadata.Matches(obj) {
					continue
				}
				side.result.Objects = append(side.result.Objects, obj)
				if docs, ok := run.Result.Manifests[id]; ok {
					if side.result.Manifests == nil {
						side.result.Manifests = make(map[manifest.NamedResource][]map[string]any)
					}
					side.result.Manifests[id] = docs
				}
				if hr, ok := obj.(*manifest.HelmRelease); ok && hr.Chart.Version == "" {
					src := manifest.NamedResource{Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName}
					var version string
					switch source := run.store.GetObject(src).(type) {
					case *manifest.OCIRepository:
						if source.Reference != nil {
							version = cmp.Or(source.Reference.Digest, source.Reference.SemVer, source.Reference.Tag)
						}
					case *manifest.HelmChartSource:
						version = source.Version
					}
					if version != "" {
						if side.result.ChartVersions == nil {
							side.result.ChartVersions = make(map[manifest.NamedResource]string)
						}
						side.result.ChartVersions[id] = version
					}
				}
			}
		}
		for _, warning := range run.Result.Warnings {
			if warning.Resource != (manifest.NamedResource{}) && !side.includes(warning.Resource.Namespace) {
				continue
			}
			if len(runs) == 2 && warning.Category == manifest.WarnOmittedValues &&
				(!kinds[warning.Resource.Kind] || (roots[0].Name != "" && warning.Resource.Name != roots[0].Name)) {
				continue
			}
			side.result.Warnings = append(side.result.Warnings, warning)
		}
	}
	var candidates []manifest.NamedResource
	if len(sides) == 2 {
		producers := make(map[manifest.NamedResource]struct{})
		for _, side := range sides {
			for _, obj := range side.result.Objects {
				id := obj.Named()
				if id.Kind == manifest.KindKustomization || id.Kind == manifest.KindHelmRelease {
					producers[id] = struct{}{}
				}
			}
			for id := range side.result.Manifests {
				producers[id] = struct{}{}
			}
		}
		candidates = slices.SortedFunc(maps.Keys(producers), manifest.NamedResource.Compare)
	}
	for i := range sides {
		side := &sides[i]
		var other *selectionSide
		if len(sides) == 2 {
			other = &sides[1-i]
		}
		side.prepareFailures(roots, other)
		side.prepareStatuses(roots[0].Name)
		if other != nil {
			side.prepareUnavailable(other, candidates)
		}
	}
	return selected, selectionError(sides, runErr)
}

func (s *selectionSide) includes(namespace string) bool {
	if namespace == "" || s.namespaces == nil {
		return true
	}
	_, ok := s.namespaces[namespace]
	return ok
}

func (s *selectionSide) prepareExternal(root manifest.NamedResource, ks *manifest.Kustomization) {
	id := ks.Named()
	if (root.Name != "" && id.Name != root.Name) || (root.Namespace != "" && id.Namespace != "" && id.Namespace != root.Namespace) {
		return
	}
	if _, _, external := loader.SourceView(s.run.store, ks, s.run.repoRoot); !external {
		return
	}
	info, _ := s.run.store.GetStatus(id)
	if !store.IsUnchanged(info) && !store.IsSkipped(info) {
		return
	}
	src := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	s.result.External = append(s.result.External, manifest.Warning{Resource: id, Message: "external source " + src.String() + " is outside the local tree; not rendered"})
}

func (s *selectionSide) prepareUnavailable(other *selectionSide, candidates []manifest.NamedResource) {
	resolved := [2]map[manifest.NamedResource]store.StatusInfo{make(map[manifest.NamedResource]store.StatusInfo, len(candidates)), nil}
	visited := make(map[manifest.NamedResource]bool)
	var path []manifest.NamedResource
	for _, id := range candidates {
		owner, index := s.run.Orchestrator, 0
		if s.run.store.GetObject(id) == nil {
			owner, index = other.run.Orchestrator, 1
		}
		// Absent producers follow the other snapshot's ancestry, which MUST
		// remain separate from the current snapshot's resolved walks.
		if resolved[index] == nil {
			resolved[index] = make(map[manifest.NamedResource]store.StatusInfo)
		}
		clear(visited)
		path = path[:0]
		var unavailable store.StatusInfo
		for parent, ok := id, true; ok && !visited[parent]; parent, ok = owner.parentOf(parent) {
			if info, ok := resolved[index][parent]; ok {
				unavailable = info
				break
			}
			visited[parent] = true
			path = append(path, parent)
			info, failed := s.run.Result.Failed[parent]
			if !failed {
				info, _ = s.run.store.GetStatus(parent)
			}
			if info.Status == store.StatusFailed || store.IsSkipped(info) || store.IsSuspended(info) {
				unavailable = info
				break
			}
		}
		for _, parent := range path {
			resolved[index][parent] = unavailable
		}
		if unavailable.Status == store.StatusFailed || store.IsSkipped(unavailable) || store.IsSuspended(unavailable) {
			if s.result.Unavailable == nil {
				s.result.Unavailable = make(map[manifest.NamedResource]store.StatusInfo)
			}
			s.result.Unavailable[id] = unavailable
		}
	}
}
