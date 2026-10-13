package orchestrator

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/selection"
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
// The second return retains selected failures and non-resource errors; the
// third contains only non-resource errors for diagnostics outside failure rows.
func Select(runs []Rendered, runErr error, roots []manifest.NamedResource, labels map[string]string) ([]*Result, error, error) {
	if len(runs) < 1 || len(runs) > 2 {
		err := errors.Join(fmt.Errorf("%w: selection requires one or two runs", manifest.ErrInput), runErr)
		return nil, err, nonResourceErrors(err)
	}
	for _, run := range runs {
		if run.Result == nil || run.Orchestrator == nil {
			if runErr != nil {
				return nil, runErr, nonResourceErrors(runErr)
			}
			if run.Err != nil {
				return nil, run.Err, nonResourceErrors(run.Err)
			}
			err := fmt.Errorf("%w: selection requires completed runs", manifest.ErrInput)
			return nil, err, err
		}
	}
	kinds, err := validateRoots(roots)
	if err != nil {
		err = errors.Join(err, runErr)
		return nil, err, nonResourceErrors(err)
	}
	sides := make([]selectionSide, len(runs))
	selected := make([]*Result, len(runs))
	for i, run := range runs {
		sides[i].run = run
		sides[i].collect(roots, selector.Metadata{Labels: labels}, len(runs) == 2)
		sides[i].collectWarnings(roots[0].Name, kinds, len(runs) == 2)
		selected[i] = sides[i].result
	}
	candidates := selectionCandidates(sides)
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
	selectedErr, remainder := selectionError(sides, runErr)
	return selected, selectedErr, remainder
}

func validateRoots(roots []manifest.NamedResource) (map[string]bool, error) {
	if len(roots) == 0 {
		return nil, fmt.Errorf("%w: selection requires requested kinds", manifest.ErrInput)
	}
	kinds := make(map[string]bool, len(roots))
	for _, root := range roots {
		if root.Kind == "" || kinds[root.Kind] || root.Namespace != roots[0].Namespace || root.Name != roots[0].Name {
			return nil, fmt.Errorf("%w: selection kinds must be distinct with a common namespace and name", manifest.ErrInput)
		}
		kinds[root.Kind] = true
	}
	return kinds, nil
}

func (s *selectionSide) collect(roots []manifest.NamedResource, metadata selector.Metadata, comparison bool) {
	if namespace := roots[0].Namespace; namespace != "" {
		s.namespaces = map[string]struct{}{namespace: {}}
	} else if s.run.filter.Enabled() {
		s.namespaces = s.run.filter.KeepNamespaces()
	}
	s.result = &Result{Counts: make(map[string]int, len(roots))}
	for _, root := range roots {
		for _, obj := range s.run.store.ListObjects(root.Kind) {
			id := obj.Named()
			if comparison {
				if ks, ok := obj.(*manifest.Kustomization); ok {
					s.prepareExternal(root, ks)
				}
			}
			if (root.Name != "" && id.Name != root.Name) || !s.includes(id.Namespace) {
				continue
			}
			s.result.Counts[root.Kind]++
			s.roots = append(s.roots, id)
			if !metadata.Matches(obj) {
				continue
			}
			s.result.Objects = append(s.result.Objects, obj)
			if docs, ok := s.run.Result.Manifests[id]; ok {
				if s.result.Manifests == nil {
					s.result.Manifests = make(map[manifest.NamedResource][]map[string]any)
				}
				s.result.Manifests[id] = docs
			}
			if version := s.chartVersion(obj); version != "" {
				if s.result.ChartVersions == nil {
					s.result.ChartVersions = make(map[manifest.NamedResource]string)
				}
				s.result.ChartVersions[id] = version
			}
		}
	}
}

func (s *selectionSide) chartVersion(obj manifest.BaseManifest) string {
	hr, ok := obj.(*manifest.HelmRelease)
	if !ok || hr.Chart.Version != "" {
		return ""
	}
	src := manifest.NamedResource{Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName}
	switch source := s.run.store.GetObject(src).(type) {
	case *manifest.OCIRepository:
		if source.Reference != nil {
			return cmp.Or(source.Reference.Digest, source.Reference.SemVer, source.Reference.Tag)
		}
	case *manifest.HelmChartSource:
		return source.Version
	}
	return ""
}

func (s *selectionSide) collectWarnings(name string, kinds map[string]bool, comparison bool) {
	for _, warning := range s.run.Result.Warnings {
		if warning.Resource != (manifest.NamedResource{}) && !s.includes(warning.Resource.Namespace) {
			continue
		}
		if comparison && warning.Category == manifest.WarnOmittedValues &&
			(!kinds[warning.Resource.Kind] || (name != "" && warning.Resource.Name != name)) {
			continue
		}
		s.result.Warnings = append(s.result.Warnings, warning)
	}
}

func selectionCandidates(sides []selectionSide) []manifest.NamedResource {
	if len(sides) != 2 {
		return nil
	}
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
	return slices.SortedFunc(maps.Keys(producers), manifest.NamedResource.Compare)
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
	local := make(map[manifest.NamedResource]store.StatusInfo, len(candidates))
	var counterpart map[manifest.NamedResource]store.StatusInfo
	var path []manifest.NamedResource
	for _, id := range candidates {
		owner, resolved := s.run.Orchestrator, local
		if s.run.store.GetObject(id) == nil {
			// Absent producers follow the other snapshot's ancestry, which MUST
			// remain separate from the current snapshot's resolved walks.
			if counterpart == nil {
				counterpart = make(map[manifest.NamedResource]store.StatusInfo)
			}
			owner, resolved = other.run.Orchestrator, counterpart
		}
		var unavailable store.StatusInfo
		unavailable, path = s.walkUnavailable(id, owner, resolved, path)
		if selection.IsUnavailable(unavailable) {
			if s.result.Unavailable == nil {
				s.result.Unavailable = make(map[manifest.NamedResource]store.StatusInfo)
			}
			s.result.Unavailable[id] = unavailable
		}
	}
}

func (s *selectionSide) walkUnavailable(
	id manifest.NamedResource,
	owner *Orchestrator,
	resolved map[manifest.NamedResource]store.StatusInfo,
	path []manifest.NamedResource,
) (store.StatusInfo, []manifest.NamedResource) {
	path = path[:0]
	var unavailable store.StatusInfo
	for {
		if info, ok := resolved[id]; ok {
			unavailable = info
			break
		}
		// A zero cache entry stops ownership cycles while the path is unresolved.
		resolved[id] = store.StatusInfo{}
		path = append(path, id)
		info, failed := s.run.Result.Failed[id]
		if !failed {
			info, _ = s.run.store.GetStatus(id)
		}
		if selection.IsUnavailable(info) {
			unavailable = info
			break
		}
		parent, ok := owner.parentOf(id)
		if !ok {
			break
		}
		id = parent
	}
	for _, parent := range path {
		resolved[parent] = unavailable
	}
	return unavailable, path
}
