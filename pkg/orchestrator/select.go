package orchestrator

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type selectionSide struct {
	run        Rendered
	result     *selection.Selection
	namespaces map[string]struct{}
	roots      []manifest.NamedResource
}

// Select projects one render, or two ordered orig/current renders,
// onto requested kinds with a common namespace and name. Labels filter output
// objects, while Counts and fatal scope retain the roots matched before labels.
// Incomplete inputs yield only warnings and their original errors.
// Borrowed objects, graphs and document batches MUST remain immutable in use.
// The second return retains selected failures and non-resource errors; the
// third contains only non-resource errors for diagnostics outside failure rows.
func Select(runs []Rendered, runErr error, roots []manifest.NamedResource, labels map[string]string) ([]*selection.Selection, error, error) {
	if len(runs) < 1 || len(runs) > 2 {
		err := errors.Join(fmt.Errorf("%w: selection requires one or two runs", manifest.ErrInput), runErr)
		return nil, err, nonResourceErrors(err)
	}
	kinds, err := validateRoots(roots)
	if err != nil {
		err = errors.Join(err, runErr)
		return nil, err, nonResourceErrors(err)
	}
	complete := true
	for _, run := range runs {
		complete = complete && run.Result != nil && run.Orchestrator != nil
	}
	if !complete {
		for _, run := range runs {
			if run.Err != nil && !errors.Is(runErr, run.Err) {
				runErr = errors.Join(runErr, run.Err)
			}
		}
		if runErr == nil {
			runErr = fmt.Errorf("%w: selection requires completed runs", manifest.ErrInput)
		}
	}
	sides := make([]selectionSide, len(runs))
	selected := make([]*selection.Selection, len(runs))
	for i, run := range runs {
		sides[i].run = run
		if namespace := roots[0].Namespace; namespace != "" {
			sides[i].namespaces = map[string]struct{}{namespace: {}}
		} else if run.Orchestrator != nil && run.filter.Enabled() {
			sides[i].namespaces = run.filter.KeepNamespaces()
		}
		if complete {
			sides[i].collect(roots, labels, len(runs) == 2)
		} else {
			sides[i].result = &selection.Selection{}
		}
		sides[i].collectWarnings(roots[0], kinds, len(runs) == 2)
		selected[i] = sides[i].result
	}
	if !complete {
		return selected, runErr, runErr
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

func validateRoots(roots []manifest.NamedResource) (map[manifest.NamedResource]bool, error) {
	if len(roots) == 0 {
		return nil, fmt.Errorf("%w: selection requires requested kinds", manifest.ErrInput)
	}
	kinds := make(map[manifest.NamedResource]bool, len(roots))
	for _, root := range roots {
		if root.Kind == "" || kinds[manifest.NamedResource{Group: root.Group, Kind: root.Kind}] || root.Namespace != roots[0].Namespace || root.Name != roots[0].Name {
			return nil, fmt.Errorf("%w: selection kinds must be distinct with a common namespace and name", manifest.ErrInput)
		}
		kinds[manifest.NamedResource{Group: root.Group, Kind: root.Kind}] = true
	}
	return kinds, nil
}

func (s *selectionSide) collect(roots []manifest.NamedResource, labels map[string]string, comparison bool) {
	s.result = &selection.Selection{Counts: make(map[string]int, len(roots))}
	for _, root := range roots {
		for _, obj := range s.run.store.ListObjects(root.Group, root.Kind) {
			id := obj.Named()
			if comparison {
				s.prepareExternal(root, obj)
			}
			if (root.Name != "" && id.Name != root.Name) || !s.includes(id.Namespace) {
				continue
			}
			s.result.Counts[root.Kind]++
			s.roots = append(s.roots, id)
			if !matchesLabels(obj, labels) {
				continue
			}
			s.result.Objects = append(s.result.Objects, obj)
			if version := s.chartVersion(obj); version != "" {
				if s.result.ChartVersions == nil {
					s.result.ChartVersions = make(map[manifest.NamedResource]string)
				}
				s.result.ChartVersions[id] = version
			}
		}
	}
	positions := make(map[manifest.NamedResource]int, len(s.run.Result.Manifests))
	for i, batch := range s.run.Result.Manifests {
		if batch.Parent != nil {
			positions[batch.Parent.Named()] = i
		}
	}
	if len(s.run.Result.Manifests) != 0 {
		s.result.Manifests = make([]manifest.DocumentBatch, 0, len(s.result.Objects)+1)
	}
	for _, obj := range s.result.Objects {
		if i, ok := positions[obj.Named()]; ok {
			s.result.Manifests = append(s.result.Manifests, s.run.Result.Manifests[i])
		}
	}
	requestedRenderKinds := 0
	for _, root := range roots {
		if root.KindInfo().Render {
			requestedRenderKinds++
		}
	}
	all := roots[0].Name == "" && requestedRenderKinds == len(reconcilableKinds)
	for _, batch := range s.run.Result.Manifests {
		if batch.Parent != nil {
			continue
		}
		var documents []map[string]any
		for _, doc := range batch.Documents {
			id := manifest.DocIdentity(doc)
			if !s.includes(id.Namespace) {
				continue
			}
			matched := all
			for _, root := range roots {
				if id.Group == root.Group && id.Kind == root.Kind && (root.Name == "" || id.Name == root.Name) {
					matched = true
				}
			}
			metadata, _ := doc["metadata"].(map[string]any)
			docLabels, _ := metadata["labels"].(map[string]any)
			for key, value := range labels {
				if actual, ok := docLabels[key]; !ok || actual != value {
					matched = false
					break
				}
			}
			if matched {
				documents = append(documents, doc)
			}
		}
		if len(documents) != 0 {
			s.result.Manifests = append(s.result.Manifests, manifest.DocumentBatch{Documents: documents})
		}
	}
}

func matchesLabels(obj manifest.BaseManifest, filter map[string]string) bool {
	if obj == nil {
		return false
	}
	if len(filter) == 0 {
		return true
	}
	var labels map[string]string
	if o, ok := obj.(interface{ GetLabels() map[string]string }); ok {
		labels = o.GetLabels()
	}
	for k, v := range filter {
		// A missing label MUST NOT match an explicitly empty filter value.
		got, ok := labels[k]
		if !ok || got != v {
			return false
		}
	}
	return true
}

func (s *selectionSide) chartVersion(obj manifest.BaseManifest) string {
	src, ok := obj.ChartDisplaySource()
	if !ok {
		return ""
	}
	if chart, ok := s.run.store.GetObject(src).(interface{ ChartVersion() string }); ok {
		return chart.ChartVersion()
	}
	return ""
}

func (s *selectionSide) collectWarnings(root manifest.NamedResource, kinds map[manifest.NamedResource]bool, comparison bool) {
	if s.run.Result == nil {
		return
	}
	for _, warning := range s.run.Result.Warnings {
		if warning.Category == manifest.WarnIncompleteInputCoverage {
			id := warning.Resource
			if (s.run.Orchestrator == nil || !s.run.filter.Enabled()) || !kinds[manifest.NamedResource{Group: id.Group, Kind: id.Kind}] || root.Name != "" && id.Name != root.Name || root.Namespace != "" && id.Namespace != "" && id.Namespace != root.Namespace {
				continue
			}
		} else if warning.Resource != (manifest.NamedResource{}) && !s.includes(warning.Resource.Namespace) {
			continue
		}
		if comparison && warning.Category == manifest.WarnOmittedValues &&
			(!kinds[manifest.NamedResource{Group: warning.Resource.Group, Kind: warning.Resource.Kind}] || (root.Name != "" && warning.Resource.Name != root.Name)) {
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
			if manifest.KindOf(obj).Render {
				producers[id] = struct{}{}
			}
		}
		for _, batch := range side.result.Manifests {
			if batch.Parent != nil {
				producers[batch.Parent.Named()] = struct{}{}
			}
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

func (s *selectionSide) prepareExternal(root manifest.NamedResource, obj manifest.BaseManifest) {
	src, _, _ := obj.SourcePath()
	id := obj.Named()
	if (root.Name != "" && id.Name != root.Name) || (root.Namespace != "" && id.Namespace != "" && id.Namespace != root.Namespace) {
		return
	}
	if _, _, external := loader.SourceView(s.run.store, src, s.run.repoRoot); !external {
		return
	}
	info, _ := s.run.store.GetStatus(id)
	if info.Outcome() != store.OutcomeUnchanged && info.Outcome() != store.OutcomeSkipped {
		return
	}
	s.result.External = append(s.result.External, manifest.Warning{Resource: id, Category: manifest.WarnExternalSource, Message: "external source " + src.String() + " is outside the local tree; not rendered"})
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
		if unavailable.IsUnavailable() {
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
		if info.IsUnavailable() {
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
