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
// Unchanged catalog-only objects contain reporting facts, not complete specs; select them for render to obtain full specs.
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
		complete = complete && (run.Absent || run.Result != nil && run.Orchestrator != nil)
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
			for _, namespace := range run.changeNamespaces {
				if sides[i].namespaces == nil {
					sides[i].namespaces = map[string]struct{}{}
				}
				sides[i].namespaces[namespace] = struct{}{}
			}
		}
	}
	for i, run := range runs {
		var other *selectionSide
		if len(sides) == 2 {
			other = &sides[1-i]
		}
		if complete && !run.Absent {
			sides[i].collect(roots, labels, other)
		} else {
			sides[i].result = &selection.Selection{}
		}
		sides[i].collectWarnings(roots[0], kinds, len(runs) == 2)
		if run.Orchestrator != nil {
			sides[i].prepareSubjects()
		}

		selected[i] = sides[i].result
	}
	if !complete {
		return selected, runErr, runErr
	}
	candidates := selectionCandidates(sides)
	for i := range sides {
		side := &sides[i]
		if side.run.Absent {
			continue
		}
		var other *selectionSide
		if len(sides) == 2 && !sides[1-i].run.Absent {
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

func (s *selectionSide) collect(roots []manifest.NamedResource, labels map[string]string, other *selectionSide) {
	s.result = &selection.Selection{Counts: make(map[string]int, len(roots))}
	if binding := s.run.entryBinding; binding.Producer != (manifest.NamedResource{}) {
		s.result.ProducerKeys = map[manifest.NamedResource]manifest.NamedResource{binding.Producer: binding.Key}
	}
	var emitters []manifest.BaseManifest
	for _, root := range roots {
		objects := s.run.store.ListObjects(root.Group, root.Kind)
		stored := len(objects)
		for id, record := range s.run.index.Files() {
			if id.Group == root.Group && id.Kind == root.Kind {
				if obj := s.run.unchangedDeclaration(id, record); obj != nil {
					objects = append(objects, obj)
				}
			}
		}
		if len(objects) != stored {
			slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
		}
		for _, obj := range objects {
			id := obj.Named()
			if other != nil {
				s.prepareExternal(root, obj)
			}
			if id.ReportHidden() && manifest.KindOf(obj).Render {
				if root.Name == "" && manifest.KindOf(obj).Render {
					s.roots = append(s.roots, id)
					emitters = append(emitters, obj)
				}
				continue
			}
			if (root.Name != "" && id.Name != root.Name) || !s.includes(id.Namespace) {
				if root.Name == "" && s.run.filter.Enabled() && s.run.filter.ShouldReconcile(id) {
					emitters = append(emitters, obj)
				}
				continue
			}
			if s.run.filter.Enabled() && !manifest.KindOf(obj).Discovery {
				_, selected, _, _ := s.run.filter.Membership(id)
				if !selected && s.run.store.GetObject(id) != nil {
					parent, owned := s.run.parentOf(id)
					_, parentSelected, _, _ := s.run.filter.Membership(parent)
					if !owned || !parentSelected {
						continue
					}
				}
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
	scoped := s.namespaces != nil
	if other != nil && !other.run.Absent {
		scoped = scoped && other.namespaces != nil
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
	for _, objects := range [2][]manifest.BaseManifest{s.result.Objects, emitters} {
		for _, obj := range objects {
			i, ok := positions[obj.Named()]
			if !ok {
				continue
			}
			batch := s.run.Result.Manifests[i]
			var counterpart []map[string]any
			counterpartKnown := false
			selectedOutput := func(id manifest.NamedResource) bool {
				_, selected, _, _ := s.run.filter.Membership(id)
				if selected || other == nil || other.run.Absent {
					return selected
				}
				if _, selected, _, _ := other.run.filter.Membership(id); !selected {
					return false
				}
				if !counterpartKnown {
					for _, batch := range other.run.Result.Manifests {
						if batch.Parent != nil && batch.Parent.Named() == obj.Named() {
							counterpart = batch.Documents
							break
						}
					}
					counterpartKnown = true
				}
				return slices.ContainsFunc(counterpart, func(doc map[string]any) bool {
					return manifest.DocIdentity(doc) == id
				})
			}
			var policy manifest.EmissionPolicy
			if producer, ok := obj.(interface {
				EmissionPolicy() manifest.EmissionPolicy
			}); ok {
				policy = producer.EmissionPolicy()
			}
			if policy.DocumentScope {
				compared := other == nil || other.run.Absent || other.run.filter.ShouldReconcile(other.actualProducer(s.comparisonKey(obj.Named())))
				batch.Documents = nil
				for _, doc := range s.run.Result.Manifests[i].Documents {
					if s.run.filter.Enabled() && !scoped {
						id := manifest.DocIdentity(doc)
						declaration := compared && slices.ContainsFunc(roots, func(root manifest.NamedResource) bool { return root.Group == id.Group && root.Kind == id.Kind })
						if !declaration && !selectedOutput(id) {
							continue
						}
					}
					selected := policy.SelectOutput(doc, roots, s.namespaces, labels)
					if !selected && other != nil && !other.run.Absent {
						selected = policy.SelectOutput(doc, roots, other.namespaces, labels)
					}
					if selected {
						batch.Documents = append(batch.Documents, doc)
					}
				}
			}
			if s.run.filter.Enabled() && !policy.DocumentScope {
				if _, selected, _, _ := s.run.filter.Membership(obj.Named()); !selected {
					documents := []map[string]any{}
					for _, doc := range batch.Documents {
						if selectedOutput(manifest.DocIdentity(doc)) {
							documents = append(documents, doc)
						}
					}
					batch.Documents = documents
				}
			}
			if len(batch.Documents) != 0 {
				s.result.Manifests = append(s.result.Manifests, batch)
			}
		}
	}
}

func (o *Orchestrator) unchangedDeclaration(id manifest.NamedResource, record manifest.DiscoveryRecord) manifest.BaseManifest {
	restricted := o.filter.Enabled()
	if restricted {
		if o.filter.ShouldReconcile(id) || !o.cfg.Selection.Matches(id) || !o.loader.AdmitsRoot(o.loader.CatalogObject(id), record.Path) {
			return nil
		}
	} else if record.Declaration == nil {
		return nil
	}
	if o.store.GetObject(id) != nil {
		return nil
	}
	if restricted {
		return o.loader.CatalogObject(id)
	}
	parent, ok := o.parentOf(id)
	status, known := o.store.GetStatus(parent)
	if !ok || !known || status.Outcome() != store.OutcomeUnchanged {
		return nil
	}
	return record.Declaration
}

func (o *Orchestrator) statusOf(id manifest.NamedResource) (store.StatusInfo, bool) {
	if info, known := o.store.GetStatus(id); known {
		return info, true
	}
	if o.filter.Enabled() && !o.filter.ShouldReconcile(id) {
		_, indexed := o.index.Lookup(id)
		if o.store.GetObject(id) != nil || indexed && o.cfg.Selection.Matches(id) {
			return store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"}, true
		}
	}
	if parent, found := o.parentOf(id); found {
		if inherited, known := o.store.GetStatus(parent); known && inherited.Outcome() == store.OutcomeUnchanged {
			return inherited, true
		}
	}
	return store.StatusInfo{}, false
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
	if owner, ok := obj.(interface{ ChartVersion() string }); ok {
		if version := owner.ChartVersion(); version != "" {
			return version
		}
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
	s.result.ClusterWarnings = s.run.Result.ClusterWarnings
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
		for _, id := range side.roots {
			if id.KindInfo().Render {
				producers[side.comparisonKey(id)] = struct{}{}
			}
		}
		for _, obj := range side.result.Objects {
			id := obj.Named()
			if manifest.KindOf(obj).Render {
				producers[side.comparisonKey(id)] = struct{}{}
			}
		}
		for _, batch := range side.result.Manifests {
			if batch.Parent != nil {
				producers[side.comparisonKey(batch.Parent.Named())] = struct{}{}
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
	info := s.status(id)
	if info.Outcome() != store.OutcomeUnchanged && info.Outcome() != store.OutcomeSkipped {
		return
	}
	if slices.ContainsFunc(s.run.Result.Warnings, func(w manifest.Warning) bool {
		return w.Resource == src && w.Category == manifest.WarnSourceCredentialsUnavailable
	}) {
		return
	}
	s.result.External = append(s.result.External, manifest.Warning{Resource: id, Subject: s.result.Diagnostic(id), Related: []manifest.NamedResource{src}, Category: manifest.WarnExternalSource, Message: "external source " + s.run.diagnostic(src).String() + " is outside the local tree; not rendered"})
}

func (s *selectionSide) prepareUnavailable(other *selectionSide, candidates []manifest.NamedResource) {
	local := make(map[manifest.NamedResource]store.StatusInfo, len(candidates))
	var counterpart map[manifest.NamedResource]store.StatusInfo
	var path []manifest.NamedResource
	for _, id := range candidates {
		actual := s.actualProducer(id)
		var unavailable store.StatusInfo
		if s.run.filter.Enabled() {
			unavailable = s.localStatus(actual)
		}
		if !unavailable.IsUnavailable() {
			owner, resolved := s.run.Orchestrator, local
			_, selected, _, _ := s.run.filter.Membership(actual)
			if s.run.store.GetObject(actual) == nil || s.run.filter.Enabled() && !selected {
				// Producers outside this projection MUST use the other snapshot's ancestry cache.
				if counterpart == nil {
					counterpart = make(map[manifest.NamedResource]store.StatusInfo)
				}
				owner, resolved = other.run.Orchestrator, counterpart
			}
			start := actual
			if owner != s.run.Orchestrator {
				start = other.actualProducer(id)
			}
			unavailable, path = s.walkUnavailable(start, owner, resolved, path)
		}
		if unavailable.IsUnavailable() {
			if s.result.Unavailable == nil {
				s.result.Unavailable = make(map[manifest.NamedResource]store.StatusInfo)
			}
			s.result.Unavailable[id] = unavailable
		}
	}
}

func (s *selectionSide) localStatus(id manifest.NamedResource) store.StatusInfo {
	if info, failed := s.run.Result.Failed[id]; failed {
		return info
	}
	info, _ := s.run.store.GetStatus(id)
	return info
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
		actual := id
		if binding := owner.entryBinding; binding.Producer == id {
			actual = s.actualProducer(binding.Key)
		}
		info := s.localStatus(actual)
		if info.IsUnavailable() {
			unavailable = info
			break
		}
		ancestry := owner
		if actual != id {
			ancestry = s.run.Orchestrator
		}
		parent, ok := ancestry.parentOf(actual)
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

func (s *selectionSide) comparisonKey(id manifest.NamedResource) manifest.NamedResource {
	if key, ok := s.result.ProducerKeys[id]; ok {
		return key
	}
	return id
}

func (s *selectionSide) actualProducer(key manifest.NamedResource) manifest.NamedResource {
	if binding := s.run.entryBinding; binding.Producer != (manifest.NamedResource{}) && binding.Key == key {
		return binding.Producer
	}
	return key
}

func (s *selectionSide) prepareSubjects() {
	add := func(subject manifest.DiagnosticIdentity) {
		if subject.Resource == subject.ID && subject.Path == "" {
			return
		}
		if s.result.Subjects == nil {
			s.result.Subjects = make(map[manifest.NamedResource]manifest.DiagnosticIdentity)
		}
		s.result.Subjects[subject.ID] = subject
	}
	for _, obj := range s.result.Objects {
		add(manifest.DiagnosticFor(obj.Named(), obj))
	}
	for _, batch := range s.result.Manifests {
		if batch.Parent != nil {
			add(manifest.DiagnosticFor(batch.Parent.Named(), batch.Parent))
		}
	}
	if s.run.Result == nil {
		return
	}
	for id := range s.run.Result.Failed {
		add(s.run.diagnostic(id))
	}
	for id, deps := range s.run.Result.Blocked {
		add(s.run.diagnostic(id))
		for _, dep := range deps {
			add(s.run.diagnostic(dep))
		}
	}
	for _, warning := range s.run.Result.Warnings {
		if warning.Subject.ID != (manifest.NamedResource{}) {
			add(warning.Subject)
		} else if warning.Resource != (manifest.NamedResource{}) {
			add(s.run.diagnostic(warning.Resource))
		}
		for _, id := range warning.Related {
			add(s.run.diagnostic(id))
		}
	}
}

func (s *selectionSide) status(id manifest.NamedResource) store.StatusInfo {
	if info, ok := s.run.statusOf(id); ok {
		return info
	}
	if s.run.filter.Enabled() && !s.run.filter.ShouldReconcile(id) {
		return store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"}
	}
	return store.StatusInfo{Status: store.StatusFailed, Message: "no status reported"}
}
