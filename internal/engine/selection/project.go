package selection

import (
	"errors"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

const comparisonSides = 2

type selectionSide struct {
	run        Snapshot
	request    Request
	reads      store.ReadSet
	result     *Result
	namespaces map[string]struct{}
	roots      []manifest.NamedResource
}

// Project projects one render, or two ordered orig/current renders,
// onto requested kinds with a common namespace and name. Labels filter output
// objects, while Counts and fatal scope retain the roots matched before labels.
// Incomplete inputs yield only warnings and their original errors.
// Unchanged catalog-only objects contain reporting facts, not complete specs; select them for render to obtain full specs.
// Borrowed objects, graphs and document batches MUST remain immutable in use.
// ProjectionErrors separates selected failures from non-resource diagnostics.
func Project(
	runs []Snapshot,
	runErr error,
	request Request,
) ([]*Result, ProjectionErrors) {
	roots := request.Roots
	if len(runs) < 1 || len(runs) > comparisonSides {
		err := errors.Join(manifest.NewError(manifest.ErrInput, nil, "selection requires one or two runs"), runErr)
		return nil, ProjectionErrors{Selected: err, Remaining: nonResourceErrors(err)}
	}
	kinds, err := validateRoots(roots)
	if err != nil {
		err = errors.Join(err, runErr)
		return nil, ProjectionErrors{Selected: err, Remaining: nonResourceErrors(err)}
	}
	complete := true
	for i := range runs {
		run := &runs[i]
		complete = complete && (run.Absent || run.Complete)
	}
	if !complete {
		runErr = incompleteSelectionError(runs, runErr)
	}
	sides := prepareProjectionSides(runs, request)
	defer func() {
		for i := range sides {
			if sides[i].run.Facts != nil && sides[i].run.Store != nil {
				sides[i].reads.Retire()
			}
		}
	}()
	selected, err := collectProjectionSides(sides, request, kinds, complete)
	if err != nil {
		err = errors.Join(err, runErr)
		return nil, ProjectionErrors{Selected: err, Remaining: nonResourceErrors(err)}
	}
	if !complete {
		return selected, ProjectionErrors{Selected: runErr, Remaining: runErr}
	}
	candidates := selectionCandidates(sides)
	prepareSelectionFailures(sides, roots, candidates)
	projection := selectionError(sides, runErr)
	return selected, projection
}

func collectProjectionSides(
	sides []selectionSide,
	request Request,
	kinds map[manifest.NamedResource]bool,
	complete bool,
) ([]*Result, error) {
	if err := validateProjectionBatches(sides, complete); err != nil {
		return nil, err
	}
	selected := make([]*Result, len(sides))
	var collections [comparisonSides]selectionCollection
	prepareCollections(sides, request, complete, &collections)
	roots := request.Roots
	for i := range sides {
		run := &sides[i].run
		if complete && !run.Absent {
			collections[i].collectBatches()
		}
		sides[i].collectWarnings(roots[0], kinds, len(sides) == comparisonSides)
		if run.Store != nil {
			sides[i].prepareSubjects()
		}

		selected[i] = sides[i].result
	}
	return selected, nil
}

func incompleteSelectionError(runs []Snapshot, runErr error) error {
	for i := range runs {
		run := &runs[i]
		if run.Err != nil && !errors.Is(runErr, run.Err) {
			runErr = errors.Join(runErr, run.Err)
		}
	}
	if runErr == nil {
		runErr = manifest.NewError(manifest.ErrInput, nil, "selection requires completed runs")
	}
	return runErr
}

func (s *selectionSide) prepareNamespaces(namespace string, run *Snapshot) {
	if namespace != "" {
		s.namespaces = map[string]struct{}{namespace: {}}
	} else if run.Store != nil && run.Decision.restricted() {
		s.namespaces = run.Decision.namespaces()
		for _, namespace := range run.ChangeNamespaces {
			if s.namespaces == nil {
				s.namespaces = map[string]struct{}{}
			}
			s.namespaces[namespace] = struct{}{}
		}
	}
}

func prepareSelectionFailures(sides []selectionSide, roots, candidates []manifest.NamedResource) {
	for i := range sides {
		side := &sides[i]
		if side.run.Absent {
			continue
		}
		var other *selectionSide
		if len(sides) == comparisonSides && !sides[1-i].run.Absent {
			other = &sides[1-i]
		}
		side.prepareFailures(roots, other)
		side.prepareStatuses(roots[0].Name)
		if other != nil {
			side.prepareUnavailable(other, candidates)
		}
	}
}

func validateRoots(roots []manifest.NamedResource) (map[manifest.NamedResource]bool, error) {
	if len(roots) == 0 {
		return nil, manifest.NewError(manifest.ErrInput, nil, "selection requires requested kinds")
	}
	kinds := make(map[manifest.NamedResource]bool, len(roots))
	for _, root := range roots {
		if root.Kind == "" || kinds[manifest.NamedResource{Group: root.Group, Kind: root.Kind}] ||
			root.Namespace != roots[0].Namespace ||
			root.Name != roots[0].Name {
			return nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				"selection kinds must be distinct with a common namespace and name",
			)
		}
		kinds[manifest.NamedResource{Group: root.Group, Kind: root.Kind}] = true
	}
	return kinds, nil
}

func (s *selectionSide) unchangedDeclaration(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) manifest.BaseManifest {
	if s.run.Store.GetObject(id) != nil {
		return nil
	}
	restricted := s.run.Decision.restricted()
	if restricted {
		if s.run.Decision.Reconcile(id) || !s.request.Matches(id) ||
			!Root(s.rootEvidence(id, record)).Admitted {
			return nil
		}
	} else if record.Declaration == nil {
		return nil
	}
	if restricted {
		return s.catalogObject(id)
	}
	parent, ok := s.run.Store.Parent(id, s.run.Index)
	status, known := s.run.Store.GetStatus(parent)
	if !ok || !known || status.Outcome() != store.OutcomeUnchanged {
		return nil
	}
	return record.Declaration
}

func (s *selectionSide) statusOf(id manifest.NamedResource) (store.StatusInfo, bool) {
	if info, known := s.run.Failed[id]; known {
		return info, true
	}
	if info, known := s.run.Store.GetStatus(id); known {
		return info, true
	}
	if s.run.Decision.restricted() && !s.run.Decision.Reconcile(id) {
		_, indexed := s.run.Index.Lookup(id)
		if s.run.Store.GetObject(id) != nil || indexed && s.request.Matches(id) {
			return store.StatusInfo{
				Status:  store.StatusReady,
				Reason:  store.ReasonUnchanged,
				Message: "unchanged",
			}, true
		}
	}
	if parent, found := s.run.Store.Parent(id, s.run.Index); found {
		if inherited, known := s.run.Store.GetStatus(parent); known && inherited.Outcome() == store.OutcomeUnchanged {
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
	labels := obj.GetLabels()
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
	if chart, ok := s.run.Store.GetObject(src).(interface{ ChartVersion() string }); ok {
		return chart.ChartVersion()
	}
	return ""
}

func (s *selectionSide) collectWarnings(
	root manifest.NamedResource,
	kinds map[manifest.NamedResource]bool,
	comparison bool,
) {
	if !s.run.HasResult {
		return
	}
	s.result.ClusterWarnings = s.run.ClusterWarnings
	for itemIndex := range s.run.Warnings {
		warning := s.run.Warnings[itemIndex]

		if warning.Category == manifest.WarnIncompleteInputCoverage {
			id := warning.Resource
			if (s.run.Store == nil || !s.run.Decision.restricted()) ||
				!kinds[manifest.NamedResource{Group: id.Group, Kind: id.Kind}] ||
				root.Name != "" && id.Name != root.Name ||
				root.Namespace != "" && id.Namespace != "" && id.Namespace != root.Namespace {
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
	if len(sides) != comparisonSides {
		return nil
	}
	producers := make(map[manifest.NamedResource]struct{})
	for i := range sides {
		side := &sides[i]
		bindings := store.Bindings(side.run.Index)
		for _, id := range side.roots {
			if id.KindInfo().Render {
				key, _ := bindings.Slot(id)
				producers[key] = struct{}{}
			}
		}
		for _, obj := range side.result.Objects {
			id := obj.Named()
			if manifest.KindOf(obj).Render {
				key, _ := bindings.Slot(id)
				producers[key] = struct{}{}
			}
		}
		for _, batch := range side.result.Manifests {
			if batch.Parent != nil {
				key, _ := bindings.Slot(batch.Parent.Named())
				producers[key] = struct{}{}
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

func (s *selectionSide) prepareUnavailable(other *selectionSide, candidates []manifest.NamedResource) {
	local := make(map[manifest.NamedResource]store.StatusInfo, len(candidates))
	counterpart := make(map[manifest.NamedResource]store.StatusInfo)
	var path []manifest.NamedResource
	for _, id := range candidates {
		actual, _ := store.Bindings(s.run.Index).Producer(id)
		var unavailable store.StatusInfo
		if s.run.Decision.restricted() {
			unavailable = s.localStatus(actual)
		}
		if unavailable.IsUnavailable() {
			s.recordUnavailable(id, unavailable)
			continue
		}
		owner, resolved := &s.run, local
		selected := s.run.Decision.Output(actual)
		if s.run.Store.GetObject(actual) == nil || s.run.Decision.restricted() && !selected {
			// Producers outside this projection MUST use the other snapshot's ancestry cache.
			owner, resolved = &other.run, counterpart
		}
		start := actual
		if owner != &s.run {
			start, _ = store.Bindings(other.run.Index).Producer(id)
		}
		unavailable, path = s.walkUnavailable(start, owner, resolved, path)
		if !unavailable.IsUnavailable() && owner == &s.run {
			if prior, found := s.unpublishedAncestor(other, path); found {
				unavailable, path = s.walkUnavailable(prior, &other.run, counterpart, path)
			}
		}
		s.recordUnavailable(id, unavailable)
	}
}

func (s *selectionSide) unpublishedAncestor(
	other *selectionSide,
	path []manifest.NamedResource,
) (manifest.NamedResource, bool) {
	if other.run.Index == nil {
		return manifest.NamedResource{}, false
	}
	for _, id := range path {
		if s.run.Store.EmissionOwner(id) != (manifest.NamedResource{}) || other.run.Store.GetDeclaration(id) != nil {
			continue
		}
		record, found := s.run.Index.Lookup(id)
		if !found || record.File == "" || record.Authored == (manifest.NamedResource{}) {
			continue
		}
		var prior manifest.NamedResource
		for candidate := range other.run.Index.Records {
			previous := other.run.Index.Records[candidate]
			if candidate != id && (prior == (manifest.NamedResource{}) || candidate.Compare(prior) < 0) &&
				other.publishedDeclaration(candidate, &previous, &record) {
				prior = candidate
			}
		}
		if prior != (manifest.NamedResource{}) {
			return prior, true
		}
	}
	return manifest.NamedResource{}, false
}

func (s *selectionSide) publishedDeclaration(
	id manifest.NamedResource,
	previous, current *manifest.DiscoveryRecord,
) bool {
	if previous.Authored != current.Authored || previous.File != current.File ||
		s.run.Store.EmissionOwner(id) == (manifest.NamedResource{}) {
		return false
	}
	return slices.ContainsFunc(previous.Productions, func(p manifest.Production) bool {
		return p.Source != (manifest.NamedResource{}) && slices.ContainsFunc(
			current.Productions,
			func(q manifest.Production) bool { return p.Source == q.Source },
		)
	})
}

func (s *selectionSide) recordUnavailable(id manifest.NamedResource, unavailable store.StatusInfo) {
	if unavailable.IsUnavailable() {
		if s.result.Unavailable == nil {
			s.result.Unavailable = make(map[manifest.NamedResource]store.StatusInfo)
		}
		s.result.Unavailable[id] = unavailable
	}
}

func (s *selectionSide) localStatus(id manifest.NamedResource) store.StatusInfo {
	if info, failed := s.run.Failed[id]; failed {
		return info
	}
	info, _ := s.run.Store.GetStatus(id)
	return info
}

func (s *selectionSide) walkUnavailable(
	id manifest.NamedResource,
	owner *Snapshot,
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
		if key, mapped := store.Bindings(owner.Index).Slot(id); mapped {
			actual, _ = store.Bindings(s.run.Index).Producer(key)
		}
		info := s.localStatus(actual)
		if info.IsUnavailable() {
			unavailable = info
			break
		}
		ancestry := owner
		if actual != id {
			ancestry = &s.run
		}
		parent, ok := ancestry.Store.Parent(actual, ancestry.Index)
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
	if !s.run.HasResult {
		return
	}
	for id := range s.run.Failed {
		add(s.diagnostic(id))
	}
	for id, deps := range s.run.Blocked {
		add(s.diagnostic(id))
		for _, dep := range deps {
			add(s.diagnostic(dep))
		}
	}
	for itemIndex := range s.run.Warnings {
		warning := s.run.Warnings[itemIndex]

		if warning.Subject.ID != (manifest.NamedResource{}) {
			add(warning.Subject)
		} else if warning.Resource != (manifest.NamedResource{}) {
			add(s.diagnostic(warning.Resource))
		}
		for _, id := range warning.Related {
			add(s.diagnostic(id))
		}
	}
}

func (s *selectionSide) status(id manifest.NamedResource) store.StatusInfo {
	if info, ok := s.statusOf(id); ok {
		return info
	}
	if s.run.Decision.restricted() && !s.run.Decision.Reconcile(id) {
		return store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"}
	}
	return store.StatusInfo{Status: store.StatusFailed, Message: "no status reported"}
}
