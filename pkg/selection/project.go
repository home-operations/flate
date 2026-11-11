package selection

import (
	"errors"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
	sides := make([]selectionSide, len(runs))
	for i := range runs {
		run := &runs[i]
		sides[i].run, sides[i].request = *run, request
		if run.Facts != nil && run.Store != nil {
			sides[i].reads.Init(run.Store, manifest.BootstrapSourceID, nil)
		}
		sides[i].run.CollectEmissions()
		sides[i].prepareNamespaces(roots[0].Namespace, run)
	}
	defer func() {
		for i := range sides {
			if sides[i].run.Facts != nil && sides[i].run.Store != nil {
				sides[i].reads.Retire()
			}
		}
	}()
	selected := collectProjectionSides(sides, request, kinds, complete)
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
) []*Result {
	selected := make([]*Result, len(sides))
	roots, labels := request.Roots, request.Labels
	for i := range sides {
		run := &sides[i].run
		var other *selectionSide
		if len(sides) == comparisonSides {
			other = &sides[1-i]
		}
		if complete && !run.Absent {
			sides[i].collect(roots, labels, other)
		} else {
			sides[i].result = &Result{}
		}
		sides[i].collectWarnings(roots[0], kinds, len(sides) == comparisonSides)
		if run.Store != nil {
			sides[i].prepareSubjects()
		}

		selected[i] = sides[i].result
	}
	return selected
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

func (s *selectionSide) collect(roots []manifest.NamedResource, labels map[string]string, other *selectionSide) {
	s.result = &Result{Counts: make(map[string]int, len(roots))}
	if binding := s.run.Entry; binding.Producer != (manifest.NamedResource{}) {
		s.result.ProducerKeys = map[manifest.NamedResource]manifest.NamedResource{binding.Producer: binding.Key}
	}
	c := selectionCollection{side: s, intent: Request{Roots: roots}, labels: labels, other: other}
	c.collectObjects()
	c.collectBatches()
}

type selectionCollection struct {
	side     *selectionSide
	intent   Request
	labels   map[string]string
	other    *selectionSide
	emitters []manifest.BaseManifest
}

func (c *selectionCollection) collectObjects() {
	for _, root := range c.intent.Roots {
		for _, obj := range c.rootObjects(root) {
			c.collectObject(root, obj)
		}
	}
}

func (c *selectionCollection) rootObjects(root manifest.NamedResource) []manifest.BaseManifest {
	s := c.side
	objects := s.run.Store.ListObjects(root.Group, root.Kind)
	stored := len(objects)
	for id, record := range s.run.Index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		if id.Group == root.Group && id.Kind == root.Kind {
			if obj := s.unchangedDeclaration(id, &record); obj != nil {
				objects = append(objects, obj)
			}
		}
	}
	if len(objects) != stored {
		slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	}
	return objects
}

func (c *selectionCollection) collectObject(root manifest.NamedResource, obj manifest.BaseManifest) {
	s := c.side
	id := obj.Named()
	if !s.run.Store.Policy(id).Report && !id.ReportHidden() {
		return
	}
	if c.other != nil {
		s.prepareExternal(root, obj)
	}
	if id.ReportHidden() && manifest.KindOf(obj).Render {
		if root.Name == "" {
			s.roots = append(s.roots, id)
			c.emitters = append(c.emitters, obj)
		}
		return
	}
	if !c.intent.Matches(id) || !s.includes(id.Namespace) {
		if root.Name == "" && s.run.Decision.restricted() && s.run.Decision.Reconcile(id) {
			c.emitters = append(c.emitters, obj)
		}
		return
	}
	if !s.selectedObject(obj) {
		return
	}
	s.result.Counts[root.Kind]++
	s.roots = append(s.roots, id)
	if !matchesLabels(obj, c.labels) {
		return
	}
	s.result.Objects = append(s.result.Objects, obj)
	if version := s.chartVersion(obj); version != "" {
		if s.result.ChartVersions == nil {
			s.result.ChartVersions = make(map[manifest.NamedResource]string)
		}
		s.result.ChartVersions[id] = version
	}
}

func (s *selectionSide) selectedObject(obj manifest.BaseManifest) bool {
	id := obj.Named()
	if !s.run.Decision.restricted() || manifest.KindOf(obj).Discovery {
		return true
	}
	selected := s.run.Decision.Output(id)
	if selected || s.run.Store.GetObject(id) == nil {
		return true
	}
	parent, owned := s.run.parentOf(id)
	parentSelected := s.run.Decision.Output(parent)
	return owned && parentSelected
}

func (c *selectionCollection) collectBatches() {
	s := c.side
	scoped := s.namespaces != nil
	if c.other != nil && !c.other.run.Absent {
		scoped = scoped && c.other.namespaces != nil
	}
	positions := make(map[manifest.NamedResource]int, len(s.run.Manifests))
	for i, batch := range s.run.Manifests {
		if batch.Parent != nil {
			positions[batch.Parent.Named()] = i
		}
	}
	if len(s.run.Manifests) != 0 {
		s.result.Manifests = make([]manifest.DocumentBatch, 0, len(s.result.Objects)+1)
	}
	for _, objects := range [2][]manifest.BaseManifest{s.result.Objects, c.emitters} {
		for _, obj := range objects {
			i, ok := positions[obj.Named()]
			if !ok {
				continue
			}
			batch := batchSelection{collection: c, object: obj, batch: s.run.Manifests[i]}
			batch.selectDocuments(scoped)
			if len(batch.batch.Documents) != 0 {
				s.result.Manifests = append(s.result.Manifests, batch.batch)
			}
		}
	}
}

type batchSelection struct {
	collection       *selectionCollection
	object           manifest.BaseManifest
	batch            manifest.DocumentBatch
	counterpart      []map[string]any
	counterpartKnown bool
}

func (b *batchSelection) selectedOutput(id manifest.NamedResource) bool {
	s, other := b.collection.side, b.collection.other
	selected := s.run.Decision.Output(id)
	if selected || other == nil || other.run.Absent {
		return selected
	}
	if selected := other.run.Decision.Output(id); !selected {
		return false
	}
	if !b.counterpartKnown {
		for _, batch := range other.run.Manifests {
			if batch.Parent != nil && batch.Parent.Named() == b.object.Named() {
				b.counterpart = batch.Documents
				break
			}
		}
		b.counterpartKnown = true
	}
	return slices.ContainsFunc(b.counterpart, func(doc map[string]any) bool { return manifest.DocIdentity(doc) == id })
}

func (b *batchSelection) selectDocuments(scoped bool) {
	var policy manifest.EmissionPolicy
	if manifest.KindOf(b.object).Parent {
		policy = b.object.EmissionPolicy()
	}
	if policy.DocumentScope {
		b.selectScopedDocuments(scoped)
	}
	s := b.collection.side
	if s.run.Decision.restricted() && !policy.DocumentScope {
		if selected := s.run.Decision.Output(b.object.Named()); !selected {
			documents := []map[string]any{}
			for _, doc := range b.batch.Documents {
				if b.selectedOutput(manifest.DocIdentity(doc)) {
					documents = append(documents, doc)
				}
			}
			b.batch.Documents = documents
		}
	}
}

func (b *batchSelection) selectScopedDocuments(scoped bool) {
	s, other := b.collection.side, b.collection.other
	compared := other == nil || other.run.Absent ||
		other.run.Decision.Reconcile(other.actualProducer(s.comparisonKey(b.object.Named())))
	documents := b.batch.Documents
	b.batch.Documents = nil
	for _, doc := range documents {
		if s.run.Decision.restricted() && !scoped && !b.declarationSelected(doc, compared) {
			continue
		}
		selected := selectOutput(doc, b.collection.intent.Roots, s.namespaces, b.collection.labels)
		if !selected && other != nil && !other.run.Absent {
			selected = selectOutput(doc, b.collection.intent.Roots, other.namespaces, b.collection.labels)
		}
		if selected {
			b.batch.Documents = append(b.batch.Documents, doc)
		}
	}
}

func (b *batchSelection) declarationSelected(doc map[string]any, compared bool) bool {
	id := manifest.DocIdentity(doc)
	declaration := compared && slices.ContainsFunc(b.collection.intent.Roots, func(root manifest.NamedResource) bool {
		return root.Group == id.Group && root.Kind == id.Kind
	})
	return declaration || b.selectedOutput(id)
}

//nolint:ireturn // Declarations retain their heterogeneous manifest types.
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
	parent, ok := s.run.parentOf(id)
	status, known := s.run.Store.GetStatus(parent)
	if !ok || !known || status.Outcome() != store.OutcomeUnchanged {
		return nil
	}
	return record.Declaration
}

func (s *selectionSide) statusOf(id manifest.NamedResource) (store.StatusInfo, bool) {
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
	if parent, found := s.run.parentOf(id); found {
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
	if !(Request{Roots: []manifest.NamedResource{root}}).Matches(id) {
		return
	}
	if s.run.Facts == nil || !s.run.Facts.SourceExternal(src, &s.reads) {
		return
	}
	info := s.status(id)
	if info.Outcome() != store.OutcomeUnchanged && info.Outcome() != store.OutcomeSkipped {
		return
	}
	if slices.ContainsFunc(s.run.Warnings, func(w manifest.Warning) bool {
		return w.Resource == src && w.Category == manifest.WarnSourceCredentialsUnavailable
	}) {
		return
	}
	s.result.External = append(
		s.result.External,
		manifest.Warning{
			Resource: id,
			Subject:  s.result.Diagnostic(id),
			Related:  []manifest.NamedResource{src},
			Category: manifest.WarnExternalSource,
			Message:  "external source " + s.diagnostic(src).String() + " is outside the local tree; not rendered",
		},
	)
}

func (s *selectionSide) prepareUnavailable(other *selectionSide, candidates []manifest.NamedResource) {
	local := make(map[manifest.NamedResource]store.StatusInfo, len(candidates))
	counterpart := make(map[manifest.NamedResource]store.StatusInfo)
	var path []manifest.NamedResource
	for _, id := range candidates {
		actual := s.actualProducer(id)
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
			start = other.actualProducer(id)
		}
		unavailable, path = s.walkUnavailable(start, owner, resolved, path)
		if !unavailable.IsUnavailable() && owner == &s.run {
			if prior, found := s.unpublishedCounterpart(other, path); found {
				unavailable, path = s.walkUnavailable(prior, &other.run, counterpart, path)
			}
		}
		s.recordUnavailable(id, unavailable)
	}
}

func (s *selectionSide) unpublishedCounterpart(
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
		if prior := other.declarationCounterpart(id, &record); prior != (manifest.NamedResource{}) {
			return prior, true
		}
	}
	return manifest.NamedResource{}, false
}

func (s *selectionSide) declarationCounterpart(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) manifest.NamedResource {
	var prior manifest.NamedResource
	for candidate := range s.run.Index.Records {
		previous := s.run.Index.Records[candidate]
		if candidate == id || previous.Authored != record.Authored || previous.File != record.File ||
			s.run.Store.EmissionOwner(candidate) == (manifest.NamedResource{}) {
			continue
		}
		sameSource := slices.ContainsFunc(previous.Productions, func(p manifest.Production) bool {
			return p.Source != (manifest.NamedResource{}) && slices.ContainsFunc(
				record.Productions,
				func(q manifest.Production) bool { return p.Source == q.Source },
			)
		})
		if sameSource && (prior == (manifest.NamedResource{}) || candidate.Compare(prior) < 0) {
			prior = candidate
		}
	}
	return prior
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
		if binding := owner.Entry; binding.Producer == id {
			actual = s.actualProducer(binding.Key)
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
	if binding := s.run.Entry; binding.Producer != (manifest.NamedResource{}) && binding.Key == key {
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
