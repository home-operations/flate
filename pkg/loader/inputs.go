package loader

import (
	"cmp"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// ParsedInputs retains immutable parsed content from exact admitted source views.
type ParsedInputs struct {
	sources map[string]parsedSource
	compact bool
}

type parsedSource struct {
	parsedDocuments

	cache    *inputCache
	borrowed bool

	input    any
	binding  *store.ArtifactInput
	walks    factMap[physicalContext, physicalWalk]
	loads    factMap[loadContext, loadFacts]
	hydrated factMap[occurrenceID, store.Child]
}

// Document history MUST NOT retain prior walk or hydration snapshots.
type parsedDocuments struct {
	documents map[string]*parsedDocument
	previous  *parsedDocuments
}

type parsedDocument struct {
	deferred   *cachedInput
	projection *projectionInputs
	coverage   manifest.DocumentRole
	possible   bool
	body       *types.Kustomization
	file       string
	objects    []parsedObject
	state      pathState
	searched   bool
	plugins    *[]pluginInputs
	decodeErr  error
}

type physicalWalk struct {
	cached   *cachedWalk
	visits   []graphVisit
	inputs   []walkInput
	imported bool
}

type walkInput struct {
	path   string
	listed bool
}

type physicalContext struct {
	graphContext

	sourceRoot, prefix, localBase string
}

type (
	loadContext   struct{ path, ignore string }
	loadDirectory struct{ adHoc, traversed bool }
	loadFacts     struct {
		cached             *cachedLoad
		cachedRoot         string
		err                error
		coverage           manifest.DocumentRole
		complete, observed bool
		imported           bool
		files              []fileAdmission
		directories        map[string]loadDirectory
	}
)

type admissionKind uint8

const (
	admissionResource admissionKind = iota
	admissionData
	admissionPackage
	admissionBootstrap
	admissionWarning
)

type fileAdmission struct {
	file       string
	kind       admissionKind
	coordinate occurrenceID
	warning    manifest.Warning
}

func (l *Loader) sourceFacts(root string) parsedSource {
	if l.facts == nil {
		l.facts = &ParsedInputs{sources: map[string]parsedSource{}}
	}
	root = new(l.documents).mountedRoot(root)
	artifact := l.documents.artifacts[root]
	var identity any
	if artifact != nil {
		identity = parsedInputIdentity(artifact)
	}
	binding := artifact.Input(store.ArtifactContent | store.ArtifactRevision)
	if source, ok := l.facts.sources[root]; ok && source.input == identity && *source.binding == binding {
		return source
	}
	sourceBinding := binding
	source := parsedSource{
		input:    identity,
		binding:  &sourceBinding,
		walks:    factMap[physicalContext, physicalWalk]{values: map[physicalContext]physicalWalk{}},
		loads:    factMap[loadContext, loadFacts]{values: map[loadContext]loadFacts{}},
		hydrated: factMap[occurrenceID, store.Child]{values: map[occurrenceID]store.Child{}},
	}
	if prepared := l.documents.parsed; prepared != nil && prepared.compact == l.documents.compact {
		if prior, ok := prepared.sources[root]; ok && prior.input == identity && *prior.binding == binding {
			source.walks.previous, source.walks.imported = new(prior.walks), true
			source.borrowed = true
			source.loads.previous, source.loads.imported = new(prior.loads), true
			source.hydrated.previous = new(prior.hydrated)
		}
	}
	l.facts.sources[root] = source
	return source
}

// ParsedInputs snapshots parsed bodies without retaining entry admission state.
func (l *Loader) ParsedInputs() *ParsedInputs {
	inputs := &ParsedInputs{
		sources: map[string]parsedSource{},
		compact: l.documents.compact,
	}
	for root := range l.documents.artifacts {
		source := l.snapshotSource(root)
		inputs.sources[root] = source
	}
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for file, doc := range l.documents.cache {
		if !doc.hasParsedFacts() && !doc.hasObservations() {
			continue
		}
		source, ok := inputs.sources[new(l.documents).mountedRoot(file)]
		if !ok {
			continue
		}
		doc.snapshot()
		l.documents.cache[file] = doc
		if source.previous.lookup(file) != doc.parsed {
			source.documents[file] = doc.parsed
		}
	}
	l.snapshot = inputs
	return inputs
}

func (d *document) hasParsedFacts() bool {
	return d.deferred != nil || d.projection != nil || d.body != nil || d.objects != nil
}

func (d *document) hasObservations() bool {
	return d.searched || d.pathState != pathUnknown || d.decodeErr != nil
}

func (s *parsedDocuments) lookup(file string) *parsedDocument {
	for source := s; source != nil; source = source.previous {
		if doc := source.documents[file]; doc != nil {
			return doc
		}
	}
	return nil
}

// Matches reports whether input owns a source view retained by these parsed facts.
func (p *ParsedInputs) Matches(input *store.SourceArtifact) bool {
	source, ok := p.sources[input.LocalPath]
	return ok && source.input == parsedInputIdentity(input) &&
		(!p.compact || *source.binding == input.Input(store.ArtifactContent|store.ArtifactRevision))
}

func parsedInputIdentity(input *store.SourceArtifact) any {
	if view, ok := input.FS.(*gittree.FS); ok {
		return view
	}
	return input
}

//nolint:gocritic // Immutable reader values retain their original snapshot semantics.
func (r documentReader) restore(file string) {
	defer r.restoreCachedFile(file)
	if r.parsed == nil || r.parsed.compact != r.compact {
		return
	}
	cached := r.cache[file]
	if cached.objects != nil && covers(cached.coverage, r.intent) {
		return
	}
	root := new(r).mountedRoot(file)
	source, ok := r.parsed.sources[root]
	artifact := r.artifacts[root]
	if !ok || artifact == nil {
		return
	}
	if parsedInputIdentity(artifact) != source.input ||
		r.compact && artifact.Input(store.ArtifactContent|store.ArtifactRevision) != *source.binding {
		return
	}
	if doc := source.lookup(file); doc != nil {
		cached.body, cached.file, cached.sourceRoot = doc.body, doc.file, root
		if cached.objects == nil || covers(doc.coverage, cached.coverage) {
			cached.objects = cloneInputObjects(doc.objects)
			cached.coverage, cached.possible = doc.coverage, doc.possible
		}
		cached.pathState, cached.searched, cached.plugins, cached.decodeErr = doc.state, doc.searched, doc.plugins, doc.decodeErr
		cached.parsed, cached.projection, cached.deferred = doc, doc.projection, doc.deferred
		r.cache[file] = cached
	}
}

func cloneInputObjects(objects []parsedObject) []parsedObject {
	cloned := slices.Clone(objects)
	for i := range cloned {
		cloned[i].Doc = manifest.DeepCopyMap(cloned[i].Doc)
		cloned[i].outcome.Document = cloned[i].Doc
	}
	return cloned
}

func (l *Loader) inputVisibility() []manifest.InputVisibility {
	if l.facts == nil {
		return nil
	}
	var observations []manifest.InputVisibility
	//nolint:gocritic // The source map supplies immutable value snapshots.
	for root, facts := range l.facts.sources {
		artifact := l.documents.artifacts[root]
		var identity any
		if artifact != nil {
			identity = parsedInputIdentity(artifact)
		}
		if facts.input != identity || *facts.binding != artifact.Input(store.ArtifactContent|store.ArtifactRevision) {
			continue
		}
		sources := []manifest.NamedResource{}
		if root == l.documents.bootstrap {
			sources = append(sources, manifest.BootstrapSourceID)
		}
		{
			scopeValues := l.scopes.sources
			for scopeIndex := range scopeValues {
				scope := scopeValues[scopeIndex]
				if !new(scope).matchesVisibility(root, sources, identity, new(facts)) {
					continue
				}
				sources = append(sources, scope.id)
			}
		}
		for domain, load := range facts.loadInputs() {
			if !load.observed {
				continue
			}
			observation := l.loadInputVisibility(root, domain, new(load))
			observations = appendSourceVisibilities(observations, sources, new(observation))
		}
	}
	slices.SortFunc(observations, func(a, b manifest.InputVisibility) int {
		return cmp.Or(a.Source.Compare(b.Source), strings.Compare(a.Root, b.Root), strings.Compare(a.Ignore, b.Ignore))
	})
	return observations
}

func (l *Loader) loadInputVisibility(root string, domain loadContext, loadValue *loadFacts) manifest.InputVisibility {
	load := *loadValue
	relative, _ := filepath.Rel(root, domain.path)
	observation := manifest.InputVisibility{Root: filepath.ToSlash(relative), Complete: load.complete}
	if domain.ignore != "" {
		relative, _ := filepath.Rel(root, domain.ignore)
		observation.Ignore = filepath.ToSlash(relative)
	}
	{
		fileValues := load.files
		for fileIndex := range fileValues {
			file := fileValues[fileIndex]
			if file.kind == admissionWarning || l.terminalInput(file.file) ||
				l.documents.cache[file.file].eligible == selection.RootInactive {
				continue
			}
			l.appendInputVisibility(&observation, root, new(file))
		}
	}
	for id, files := range observation.Inputs {
		slices.Sort(files)
		observation.Inputs[id] = slices.Compact(files)
	}
	return observation
}

func (l *Loader) appendInputVisibility(observation *manifest.InputVisibility, root string, fileValue *fileAdmission) {
	file := *fileValue
	{
		childValues := l.documents.cache[file.file].objects
		for childIndex := range childValues {
			child := childValues[childIndex]
			if child.outcome.Err != nil {
				continue
			}
			var id manifest.NamedResource
			if child.fact != nil {
				if child.fact.facts.uncertain ||
					file.kind == admissionBootstrap && child.fact.Declaration != file.coordinate {
					continue
				}
				id = child.fact.Authored
			} else if child.Obj != nil {
				id = child.Obj.Named()
			}
			if id.Name == "" || !id.KindInfo().Tracked ||
				file.kind == admissionData && !id.KindInfo().Data {
				continue
			}
			record := manifest.DiscoveryRecord{
				Authored: id,
				Path:     file.file,
				File:     l.fileCoordinate(file.file),
			}
			relative, _ := filepath.Rel(root, file.file)
			for _, namespace := range l.recordNamespaces(new(record), "") {
				effective := id
				effective.Namespace = namespace
				if observation.Inputs == nil {
					observation.Inputs = map[manifest.NamedResource][]string{}
				}
				observation.Inputs[effective] = append(
					observation.Inputs[effective],
					filepath.ToSlash(relative),
				)
			}
		}
	}
}

func (s *sourceScope) matchesVisibility(
	root string,
	sources []manifest.NamedResource,
	identity any,
	factsValue *parsedSource,
) bool {
	scope := *s
	facts := *factsValue
	if scope.root != root || scope.artifact == nil || slices.Contains(sources, scope.id) {
		return false
	}
	if parsedInputIdentity(scope.artifact) != identity ||
		scope.artifact.Input(store.ArtifactContent|store.ArtifactRevision) != *facts.binding {
		return false
	}
	return true
}

func appendSourceVisibilities(
	observations []manifest.InputVisibility,
	sources []manifest.NamedResource,
	observationValue *manifest.InputVisibility,
) []manifest.InputVisibility {
	observation := *observationValue
	for _, source := range sources {
		observation.Source = source
		observations = append(observations, observation)
	}
	return observations
}

func (d *document) snapshot() {
	sameParse := d.parsed != nil && d.parsed.coverage == d.coverage &&
		d.parsed.possible == d.possible && d.parsed.deferred == d.deferred
	sameBuilder := sameParse && d.parsed.body == d.body && d.parsed.file == d.file
	if sameBuilder && d.parsed.matchesObservation(d) {
		return
	}
	var objects []parsedObject
	if sameParse {
		objects = d.parsed.objects
	} else {
		objects = cloneInputObjects(d.objects)
	}
	d.parsed = &parsedDocument{
		body: d.body, file: d.file, objects: objects, projection: d.projection, deferred: d.deferred,
		coverage: d.coverage, possible: d.possible,
		state: d.pathState, searched: d.searched, plugins: d.plugins, decodeErr: d.decodeErr,
	}
}

func (p *parsedDocument) matchesObservation(doc *document) bool {
	return p.state == doc.pathState && p.searched == doc.searched &&
		p.plugins == doc.plugins && p.projection == doc.projection
}

// Frozen map layers MUST remain immutable while independent roots borrow them.
type factMap[K comparable, V any] struct {
	values   map[K]V
	previous *factMap[K, V]
	imported bool
}

type factValue[V any] struct {
	value    V
	imported bool
}

//nolint:gocritic,ireturn // Generic values retain separate key-presence and imported-ownership flags.
func (m *factMap[K, V]) get(key K) (V, bool, bool) {
	imported := false
	for layer := m; layer != nil; layer = layer.previous {
		if value, ok := layer.values[key]; ok {
			return value, true, imported
		}
		imported = imported || layer.imported
	}
	var zero V
	return zero, false, false
}

func (m *factMap[K, V]) freeze() factMap[K, V] {
	if len(m.values) == 0 {
		if !m.imported && m.previous != nil {
			return *m.previous
		}
		frozen := *m
		frozen.values = nil
		return frozen
	}
	frozen := *m
	*m = factMap[K, V]{values: map[K]V{}, previous: &frozen}
	return frozen
}

func (m *factMap[K, V]) entries() iter.Seq2[K, factValue[V]] {
	if m.previous == nil {
		return func(yield func(K, factValue[V]) bool) {
			for key, value := range m.values {
				if !yield(key, factValue[V]{value: value}) {
					return
				}
			}
		}
	}
	return m.uniqueEntries
}

func (m *factMap[K, V]) uniqueEntries(yield func(K, factValue[V]) bool) {
	seen := map[K]bool{}
	imported := false
	for layer := m; layer != nil; layer = layer.previous {
		for key, value := range layer.values {
			if seen[key] {
				continue
			}
			seen[key] = true
			if !yield(key, factValue[V]{value: value, imported: imported}) {
				return
			}
		}
		imported = imported || layer.imported
	}
}

func (s *parsedSource) walk(key *physicalContext) (physicalWalk, bool) {
	walk, known, imported := s.walks.get(*key)
	walk.imported = walk.imported || imported
	return walk, known
}

func (s *parsedSource) load(key loadContext) (loadFacts, bool) {
	load, known, imported := s.loads.get(key)
	if load.cached != nil {
		load = load.cached.restore(load.cachedRoot)
		s.loads.values[key] = load
	}
	if imported {
		load.observed, load.err, load.imported = false, nil, true
	}
	return load, known
}

func (s *parsedSource) loadInputs() iter.Seq2[loadContext, loadFacts] {
	return func(yield func(loadContext, loadFacts) bool) {
		for key, entry := range s.loads.entries() {
			load := entry.value
			if entry.imported {
				load.observed, load.err, load.imported = false, nil, true
			}
			if !yield(key, load) {
				return
			}
		}
	}
}

func (l *Loader) snapshotSource(root string) parsedSource {
	source := l.sourceFacts(root)
	if state := l.inputCaches[root]; state != nil {
		source.cache = new(*state)
	}
	if l.snapshot != nil && l.snapshot.compact == l.documents.compact {
		if prior, ok := l.snapshot.sources[root]; ok && prior.input == source.input &&
			*prior.binding == *source.binding {
			source.previous = new(prior.parsedDocuments)
			for source.previous != nil && len(source.previous.documents) == 0 {
				source.previous = source.previous.previous
			}
		}
	}
	next := l.facts.sources[root]
	source.walks, source.loads, source.hydrated = next.walks.freeze(), next.loads.freeze(), next.hydrated.freeze()
	l.facts.sources[root] = next
	if source.borrowed && !l.documents.compact && root == l.documents.bootstrap {
		source.retainNativeFacts()
	}
	source.documents = map[string]*parsedDocument{}
	return source
}

func (s *parsedSource) retainNativeFacts() {
	walks := factMap[physicalContext, physicalWalk]{values: map[physicalContext]physicalWalk{}}
	for key, entry := range s.walks.entries() {
		if !entry.imported && !entry.value.imported {
			walks.values[key] = entry.value
		}
	}
	loads := factMap[loadContext, loadFacts]{values: map[loadContext]loadFacts{}}
	for key, value := range s.loadInputs() {
		if !value.imported {
			loads.values[key] = value
		}
	}
	s.walks, s.loads = walks, loads
}
