package loader

import (
	"cmp"
	"maps"
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
	input     any
	documents map[string]parsedDocument
	binding   *store.ArtifactInput
	walks     map[physicalContext]physicalWalk
	loads     map[loadContext]loadFacts
	hydrated  map[occurrenceID]store.Child
}

type parsedDocument struct {
	coverage  manifest.DocumentRole
	possible  bool
	body      *types.Kustomization
	file      string
	objects   []parsedObject
	state     pathState
	searched  bool
	plugins   *[]pluginInputs
	decodeErr error
}

type physicalWalk struct {
	visits   []graphVisit
	imported bool
}

type physicalContext struct {
	graphContext

	sourceRoot, prefix, localBase string
}

type (
	loadContext   struct{ path, ignore string }
	loadDirectory struct{ adHoc, traversed bool }
	loadFacts     struct {
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
		walks:    map[physicalContext]physicalWalk{},
		loads:    map[loadContext]loadFacts{},
		hydrated: map[occurrenceID]store.Child{},
	}
	if prepared := l.documents.parsed; prepared != nil && prepared.compact == l.documents.compact {
		if prior, ok := prepared.sources[root]; ok && prior.input == identity && *prior.binding == binding {
			source.walks, source.loads = maps.Clone(prior.walks), maps.Clone(prior.loads)
			source.hydrated = maps.Clone(prior.hydrated)
			for key, load := range source.loads {
				load.observed, load.err, load.imported = false, nil, true
				source.loads[key] = load
			}
			for key, walk := range source.walks {
				walk.imported = true
				source.walks[key] = walk
			}
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
		source := l.sourceFacts(root)
		source.walks, source.loads = maps.Clone(source.walks), maps.Clone(source.loads)
		if !l.documents.compact && root == l.documents.bootstrap {
			maps.DeleteFunc(source.walks, func(_ physicalContext, walk physicalWalk) bool { return walk.imported })
			maps.DeleteFunc(source.loads, func(_ loadContext, load loadFacts) bool { return load.imported })
		}
		source.hydrated = maps.Clone(source.hydrated)
		source.documents = map[string]parsedDocument{}
		inputs.sources[root] = source
	}
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for file, doc := range l.documents.cache {
		if doc.body == nil && doc.objects == nil &&
			(!doc.searched && doc.pathState == pathUnknown && doc.decodeErr == nil) {
			continue
		}
		if source, ok := inputs.sources[new(l.documents).mountedRoot(file)]; ok {
			source.documents[file] = parsedDocument{
				body: doc.body, file: doc.file, objects: cloneInputObjects(doc.objects),
				coverage: doc.coverage, possible: doc.possible,
				state: doc.pathState, searched: doc.searched, plugins: doc.plugins, decodeErr: doc.decodeErr,
			}
		}
	}
	return inputs
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
	if doc, ok := source.documents[file]; ok {
		cached.body, cached.file = doc.body, doc.file
		if cached.objects == nil || covers(doc.coverage, cached.coverage) {
			cached.objects = cloneInputObjects(doc.objects)
			cached.coverage, cached.possible = doc.coverage, doc.possible
		}
		cached.pathState, cached.searched, cached.plugins, cached.decodeErr = doc.state, doc.searched, doc.plugins, doc.decodeErr
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
		for domain, load := range facts.loads {
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
