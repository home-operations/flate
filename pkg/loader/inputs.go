package loader

import (
	"maps"
	"slices"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// ParsedInputs retains immutable parsed content from exact admitted source views.
type ParsedInputs struct {
	sources     map[string]parsedSource
	wipeSecrets bool
	compact     bool
}

type parsedSource struct {
	input     any
	documents map[string]parsedDocument
	binding   *store.ArtifactInput
	walks     map[physicalContext][]graphVisit
	loads     map[loadContext]loadFacts
}

type parsedDocument struct {
	body      *types.Kustomization
	file      string
	objects   []parsedObject
	state     pathState
	searched  bool
	plugins   *[]pluginInputs
	decodeErr error
}

type physicalContext struct {
	graphContext
	sourceRoot, prefix, localBase string
}

type loadContext struct{ path, ignore string }
type loadDirectory struct{ adHoc, traversed bool }
type loadFacts struct {
	files       []fileAdmission
	directories map[string]loadDirectory
}

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
	coordinate OccurrenceID
	warning    manifest.Warning
}

func (l *Loader) sourceFacts(root string) parsedSource {
	if l.facts == nil {
		l.facts = &ParsedInputs{sources: map[string]parsedSource{}}
	}
	root = l.documents.mountedRoot(root)
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
	source := parsedSource{input: identity, binding: &sourceBinding, walks: map[physicalContext][]graphVisit{}, loads: map[loadContext]loadFacts{}}
	if prepared := l.documents.parsed; prepared != nil && prepared.compact {
		if prior, ok := prepared.sources[root]; ok && prior.input == identity && *prior.binding == binding {
			source.walks, source.loads = maps.Clone(prior.walks), maps.Clone(prior.loads)
		}
	}
	l.facts.sources[root] = source
	return source
}

// ParsedInputs snapshots parsed bodies without retaining entry admission state.
func (l *Loader) ParsedInputs() *ParsedInputs {
	inputs := &ParsedInputs{sources: map[string]parsedSource{}, wipeSecrets: l.Options.WipeSecrets, compact: l.documents.compact}
	for root, artifact := range l.documents.artifacts {
		source := parsedSource{input: parsedInputIdentity(artifact)}
		if inputs.compact {
			source = l.sourceFacts(root)
			source.walks, source.loads = maps.Clone(source.walks), maps.Clone(source.loads)
		}
		source.documents = map[string]parsedDocument{}
		inputs.sources[root] = source
	}
	for file, doc := range l.documents.cache {
		if doc.body == nil && doc.objects == nil && (!inputs.compact || !doc.searched && doc.pathState == pathUnknown && doc.decodeErr == nil) {
			continue
		}
		if source, ok := inputs.sources[l.documents.mountedRoot(file)]; ok {
			source.documents[file] = parsedDocument{
				body: doc.body, file: doc.file, objects: cloneInputObjects(doc.objects),
				state: doc.pathState, searched: doc.searched, plugins: doc.plugins, decodeErr: doc.decodeErr,
			}
		}
	}
	return inputs
}

// Matches reports whether input owns a source view retained by these parsed facts.
func (p *ParsedInputs) Matches(input *store.SourceArtifact) bool {
	source, ok := p.sources[input.LocalPath]
	return ok && source.input == parsedInputIdentity(input) && (!p.compact || *source.binding == input.Input(store.ArtifactContent|store.ArtifactRevision))
}

func parsedInputIdentity(input *store.SourceArtifact) any {
	if view, ok := input.FS.(*gittree.FS); ok {
		return view
	}
	return input
}

func (r documentReader) restore(file string) {
	if r.parsed == nil || r.parsed.compact != r.compact {
		return
	}
	cached := r.cache[file]
	if cached.body != nil || cached.objects != nil {
		return
	}
	root := r.mountedRoot(file)
	source, ok := r.parsed.sources[root]
	artifact := r.artifacts[root]
	if !ok || artifact == nil {
		return
	}
	if parsedInputIdentity(artifact) != source.input || r.compact && artifact.Input(store.ArtifactContent|store.ArtifactRevision) != *source.binding {
		return
	}
	if doc, ok := source.documents[file]; ok {
		cached.body, cached.file = doc.body, doc.file
		cached.objects = cloneInputObjects(doc.objects)
		if r.compact {
			cached.pathState, cached.searched, cached.plugins, cached.decodeErr = doc.state, doc.searched, doc.plugins, doc.decodeErr
		}
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
