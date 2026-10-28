package loader

import (
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
}

type parsedSource struct {
	input     any
	documents map[string]parsedDocument
}

type parsedDocument struct {
	body    *types.Kustomization
	file    string
	objects []parsedObject
}

// ParsedInputs snapshots parsed bodies without retaining entry admission state.
func (l *Loader) ParsedInputs() *ParsedInputs {
	inputs := &ParsedInputs{sources: map[string]parsedSource{}, wipeSecrets: l.Options.WipeSecrets}
	for root, artifact := range l.documents.artifacts {
		inputs.sources[root] = parsedSource{input: parsedInputIdentity(artifact), documents: map[string]parsedDocument{}}
	}
	for file, doc := range l.documents.cache {
		if doc.body == nil && doc.objects == nil {
			continue
		}
		if source, ok := inputs.sources[l.documents.mountedRoot(file)]; ok {
			source.documents[file] = parsedDocument{
				body: doc.body, file: doc.file, objects: cloneInputObjects(doc.objects),
			}
		}
	}
	return inputs
}

// Matches reports whether input owns a source view retained by these parsed facts.
func (p *ParsedInputs) Matches(input *store.SourceArtifact) bool {
	source, ok := p.sources[input.LocalPath]
	return ok && source.input == parsedInputIdentity(input)
}

func parsedInputIdentity(input *store.SourceArtifact) any {
	if view, ok := input.FS.(*gittree.FS); ok {
		return view
	}
	return input
}

func (r documentReader) restore(file string) {
	if r.parsed == nil {
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
	if parsedInputIdentity(artifact) != source.input {
		return
	}
	if doc, ok := source.documents[file]; ok {
		cached.body, cached.file = doc.body, doc.file
		cached.objects = cloneInputObjects(doc.objects)
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
