package loader

import (
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/claimpath"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type rootOwnerInventory struct {
	identities map[manifest.NamedResource]selection.RootCoveringOwner
	sources    map[string][]*rootOwnerPaths
}

type rootOwnerPaths struct {
	input *store.SourceArtifact
	paths map[string][]manifest.NamedResource
}

func rootCoveringFacts(objects admissionReader, index *manifest.DiscoveryIndex) *rootOwnerInventory {
	owners := &rootOwnerInventory{
		identities: map[manifest.NamedResource]selection.RootCoveringOwner{},
		sources:    map[string][]*rootOwnerPaths{},
	}
	inputs := map[*store.SourceArtifact]*rootOwnerPaths{}
	for info := range manifest.Kinds() {
		if !info.Structural {
			continue
		}
		for _, object := range objects.ListProductionCandidates(info.Group, info.Kind, index, nil) {
			fact := coveringFact(object.Named(), object, objects)
			owners.identities[fact.Identity] = fact
			if fact.Input == nil || !safepath.Contains(fact.Root, fact.Path) {
				continue
			}
			paths := inputs[fact.Input]
			if paths == nil {
				paths = &rootOwnerPaths{input: fact.Input, paths: map[string][]manifest.NamedResource{}}
				inputs[fact.Input] = paths
				owners.sources[fact.Input.LocalPath] = append(owners.sources[fact.Input.LocalPath], paths)
				if fact.Root != fact.Input.LocalPath {
					owners.sources[fact.Root] = append(owners.sources[fact.Root], paths)
				}
			}
			path := strings.TrimSuffix(filepath.ToSlash(filepath.Clean(fact.Path)), "/") + "/"
			if path == "./" {
				path = ""
			}
			paths.paths[path] = append(paths.paths[path], fact.Identity)
		}
	}
	return owners
}

func (o *rootOwnerInventory) candidates(evidence *selection.RootEvidence) []manifest.NamedResource {
	var pending []manifest.NamedResource
	for i := range evidence.Productions {
		production := &evidence.Productions[i]
		if owner := production.Materializer(
			evidence.Identity,
		); owner != (manifest.NamedResource{}) && owner != evidence.Identity &&
			!slices.Contains(pending, owner) {
			pending = append(pending, owner)
		}
	}
	if o == nil {
		return pending
	}
	source := evidence.Occurrence.Declaration.Source
	if source == "" {
		source = evidence.Sources[0].Origin
	}
	start := len(pending)
	for _, input := range o.sources[source] {
		file := strings.TrimSuffix(
			filepath.ToSlash(filepath.Clean(input.input.OriginPath(evidence.Record.Path))),
			"/",
		) + "/"
		for end, ids := range claimpath.Groups(input.paths, file) {
			if end == 0 && !filepath.IsLocal(file) {
				continue
			}
			for _, id := range ids {
				if !slices.Contains(pending, id) {
					pending = append(pending, id)
				}
			}
		}
	}
	slices.SortFunc(pending[start:], manifest.NamedResource.Compare)
	return pending
}
