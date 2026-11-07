package loader

import (
	"cmp"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) projectTemplates() {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for owner, occurrence := range l.locations {
		if occurrence.facts == nil || occurrence.requiresBuild {
			continue
		}
		parent, ok := l.Index.Lookup(owner)
		if !ok || parent.Builder != "" {
			continue
		}
		productions := l.templateProductions(owner, &occurrence, &parent)
		if len(productions) == 0 {
			continue
		}
		objects := append(slices.Clone(occurrence.facts.templateObjects), l.templateProjections[owner].Objects...)
		for _, object := range objects {
			l.projectTemplate(owner, parent.File, object, productions)
		}
	}
}

func (l *Loader) templateProductions(
	owner manifest.NamedResource,
	occurrence *authoredOccurrence,
	parent *manifest.DiscoveryRecord,
) []manifest.Production {
	var productions []manifest.Production
	{
		productionValues := parent.Productions
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			source := production.Source
			if source == (manifest.NamedResource{}) {
				source = manifest.BootstrapSourceID
			}
			input, _ := l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			if input == nil || input.OriginPath(input.LocalPath) != occurrence.Declaration.Source {
				continue
			}
			if !slices.ContainsFunc(
				productions,
				func(p manifest.Production) bool { return p.Source == source },
			) {
				productions = append(
					productions,
					manifest.Production{Owner: owner, Declaration: owner, Source: source},
				)
			}
		}
	}
	if len(productions) == 0 && occurrence.Declaration.Source == l.SourceRoot {
		productions = []manifest.Production{
			{Owner: owner, Declaration: owner, Source: manifest.BootstrapSourceID},
		}
	}
	return productions
}

func (l *Loader) projectTemplate(
	owner manifest.NamedResource,
	parentFile string,
	object manifest.BaseManifest,
	productions []manifest.Production,
) {
	id := object.Named()
	if id.Namespace == "" && id.KindInfo().Namespaced() {
		object = object.WithNamespace(owner.Namespace)
		id = object.Named()
	}
	if id == owner {
		return
	}
	previous, known := l.Index.Lookup(id)
	if known && previous.Path != "" {
		return
	}
	contributors := productions
	if known {
		contributors = slices.Clone(previous.Productions)
		{
			productionValues2 := productions
			for productionIndex2 := range productionValues2 {
				production := productionValues2[productionIndex2]
				if !slices.Contains(contributors, production) {
					contributors = append(contributors, production)
				}
			}
		}
		slices.SortFunc(contributors, func(a, b manifest.Production) int {
			return cmp.Or(
				a.Owner.Compare(b.Owner),
				a.Declaration.Compare(b.Declaration),
				a.Authored.Compare(b.Authored),
				a.Source.Compare(b.Source),
				strings.Compare(a.Path, b.Path),
				strings.Compare(a.File, b.File),
			)
		})
	}
	if known && previous.Parent != (manifest.NamedResource{}) && previous.Parent.Compare(owner) < 0 {
		previous.Productions = contributors
		l.Index.Records[id] = previous
		return
	}
	if l.candidates == nil {
		l.candidates = map[manifest.NamedResource]store.Child{}
	}
	l.candidates[id] = store.Child{Obj: object}
	record := manifest.DiscoveryRecord{File: parentFile, Parent: owner, Productions: contributors}
	record.Refs = discoveryReferences(nil, object)
	l.Index.Records[id] = record
}
