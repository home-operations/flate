package loader

import (
	"cmp"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) projectTemplates() {
	for owner, occurrence := range l.locations {
		if occurrence.facts == nil || len(occurrence.facts.templateObjects) == 0 {
			continue
		}
		parent, ok := l.Index.Lookup(owner)
		if !ok {
			continue
		}
		var productions []manifest.Production
		for _, production := range parent.Productions {
			source := production.Source
			if source == (manifest.NamedResource{}) {
				source = manifest.BootstrapSourceID
			}
			input, _ := l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			if input == nil || input.OriginPath(input.LocalPath) != occurrence.Declaration.Source {
				continue
			}
			if !slices.ContainsFunc(productions, func(p manifest.Production) bool { return p.Source == source }) {
				productions = append(productions, manifest.Production{Owner: owner, Declaration: owner, Source: source})
			}
		}
		if len(productions) == 0 && occurrence.Declaration.Source == l.SourceRoot {
			productions = []manifest.Production{{Owner: owner, Declaration: owner, Source: manifest.BootstrapSourceID}}
		}
		if len(productions) == 0 {
			continue
		}
		for _, object := range occurrence.facts.templateObjects {
			id := object.Named()
			if id.Namespace == "" && id.KindInfo().Namespaced() {
				object = object.WithNamespace(owner.Namespace)
				id = object.Named()
			}
			if id == owner {
				continue
			}
			previous, known := l.Index.Lookup(id)
			if known && previous.Path != "" {
				continue
			}
			contributors := productions
			if known {
				contributors = slices.Clone(previous.Productions)
				for _, production := range productions {
					if !slices.Contains(contributors, production) {
						contributors = append(contributors, production)
					}
				}
				slices.SortFunc(contributors, func(a, b manifest.Production) int {
					return cmp.Or(a.Owner.Compare(b.Owner), a.Declaration.Compare(b.Declaration), a.Authored.Compare(b.Authored), a.Source.Compare(b.Source), strings.Compare(a.Path, b.Path), strings.Compare(a.File, b.File))
				})
			}
			if known && previous.Parent != (manifest.NamedResource{}) && previous.Parent.Compare(owner) < 0 {
				previous.Productions = contributors
				l.Index.Records[id] = previous
				continue
			}
			if l.candidates == nil {
				l.candidates = map[manifest.NamedResource]store.Child{}
			}
			l.candidates[id] = store.Child{Obj: object}
			record := manifest.DiscoveryRecord{File: parent.File, Parent: owner, Productions: contributors}
			record.Refs = discoveryReferences(nil, object)
			l.Index.Records[id] = record
		}
	}
}
