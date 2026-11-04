package store

import (
	"path/filepath"

	"github.com/home-operations/flate/pkg/manifest"
)

// SourcePaths projects the source paths of a complete accepted inventory.
func (e *Emission) SourcePaths() []manifest.SourceEvidence {
	if e == nil || !e.accepted || !e.Complete || e.OutputReads == nil || !e.OutputReads.Valid() {
		return nil
	}
	if record, _ := e.family.Lookup(e.origin); record.StaticOutput && !e.origin.ReportHidden() {
		return nil
	}
	return sourcePaths(e.OutputReads.store, e.OutputReads, e.entries, e.family)
}

// ChildSourcePaths projects the source paths of a complete discovery batch.
func (s *Store) ChildSourcePaths(
	parent manifest.NamedResource,
	children []Child,
	index *manifest.DiscoveryIndex,
) []manifest.SourceEvidence {
	_, _, assessment := s.GetEmission(parent)
	defer assessment.Finish()
	assessment.Children, assessment.family, assessment.origin = children, index, parent
	entries := s.prepareAdmissions(
		parent,
		&assessment,
		assessment.previous,
		make(map[manifest.NamedResource]int, len(children)),
	)
	return sourcePaths(s, assessment.OutputReads, entries, index)
}

func sourcePaths(
	s *Store,
	reads *ReadSet,
	entries []admission,
	index *manifest.DiscoveryIndex,
) []manifest.SourceEvidence {
	p := sourcePathProjection{store: s, index: index, paths: []manifest.SourceEvidence{}}
	if reads != nil {
		if !reads.Valid() {
			return nil
		}
		p.addReadPaths(reads)
	}
	p.addEntryPaths(entries)
	if reads != nil && !reads.Valid() {
		return nil
	}
	return p.paths
}

type sourcePathProjection struct {
	store *Store
	index *manifest.DiscoveryIndex
	paths []manifest.SourceEvidence
}

func (p *sourcePathProjection) addRoots(object manifest.BaseManifest) {
	if object == nil {
		return
	}
	source, _, applies := object.SourcePath()
	if !applies {
		return
	}
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	artifact, _ := p.store.GetArtifact(source, ArtifactContent).(*SourceArtifact)
	if artifact == nil {
		return
	}
	identity := artifact.HistoryIdentity(source)
	for root := range manifest.SourceRoots(object) {
		p.paths = append(
			p.paths,
			manifest.SourceEvidence{Source: identity, Path: filepath.ToSlash(root), Subtree: true},
		)
	}
}

func (p *sourcePathProjection) addReadPaths(reads *ReadSet) {
	for input := reads.inputs; input != nil; input = input.next {
		if input.Ref.NamedResource == reads.ID() {
			p.addRoots(new(input.view).declaration())
		}
		binding := input.view.sourceBinding()
		{
			productionValues := input.view.child.Productions
			for productionIndex := range productionValues {
				production := productionValues[productionIndex]
				if production.File != "" && production.Source == binding.source && binding.artifact != nil {
					p.paths = append(p.paths, manifest.SourceEvidence{
						Source: binding.artifact.HistoryIdentity(binding.source),
						Path:   filepath.ToSlash(production.File),
					},
					)
				}
			}
		}
	}
}

func (p *sourcePathProjection) addEntryPaths(entries []admission) {
	{
		entryValues := entries
		for entryIndex := range entryValues {
			entry := entryValues[entryIndex]
			p.addRoots(entry.child.Obj)
			{
				productionValues2 := entry.child.Productions
				for productionIndex2 := range productionValues2 {
					production := productionValues2[productionIndex2]
					if production.File != "" && production.Source == entry.binding.source &&
						entry.binding.artifact != nil {
						p.paths = append(p.paths, manifest.SourceEvidence{
							Source: entry.binding.artifact.HistoryIdentity(entry.binding.source),
							Path:   filepath.ToSlash(production.File),
						},
						)
					}
				}
			}
			p.addReferencePaths(entry.child.Obj)
		}
	}
}

func (p *sourcePathProjection) addReferencePaths(object manifest.BaseManifest) {
	for ref := range manifest.References(object, manifest.ReferenceRelations) {
		if !ref.ContentInput() || ref.Name == "" {
			continue
		}
		facts := p.store.ReferenceFacts(ref.NamedResource, p.index)
		if facts.Supplied != nil {
			continue
		}
		{
			productionValues3 := facts.Productions
			for productionIndex3 := range productionValues3 {
				production := productionValues3[productionIndex3]
				if production.File == "" {
					continue
				}
				artifact, _ := p.store.GetArtifact(production.Source, ArtifactContent).(*SourceArtifact)
				if artifact != nil {
					p.paths = append(p.paths, manifest.SourceEvidence{
						Source: artifact.HistoryIdentity(production.Source),
						Path:   filepath.ToSlash(production.File),
					},
					)
				}
			}
		}
	}
}
