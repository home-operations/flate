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
	return sourcePaths(e.OutputReads.store, e.OutputReads, e.entries, e.family)
}

// ChildSourcePaths projects the source paths of a complete discovery batch.
func (s *Store) ChildSourcePaths(parent manifest.NamedResource, children []Child, index *manifest.DiscoveryIndex) []manifest.SourceEvidence {
	_, _, assessment := s.GetEmission(parent)
	defer assessment.Finish()
	assessment.Children, assessment.family, assessment.origin = children, index, parent
	entries := s.prepareAdmissions(parent, &assessment, assessment.previous, make(map[manifest.NamedResource]int, len(children)))
	return sourcePaths(s, assessment.OutputReads, entries, index)
}

func sourcePaths(s *Store, reads *ReadSet, entries []admission, index *manifest.DiscoveryIndex) []manifest.SourceEvidence {
	out := []manifest.SourceEvidence{}
	roots := func(object manifest.BaseManifest) {
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
		artifact, _ := s.GetArtifact(source, ArtifactContent).(*SourceArtifact)
		if artifact == nil {
			return
		}
		identity := artifact.HistoryIdentity(source)
		for root := range manifest.SourceRoots(object) {
			out = append(out, manifest.SourceEvidence{Source: identity, Path: filepath.ToSlash(root), Subtree: true})
		}
	}
	if reads != nil {
		if !reads.Valid() {
			return nil
		}
		for input := reads.inputs; input != nil; input = input.next {
			if input.Ref.NamedResource == reads.ID() {
				roots(input.view.declaration())
			}
			binding := input.view.sourceBinding()
			for _, production := range input.view.child.Productions {
				if production.File != "" && production.Source == binding.source && binding.artifact != nil {
					out = append(out, manifest.SourceEvidence{Source: binding.artifact.HistoryIdentity(binding.source), Path: filepath.ToSlash(production.File)})
				}
			}
		}
	}
	for _, entry := range entries {
		roots(entry.child.Obj)
		for _, production := range entry.child.Productions {
			if production.File != "" && production.Source == entry.binding.source && entry.binding.artifact != nil {
				out = append(out, manifest.SourceEvidence{Source: entry.binding.artifact.HistoryIdentity(entry.binding.source), Path: filepath.ToSlash(production.File)})
			}
		}
		for ref := range manifest.References(entry.child.Obj, manifest.ReferenceRelations) {
			if !ref.ContentInput() || ref.Name == "" {
				continue
			}
			facts := s.ReferenceFacts(ref.NamedResource, index)
			if facts.Supplied != nil {
				continue
			}
			for _, production := range facts.Productions {
				if production.File == "" {
					continue
				}
				artifact, _ := s.GetArtifact(production.Source, ArtifactContent).(*SourceArtifact)
				if artifact != nil {
					out = append(out, manifest.SourceEvidence{Source: artifact.HistoryIdentity(production.Source), Path: filepath.ToSlash(production.File)})
				}
			}
		}
	}
	if reads != nil && !reads.Valid() {
		return nil
	}
	return out
}
