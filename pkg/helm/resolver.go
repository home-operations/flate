package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type sourceReader interface {
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
}

// Sources resolves chart declarations and content from one operation's inputs.
type Sources struct {
	reader sourceReader
}

// NewSources binds chart resolution to the supplied captured or completed inputs.
func NewSources(reader sourceReader) Sources { return Sources{reader: reader} }

// LocalSourceArtifact returns the selected source content.
func (s Sources) LocalSourceArtifact(id manifest.NamedResource, read store.ArtifactRead) *store.SourceArtifact {
	if s.reader == nil {
		return nil
	}
	art, _ := s.reader.GetArtifact(id, read).(*store.SourceArtifact)
	return art
}

// Declaration returns the captured chart-source declaration.
func (s Sources) Declaration(id manifest.NamedResource) manifest.BaseManifest {
	return s.reader.GetDeclaration(id)
}
