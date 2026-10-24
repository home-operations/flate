package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type sourceReader interface {
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource) store.Artifact
}

// Sources resolves chart declarations and content from one operation's inputs.
type Sources struct {
	reader sourceReader
}

// NewSources binds chart resolution to the supplied captured or completed inputs.
func NewSources(reader sourceReader) Sources { return Sources{reader: reader} }

// HelmRepository returns the selected repository declaration.
func (s Sources) HelmRepository(namespace, name string) *manifest.HelmRepository {
	obj, _ := s.reader.GetDeclaration(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: namespace, Name: name}).(*manifest.HelmRepository)
	return obj
}

// LocalSourceArtifact returns the selected source content.
func (s Sources) LocalSourceArtifact(id manifest.NamedResource) *store.SourceArtifact {
	if s.reader == nil {
		return nil
	}
	art, _ := s.reader.GetArtifact(id).(*store.SourceArtifact)
	return art
}

// HelmChart returns the selected chart declaration.
func (s Sources) HelmChart(id manifest.NamedResource) *manifest.HelmChartSource {
	obj, _ := s.reader.GetDeclaration(id).(*manifest.HelmChartSource)
	return obj
}
