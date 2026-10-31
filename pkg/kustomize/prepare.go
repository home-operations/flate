package kustomize

import (
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones ks and resolves referenced and inline substitutions.
func Prepare(ks *manifest.Kustomization, provider values.Provider) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReference(ks, provider); err != nil {
		return nil, err
	}
	return ks, nil
}

type sourceReader interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
}

// SourceInput resolves the ordinary Kustomization source and local ignore policy.
func SourceInput(reads sourceReader, ks *manifest.Kustomization) (*store.SourceArtifact, bool, error) {
	srcID := ks.SourceID()
	if srcID.Kind == "" || srcID.Name == "" {
		if ks.Path == "" {
			return nil, false, manifest.NewError(manifest.ErrInput, nil, "has no path and no source")
		}
		srcID = manifest.BootstrapSourceID
	}
	art := reads.GetArtifact(srcID, store.ArtifactContent)
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa, srcID == manifest.BootstrapSourceID || sa.InputAlias || sa.Content == "", nil
	}
	return nil, false, manifest.NewError(manifest.ErrFlux, nil, fmt.Sprintf("unsupported source artifact type %T for %s", art, manifest.DiagnosticFor(srcID, reads.GetObject(srcID))))
}
