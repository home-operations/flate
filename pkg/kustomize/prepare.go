package kustomize

import (
	"fmt"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones ks and resolves referenced and inline substitutions.
func Prepare(
	ks *manifest.Kustomization,
	provider values.Provider,
	missing *depwait.MissingPolicy,
) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReference(ks, provider, missing); err != nil {
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
	if (ks.SourceRef.Kind == "" || ks.SourceRef.Name == "") && ks.Path == "" {
		return nil, false, manifest.NewError(manifest.ErrInput, nil, "has no path and no source")
	}
	srcID := ks.EffectiveSourceID()
	art := reads.GetArtifact(srcID, store.ArtifactContent)
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa, ApplySourceIgnore(ks, sa), nil
	}
	return nil, false, manifest.NewError(
		manifest.ErrFlux,
		nil,
		fmt.Sprintf(
			"unsupported source artifact type %T for %s",
			art,
			manifest.DiagnosticFor(srcID, reads.GetObject(srcID)),
		),
	)
}

// ApplySourceIgnore preserves source-controller filtering for ordinary acquired content.
func ApplySourceIgnore(ks *manifest.Kustomization, source *store.SourceArtifact) bool {
	return ks.EffectiveSourceID() == manifest.BootstrapSourceID || source.InputAlias || source.Content == ""
}
