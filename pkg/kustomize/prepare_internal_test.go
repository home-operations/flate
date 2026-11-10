package kustomize

import (
	"errors"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestApplySourceIgnore_Policy(t *testing.T) {
	t.Parallel()
	ordinary := &manifest.Kustomization{Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{
		Kind: manifest.KindGitRepository, Name: "source",
	}}
	for _, tc := range []struct {
		name   string
		ks     *manifest.Kustomization
		source *store.SourceArtifact
		want   bool
	}{
		{name: "implicit bootstrap", ks: &manifest.Kustomization{Path: "."}, source: &store.SourceArtifact{Content: "snapshot"}, want: true},
		{
			name: "explicit bootstrap",
			ks: manifest.NewEntryKustomization(
				manifest.BootstrapSourceID, ".", manifest.NamedResource{},
			),
			source: &store.SourceArtifact{Content: "snapshot"}, want: true,
		},
		{name: "input alias", ks: ordinary, source: &store.SourceArtifact{Content: "snapshot", InputAlias: true}, want: true},
		{name: "empty content", ks: ordinary, source: &store.SourceArtifact{}, want: true},
		{name: "acquired content", ks: ordinary, source: &store.SourceArtifact{Content: "snapshot"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ApplySourceIgnore(tc.ks, tc.source), tc.want)
		})
	}
}

func TestSourceInput_Validation(t *testing.T) {
	t.Parallel()
	ordinary := kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source"}
	acquired := &store.SourceArtifact{Content: "snapshot"}
	for _, tc := range []struct {
		name     string
		path     string
		ref      kustomizev1.CrossNamespaceSourceReference
		artifact store.Artifact
		lookup   manifest.NamedResource
		category error
		message  string
	}{
		{name: "omitted source without path", category: manifest.ErrInput, message: "has no path and no source"},
		{
			name: "missing name without path", ref: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository},
			category: manifest.ErrInput, message: "has no path and no source",
		},
		{
			name: "missing kind without path", ref: kustomizev1.CrossNamespaceSourceReference{Name: "source"},
			category: manifest.ErrInput, message: "has no path and no source",
		},
		{
			name: "unavailable bootstrap", path: ".", lookup: manifest.BootstrapSourceID,
			category: manifest.ErrFlux,
			message:  "unsupported source artifact type <nil> for GitRepository.source.toolkit.fluxcd.io/flux-system/flux-system",
		},
		{
			name: "unsupported bootstrap artifact", path: ".", artifact: &store.Emission{}, lookup: manifest.BootstrapSourceID,
			category: manifest.ErrFlux,
			message:  "unsupported source artifact type *store.Emission for GitRepository.source.toolkit.fluxcd.io/flux-system/flux-system",
		},
		{
			name: "unavailable explicit source", path: ".", ref: ordinary,
			lookup:   manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"},
			category: manifest.ErrFlux,
			message:  "unsupported source artifact type <nil> for GitRepository.source.toolkit.fluxcd.io/apps/source",
		},
		{
			name: "explicit source without path", ref: ordinary, artifact: acquired,
			lookup: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reads := &sourceInputFixture{artifact: tc.artifact}
			ks := &manifest.Kustomization{Namespace: "apps", Path: tc.path, SourceRef: tc.ref}
			source, ignore, err := SourceInput(reads, ks)
			assert.Equal(t, reads.lookup, tc.lookup)
			if tc.category != nil {
				assert.FatalIf(t, err == nil, "expected source input error")
				assert.Equal(t, errors.Is(err, tc.category), true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, err.Error(), tc.message)
				assert.Equal(t, source, (*store.SourceArtifact)(nil))
				assert.Equal(t, ignore, false)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, source, acquired)
			assert.Equal(t, ignore, false)
		})
	}
}

type sourceInputFixture struct {
	artifact store.Artifact
	lookup   manifest.NamedResource
}

//nolint:ireturn // The source reader contract returns manifest implementations.
func (*sourceInputFixture) GetObject(manifest.NamedResource) manifest.BaseManifest { return nil }

//nolint:ireturn // The source reader contract returns heterogeneous artifacts.
func (f *sourceInputFixture) GetArtifact(id manifest.NamedResource, _ store.ArtifactRead) store.Artifact {
	f.lookup = id
	return f.artifact
}
