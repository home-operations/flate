package loader

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_SharedProducerNamespaces(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"owners.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: selected, namespace: flux-system}
spec: {path: ./selected, targetNamespace: selected, components: [../shared]}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sibling, namespace: flux-system}
spec: {path: ./sibling, targetNamespace: sibling, components: [../shared]}
`)},
		"selected/kustomization.yaml": {Data: []byte("resources: [payload.yaml]\n")},
		"selected/payload.yaml":       {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: changed}\n")},
		"sibling/kustomization.yaml":  {Data: []byte("resources: []\n")},
		"shared/kustomization.yaml":   {Data: []byte("apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [external.yaml]\n")},
		"shared/external.yaml":        {Data: []byte("apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: credentials}\nspec: {target: {name: credentials}, dataFrom: [{extract: {key: credentials}}]}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"selected/payload.yaml"})
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	selected := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "selected"}
	sibling := selected
	sibling.Name = "sibling"
	assert.Equal(t, slices.Contains(proposal.Selected, selected), true)
	assert.Equal(t, slices.Contains(proposal.Needed, sibling), false)
}

func TestAdmission_MissingInputWaitsForKnownOwner(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "complete"}[complete], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "missing"}}}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./owned"}
			opaque := &manifest.HelmRelease{Name: "opaque", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{consumer, owner, opaque})
			if complete {
				st.RecordEmitted(owner.Named(), &store.Emission{Complete: true}, nil)
				st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady})
			}
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer.Named()}}
			l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {Parent: owner.Named(), Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}}},
			}}
			occurrence := compactOccurrence(opaque, manifest.ProjectProducer(opaque))
			occurrence.Declaration.Source = root
			l.locations = map[manifest.NamedResource]Occurrence{opaque.Named(): occurrence}
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, opaque.Named()), false)
			assert.Equal(t, slices.Contains(proposal.Selected, opaque.Named()), false)
		})
	}
}
