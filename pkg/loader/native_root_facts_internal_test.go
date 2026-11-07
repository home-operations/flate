package loader

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestNativeRootFacts_GraphMembershipAndBuildState(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "other/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [../shared]\n")
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "namespace: apps\nresources: [release.yaml]\n")
	testutil.WriteFile(t, root, "shared/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release}
spec:
  interval: 1h
  chart:
    spec: {chart: demo, sourceRef: {kind: HelmRepository, name: demo}}
`)
	st := store.New()
	admitForTest(st, root)
	owner := &manifest.Kustomization{Name: "other", Namespace: "apps", Path: "other"}
	st.AddObject(owner)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{
		{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease},
	}})
	entry := filepath.Join(root, "entry")
	_, err := l.Load(t.Context(), entry, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	before := l.SourceFacts(manifest.BootstrapSourceID, nil).Context.Roots
	assert.FatalIf(t, len(before) != 1, "native roots = %v", before)
	assert.Equal(t, before[0].Path, "entry")
	claims := slices.Clone(before[0].Claims)
	assert.FatalIf(t, len(claims) == 0, "missing physical owner claim")
	assert.Equal(t, slices.Contains(before[0].Identities, testutil.HelmReleaseID("apps", "release")), true)
	assert.Equal(t, selection.Root(selection.RootEvidence{Native: &before[0]}).Construction, true)
	l.RegisterBuilt(supportedBuiltInputForTest(l, &BuiltInput{
		Root: entry, Source: manifest.BootstrapSourceID,
		Owner: manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named(),
	}), nil)
	registered := l.SourceFacts(manifest.BootstrapSourceID, nil).Context.Roots
	assert.Equal(t, registered[0].Built, true)
	assert.Equal(t, selection.Root(selection.RootEvidence{Native: &registered[0]}).Construction, false)
	assert.Equal(t, before[0].Built, false)
	assert.Equal(t, selection.Root(selection.RootEvidence{Native: &before[0]}).Construction, true)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	after := l.SourceFacts(manifest.BootstrapSourceID, nil).Context.Roots
	assert.FatalIf(t, len(after) != 1, "native roots = %v", after)
	assert.Equal(t, selection.Root(selection.RootEvidence{Native: &after[0]}).Construction, false)
	assert.Equal(t, selection.Root(selection.RootEvidence{Native: &before[0]}).Construction, true)
	st.DeleteObject(owner.Named())
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	assert.Equal(t, len(l.SourceFacts(manifest.BootstrapSourceID, nil).Context.Roots[0].Claims), 0)
	assert.Diff(t, before[0].Claims, claims)
}
