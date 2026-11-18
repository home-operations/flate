package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRootOwners_RelativeSourceBoundary(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: ".", FS: fstest.MapFS{}})
	owner := &manifest.Kustomization{Namespace: "apps", Name: "owner", Path: "."}
	factory := (&manifest.ResourceSet{Namespace: "apps", Name: "factory"}).Named()
	st.AddObject(owner)
	index := &manifest.DiscoveryIndex{
		Claims: map[string][]manifest.NamedResource{"": {owner.Named()}},
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			owner.Named(): {Productions: []manifest.Production{{Owner: factory}}},
		},
	}
	input := &store.SourceArtifact{LocalPath: "."}
	mapped, known := input.IndexPath("nested", "input.yaml")
	assert.Equal(t, mapped, "../input.yaml")
	assert.Equal(t, known, true)
	for _, tc := range []struct {
		name    string
		file    string
		covered bool
	}{
		{name: "descendant", file: "nested/input.yaml", covered: true},
		{name: "source itself", file: ".", covered: true},
		{name: "parent", file: "../input.yaml"},
		{name: "mapped parent", file: mapped},
		{name: "absolute", file: filepath.Join(t.TempDir(), "input.yaml")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			evidence := selection.RootEvidence{
				Record:     selection.RootRecord{Path: tc.file},
				Scope:      selection.RootScopeFacts{NamespaceEligible: true, Initial: true},
				Occurrence: selection.OccurrenceFacts{Declaration: selection.OccurrenceID{Source: "."}},
			}
			evidence = selection.CaptureRoot(evidence, st, index)
			assert.Equal(
				t,
				slices.ContainsFunc(
					evidence.CoveringOwners,
					func(candidate selection.RootCoveringOwner) bool { return candidate.Identity == owner.Named() },
				),
				tc.covered,
			)
		})
	}
}

func TestRootOwners_NativeComponent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"app/kustomization.yaml": {Data: []byte("resources: []\ncomponents: [../components]\n")},
		"components/kustomization.yaml": {
			Data: []byte("apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [child.yaml]\n"),
		},
	}})
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "app"}
	factory := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
	st.AddObject(factory)
	st.AddObject(owner)
	st.RecordEmitted(factory.Named(), &store.Emission{Children: []store.Child{{Obj: owner}}, Complete: true}, nil)
	index := Claims(st, root, []*manifest.Kustomization{owner})
	id := manifest.NamedResource{
		Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "child",
	}
	file := "components/child.yaml"
	index.Records[id] = manifest.DiscoveryRecord{Path: filepath.Join(root, file), File: file}
	assert.Diff(t, slices.Collect(index.ClaimOwners(file, manifest.BootstrapSourceID, id, nil)),
		[][]manifest.NamedResource{{owner.Named()}})
	captured := selection.CaptureRoot(selection.RootEvidence{
		Identity: id,
		Record:   selection.RootRecordFrom(new(index.Records[id])),
		Occurrence: selection.OccurrenceFacts{
			Declaration: selection.OccurrenceID{Source: root, File: file},
		},
		Scope: selection.RootScopeFacts{NamespaceEligible: true, Initial: true},
	}, st, index)
	assert.Equal(t, slices.ContainsFunc(captured.CoveringOwners, func(candidate selection.RootCoveringOwner) bool {
		return candidate.Identity == owner.Named()
	}), true)
	assert.Equal(t, selection.Root(captured).Owner, owner.Named())
}
