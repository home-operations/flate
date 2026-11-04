package loader

import (
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFilter_CurrentSideEvidenceAmbiguity(t *testing.T) {
	t.Parallel()
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	owner := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
	}
	head := store.New()
	head.AddObjects([]manifest.BaseManifest{source, owner})
	current := &store.SourceArtifact{
		LocalPath:  "/head",
		LocalRoot:  "/head",
		InputAlias: true,
		FS:         fstest.MapFS{"apps/cm.yaml": {Data: []byte("B\n")}},
	}
	head.SetArtifact(source.Named(), current)
	head.SetArtifact(manifest.BootstrapSourceID, current)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		source.Named(): {
			File: "flux/source.yaml",
			Productions: []manifest.Production{
				{Source: manifest.BootstrapSourceID, File: "flux/source.yaml", Path: "/head/flux/source.yaml"},
			},
		},
		owner.Named(): {
			File: "flux/ks.yaml",
			Refs: []manifest.NamedResource{source.Named()},
			Productions: []manifest.Production{
				{Owner: owner.Named(), Source: source.Named(), File: "apps/cm.yaml", Path: "/head/apps/cm.yaml"},
			},
		},
	}, Claims: map[string][]manifest.NamedResource{"apps/": {owner.Named()}}, Readers: map[string][]manifest.FileReader{"apps/cm.yaml": {{Reader: owner.Named(), Source: source.Named()}}}}
	head.SetDiscoveryIndex(index)
	selectedNames := []string{source.Named().String(), owner.Named().String()}
	slices.Sort(selectedNames)
	var unqualified [2][]string
	for i, payload := range []string{"B\n", "A\n"} {
		before := store.New()
		before.AddObjects([]manifest.BaseManifest{source, owner})
		input := &store.SourceArtifact{
			LocalPath: "/base",
			LocalRoot: "/base",
			FS:        fstest.MapFS{"apps/cm.yaml": {Data: []byte("A\n")}},
		}
		before.SetArtifact(manifest.BootstrapSourceID, input)
		before.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalPath: "/selected",
				LocalRoot: "/base",
				Key:       "committed",
				Content:   "committed",
				FS:        fstest.MapFS{"apps/cm.yaml": {Data: []byte(payload)}},
			},
		)
		paths, err := change.Detect(t.Context(), input, current, manifest.WorkScope{})
		assert.NoError(t, err)
		assert.Diff(t, paths.Paths(), []string{"apps/cm.yaml"})
		unqualified[i] = newAdmission(
			t,
			admissionConfig{changes: paths, index: index, inventory: head, root: ""},
		).KeepNames()
		assert.Diff(t, unqualified[i], selectedNames)
		evidence, err := change.Qualify(t.Context(), paths, [2]*store.Store{before, head}, manifest.WorkScope{})
		assert.NoError(t, err)
		got := newAdmission(t, admissionConfig{changes: evidence, index: index, inventory: head, root: ""}).KeepNames()
		want := []string{}
		if i == 1 {
			want = selectedNames
		}
		assert.Diff(t, got, want)
		t.Logf(
			"same current Store=%p/index=%p/current artifact=%p; bootstrap delta=%v; peer selected payload=%q; unqualified keep=%v; required paired keep=%v",
			head,
			index,
			current,
			paths.Paths(),
			payload,
			unqualified[i],
			got,
		)
	}
	assert.Diff(t, unqualified[0], unqualified[1])
}

func TestFilter_PrimaryRolesAndCycles(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := &manifest.GitRepository{Name: "shared", Namespace: "apps"}
	a := &manifest.Kustomization{
		Name:      "a",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "from-b", Optional: true},
			},
		},
	}
	b := a.Clone()
	b.Name = "b"
	b.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "from-a", Optional: true}},
	}
	peer := a.Clone()
	peer.Name, peer.PostBuild = "unrelated", nil
	st.AddObjects([]manifest.BaseManifest{source, a, b, peer})
	fromA := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "from-a"}
	fromB := fromA
	fromB.Name = "from-b"
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		a.Named(): {File: "a.yaml"}, b.Named(): {File: "b.yaml"}, peer.Named(): {File: "peer.yaml"},
		fromA: {Productions: []manifest.Production{{Owner: a.Named(), Source: manifest.BootstrapSourceID}}},
		fromB: {Productions: []manifest.Production{{Owner: b.Named(), Source: manifest.BootstrapSourceID}}},
	}}
	filter := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"a.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	assert.Diff(
		t,
		filter.keep,
		map[manifest.NamedResource]role{
			a.Named():      primary,
			b.Named():      primary,
			fromA:          primary,
			fromB:          primary,
			source.Named(): kept,
		},
	)
	assert.Equal(t, filter.ShouldReconcile(peer.Named()), false)

	c := a.Clone()
	c.Name = "c"
	c.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: fromA.Name, Optional: true},
		},
	}
	fromC := fromA
	fromC.Name = "from-c"
	b = b.Clone()
	b.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: fromC.Name, Optional: true},
		},
	}
	st.AddObjects([]manifest.BaseManifest{b, c})
	next := *index
	next.Records = maps.Clone(index.Records)
	next.Records[c.Named()] = manifest.DiscoveryRecord{File: "c.yaml"}
	next.Records[fromC] = manifest.DiscoveryRecord{
		Productions: []manifest.Production{{Owner: c.Named(), Source: manifest.BootstrapSourceID}},
	}
	filter = newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"a.yaml"}), index: &next, inventory: st, root: "/repo"},
	)
	assert.Diff(
		t,
		filter.keep,
		map[manifest.NamedResource]role{
			a.Named():      primary,
			b.Named():      primary,
			c.Named():      primary,
			fromA:          primary,
			fromB:          primary,
			fromC:          primary,
			source.Named(): kept,
		},
	)
	assert.Equal(t, filter.ShouldReconcile(peer.Named()), false)
}
