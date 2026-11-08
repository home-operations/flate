package loader

import (
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_ParentPatchRemoval(t *testing.T) {
	t.Parallel()
	var stores [2]*store.Store
	var loaders [2]*Loader
	for side := range stores {
		root := t.TempDir()
		files := fstest.MapFS{
			"flux/consumer.yaml": {
				Data: []byte(
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n interval: 10m\n path: ./app\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n postBuild:\n  substituteFrom: [{kind: ConfigMap, name: vars}]\n patches:\n - target: {kind: HelmRelease, name: a}\n   patch: |\n    - op: add\n      path: /metadata/annotations\n      value: {kustomize.toolkit.fluxcd.io/substitute: disabled}\n",
				),
			},
			"flux/vars.yaml": {
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata:\n FLAG: old\n",
				),
			},
			"app/kustomization.yaml": {Data: []byte("resources: [hr.yaml]\n")},
			"chart/Chart.yaml":       {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
			"chart/values.yaml":      {Data: []byte("value: original\n")},
			"chart/payload.txt":      {Data: []byte("original")},
			"chart/templates/cm.yaml": {
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"{{ .Release.Name }}\", namespace: apps}\ndata:\n value: {{ .Values.value | quote }}\n version: {{ .Chart.Version | quote }}\n file: {{ .Files.Get \"payload.txt\" | quote }}\n",
				),
			},
			"app/hr.yaml": {
				Data: []byte(
					"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: a, namespace: apps}\nspec:\n interval: 10m\n releaseName: a\n chart:\n  spec:\n   chart: ./chart\n   sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n values: {value: \"${FLAG}\"}\n",
				),
			},
		}
		if side == 1 {
			files["flux/vars.yaml"] = &fstest.MapFile{
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata:\n FLAG: new\n",
				),
			}
			files["flux/consumer.yaml"] = &fstest.MapFile{
				Data: []byte(
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n interval: 10m\n path: ./app\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n postBuild:\n  substituteFrom: [{kind: ConfigMap, name: vars}]\n",
				),
			}
		}
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files})
		l := New(st)
		l.SourceRoot = root
		configureSelection(t, l, selection.Request{Changed: true})

		_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		_, err = l.Load(t.Context(), filepath.Join(root, "app"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
		stores[side], loaders[side] = st, l
	}
	changes, err := change.Qualify(t.Context(), nil, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	for side, l := range loaders {
		l.Options.Changes = changes
		proposal, err := prepareSelection(
			t.Context(),
			t,
			l,
			selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
		)
		assert.NoError(t, err)
		id := (&manifest.Kustomization{Name: "consumer", Namespace: "flux-system"}).Named()
		assert.ErrorIf(
			t,
			!slices.ContainsFunc(proposal.Objects, func(child store.Child) bool { return child.Obj.Named() == id }),
			"side %d failed to hydrate changed parent",
			side,
		)
	}
}

func TestAdmission_HistoryOwnerDeclarationAndInputs(t *testing.T) {
	t.Parallel()
	for _, seeded := range []bool{false, true} {
		t.Run(strconv.FormatBool(seeded), func(t *testing.T) {
			t.Parallel()
			testAdmissionHistoryOwnerDeclarationAndInputsCase(
				t,
				&seeded,
			)
		},
		)
	}
}

func testAdmissionHistoryOwnerDeclarationAndInputsCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	root := t.TempDir()
	input := fstest.MapFS{
		"release.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: emitter, namespace: apps}\nspec:\n  interval: 1m\n  values: {marker: keep}\n  valuesFrom: [{kind: ConfigMap, name: supplied}]\n  chart: {spec: {chart: ./charts/demo, sourceRef: {kind: GitRepository, name: charts}}}\n",
			),
		},
		"values.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata: {values.yaml: '{}'}\n",
			),
		},
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet([]string{"removed/child.yaml"})

	owner := testutil.HelmReleaseID("apps", "emitter")
	var historyOwners map[manifest.NamedResource][]manifest.SourceEvidence
	if *testCaseInput {
		historyOwners = map[manifest.NamedResource][]manifest.SourceEvidence{
			owner: {
				{
					Source: st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).HistoryIdentity(
						manifest.BootstrapSourceID,
					),
					Path: "input.yaml",
				},
			},
		}
	}
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	for range 2 {
		proposal, err := prepareSelection(
			t.Context(),
			t,
			l,
			selection.PrepareRequest{HistoryOwners: historyOwners, Changes: l.Options.Changes, Invocation: true},
		)
		assert.NoError(t, err)
		assert.Equal(t, slices.Contains(proposal.Needed, owner), (*testCaseInput))
		assert.Equal(t, slices.Contains(proposal.Selected, owner), false)
		assert.Equal(t, slices.Contains(proposal.Dirty, owner), false)
		inputID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
		assert.Equal(t, slices.Contains(proposal.Needed, inputID), (*testCaseInput))
		found := false
		for i := range proposal.Objects {
			child := &proposal.Objects[i]
			if child.Obj.Named() == owner {
				declaration := child.Obj.(*manifest.HelmRelease)
				assert.Equal(t, declaration.Values["marker"], any("keep"))
				assert.Equal(t, len(declaration.ValuesFrom), 1)
				found = true
			}
		}
		assert.Equal(t, found, (*testCaseInput))
	}
}
