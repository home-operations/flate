package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ChangedDeclarationsPrepareLinearly(t *testing.T) {
	t.Parallel()
	for _, count := range []int{32, 128} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			testAdmissionChangedDeclarationsPrepareLinearlyCase(
				t,
				&count,
			)
		},
		)
	}
}

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
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
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
		proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
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

func TestAdmission_StableAncestorsPrepareLinearly(t *testing.T) {
	t.Parallel()
	for _, count := range []int{32, 128} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{"unrelated.yaml"})
			l.Options.Filter = change.NewFilter()
			l.locations = map[manifest.NamedResource]Occurrence{}
			var parent manifest.NamedResource
			for i := range count {
				object := &manifest.Kustomization{Name: fmt.Sprintf("item-%05d", i), Namespace: "apps"}
				object.Path = "./apps"
				id := object.Named()
				file := fmt.Sprintf("declarations/%05d.yaml", i)
				l.Index.Records[id] = manifest.DiscoveryRecord{File: file, Parent: parent, Declaration: object}
				occurrence := compactOccurrence(object, projectedProducerForTest(object))
				occurrence.Declaration = OccurrenceID{Source: root, File: file}
				l.locations[id] = occurrence
				parent = id
			}
			reads := &admissionArtifactReads{admissionReader: st}
			c := admissionClosure{loader: l, objects: reads}
			c.proposal = &c.result
			proposal, err := c.evaluate(t.Context(), new(PrepareRequest{Invocation: true}))
			assert.NoError(t, err)
			assert.Equal(t, len(proposal.Needed), 0)
			t.Logf("%d stable ancestors: %d artifact reads", count, reads.calls)
			if reads.calls > 128*count+128 {
				t.Errorf(
					"%d stable ancestors caused %d artifact reads; want at most %d",
					count,
					reads.calls,
					128*count+128,
				)
			}
		})
	}
}

func testAdmissionChangedDeclarationsPrepareLinearlyCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	l := publishedAdmissionDeclarations(t, *testCaseInput)
	st := l.Store
	var required store.ReadSet
	required.Init(st, manifest.NamedResource{}, nil)
	defer required.Close()
	ids := make([]manifest.NamedResource, 0, (*testCaseInput))
	for _, object := range st.Objects() {
		ids = append(ids, object.Named())
		required.RequireData(object.Named())
	}
	revision := l.Options.Filter.Membership(manifest.NamedResource{}).Revision
	_, retry := l.Options.Filter.AddEmitted(
		change.MembershipUpdate{Needed: ids, Selected: ids, Dirty: ids, Ordinary: ids},
		revision,
	)
	assert.Equal(t, retry, false)
	reads := &admissionArtifactReads{admissionReader: st}
	c := admissionClosure{loader: l, objects: reads}
	c.proposal = &c.result
	proposal, err := c.evaluate(t.Context(), new(PrepareRequest{Reads: &required}))
	assert.NoError(t, err)
	assert.Equal(t, len(proposal.Needed), 0)
	assert.Equal(t, len(proposal.Selected), 0)
	assert.Equal(t, len(proposal.Objects), 0)
	assert.Equal(t, reads.calls, 0)
	assert.Equal(t, reads.lists, 0)
	calls := 0
	for _, object := range st.Objects() {
		var proof store.ReadSet
		proof.Init(st, object.Named(), nil)
		readsResult := &admissionArtifactReads{admissionReader: &proof}
		cResult := admissionClosure{
			loader:   l,
			objects:  readsResult,
			incoming: store.Child{Obj: object, Productions: st.Productions(object.Named(), l.Index)},
		}
		cResult.proposal = &cResult.result
		evaluateProposal, evaluateErr := cResult.evaluate(t.Context(), new(PrepareRequest{Reads: &proof}))
		proof.Close()
		assert.NoError(t, evaluateErr)
		assert.Equal(t, len(evaluateProposal.Objects), 0)
		assert.Equal(t, readsResult.lists, 1)
		calls += readsResult.calls
	}
	t.Logf("%d dispatches: %d artifact reads", (*testCaseInput), calls)
	if calls > 32*(*testCaseInput)+32 {
		t.Errorf(
			"%d declarations reopened %d artifact reads during dispatch; want at most %d",
			(*testCaseInput),
			calls,
			32*(*testCaseInput)+32,
		)
	}
	child := store.Child{
		Obj:         &manifest.ConfigMap{Name: "emitted", Namespace: "apps"},
		Productions: []manifest.Production{{Owner: ids[0]}},
	}
	reads = &admissionArtifactReads{admissionReader: st}
	c = admissionClosure{loader: l, objects: reads, incoming: child}
	c.proposal = &c.result
	proposal, err = c.evaluate(t.Context(), new(PrepareRequest{Invocation: true}))
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, child.Obj.Named()), true)
	assert.Equal(t, slices.Contains(proposal.Selected, child.Obj.Named()), true)
	t.Logf("emitted data child with %d unrelated declarations: %d inventories", (*testCaseInput), reads.lists)
	assert.Equal(t, reads.lists, 0)
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
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes = change.NewSet([]string{"removed/child.yaml"})
	l.Options.Filter = change.NewFilter()
	owner := testutil.HelmReleaseID("apps", "emitter")
	if *testCaseInput {
		l.Options.HistoryOwners = map[manifest.NamedResource][]manifest.SourceEvidence{
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
		proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
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
