package orchestrator

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCollectManifests_ResourceSetChildEligibility(t *testing.T) {
	t.Parallel()

	for _, skip := range [][]string{nil, {manifest.KindConfigMap, manifest.KindSecret}} {
		t.Run(fmt.Sprintf("skip=%v", skip), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := testutil.KustomizationID("apps", "parent")
			st.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
			rs := &manifest.ResourceSet{Name: "mixed", Namespace: "apps"}
			st.AddObject(rs)
			batch, err := testutil.DecodeDocs(strings.NewReader(`apiVersion: v1
kind: ServiceAccount
metadata: {name: raw, namespace: apps}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: values, namespace: apps}
data: {key: value}
---
apiVersion: v1
kind: Secret
metadata: {name: credentials, namespace: apps}
stringData: {key: value}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: inputs, namespace: apps}
spec: {type: Static, defaultValues: {name: one}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: nested, namespace: apps}
spec: {interval: 1m, path: ./nested, sourceRef: {kind: GitRepository, name: flux-system}}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: nested, namespace: apps}
spec:
  interval: 1m
  chart:
    spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}}
`))
			assert.NoError(t, err)
			parser := base.New(st, nil)
			parser.Configure(base.Options{SkipKinds: skip})
			children, docs := testutil.ParseChildren(parser, rs, batch, manifest.ParseDocOptions{})
			{
				assert.FatalIf(
					t,
					len(children) != len(batch),
					"parsed %d of %d mixed children",
					len(children),
					len(batch),
				)
			}
			st.RecordEmitted(
				rs.Named(),
				&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true},
				nil,
			)
			ks := testutil.KustomizationID("apps", "nested")
			hr := testutil.HelmReleaseID("apps", "nested")
			ksDocs := []map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ServiceAccount",
					"metadata":   map[string]any{"name": "from-ks", "namespace": "apps"},
				},
			}
			hrDocs := []map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ServiceAccount",
					"metadata":   map[string]any{"name": "from-hr", "namespace": "apps"},
				},
			}
			st.RecordEmitted(ks, &store.Emission{Manifests: testutil.DocumentOccurrences(ksDocs), Complete: true}, nil)
			st.RecordEmitted(hr, &store.Emission{Manifests: testutil.DocumentOccurrences(hrDocs), Complete: true}, nil)
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{rs.Named(): {Parent: parent}},
			}
			_, _, before := st.GetEmission(rs.Named())
			before.Finish()
			for i, child := range before.Children {
				before.Children[i].Doc = manifest.DeepCopyMap(child.Doc)
			}
			for i, doc := range before.Manifests {
				before.Manifests[i].Document = manifest.DeepCopyMap(doc.Document)
			}
			o := &Orchestrator{
				store: st,
				index: index,
				cfg:   treeConfig{Config: Config{HelmOptions: helm.Options{SkipKinds: skip}}, Tree: Tree{}},
			}
			parentDocs := docs[:4]
			if len(skip) > 0 {
				parentDocs = docs[:2]
			}
			assert.Diff(t, outputByParent(o.collectManifests()), map[manifest.NamedResource][]map[string]any{
				parent: parentDocs,
				ks:     ksDocs,
				hr:     hrDocs,
			})
			_, _, after := st.GetEmission(rs.Named())
			after.Finish()
			{
				{
					diff := cmp.Diff(
						after.Children,
						before.Children,
						cmp.AllowUnexported(manifest.Kustomization{}),
						cmpopts.IgnoreUnexported(store.Child{}),
						cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
						cmpopts.IgnoreUnexported(
							manifest.HelmRelease{},
							manifest.OCIRepository{},
							manifest.HelmChartSource{},
						),
					)
					assert.FatalIf(t, diff != "", "retained children changed (-want +got): %s", diff)
				}
			}
			assert.Diff(t, after.Manifests, before.Manifests)
			cm, ok := st.Get[*manifest.ConfigMap](children[1].Obj.Named())
			{
				assert.FatalIf(t, !ok || cm.Data["key"] != "value", "%v", "collection discarded ConfigMap values")
			}
			secret, ok := st.Get[*manifest.Secret](children[2].Obj.Named())
			{
				assert.FatalIf(
					t,
					!ok || secret.StringData["key"] != "value",
					"%v",
					"collection discarded Secret values",
				)
			}
		})
	}
}

func TestCollectManifests_ArtifactGenerators(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		emitted, orphan, rival bool
		skip                   bool
	}{
		{name: "static"},
		{name: "orphan", orphan: true},
		{name: "emitted", emitted: true},
		{name: "parentless_resourceset_wins", rival: true},
		{name: "skip_generated_kind", emitted: true, skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCollectManifestsArtifactGeneratorsCase(t, &tc)
		},
		)
	}
}

func outputByParent(batches []manifest.DocumentBatch) map[manifest.NamedResource][]map[string]any {
	out := make(map[manifest.NamedResource][]map[string]any, len(batches))
	for _, batch := range batches {
		if batch.Parent != nil {
			out[batch.Parent.Named()] = batch.Documents
		}
	}
	return out
}

func TestCollectManifests_CoveredRetainedOutput(t *testing.T) {
	t.Parallel()
	for _, survivor := range []bool{false, true} {
		t.Run(strconv.FormatBool(survivor), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			peer := &manifest.Kustomization{Name: "peer", Namespace: "apps"}
			raw := &manifest.HelmRelease{Name: "release", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{parent, peer, raw})
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Named(): {
					Productions: []manifest.Production{
						{Owner: parent.Named(), Source: manifest.BootstrapSourceID},
						{Owner: peer.Named(), Source: manifest.BootstrapSourceID},
					},
				},
			}})
			accepted := raw.Clone()
			accepted.Chart.Name = "accepted"
			st.RecordEmitted(
				parent.Named(),
				captureEmission(st, parent.Named(), &store.Emission{Children: []store.Child{{Obj: accepted}}}),
				nil,
			)
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "accepted-output"},
			}
			st.RecordEmitted(
				raw.Named(),
				captureEmission(
					st,
					raw.Named(),
					&store.Emission{Manifests: testutil.DocumentOccurrences([]map[string]any{doc}), Complete: true},
				),
				nil,
			)
			if survivor {
				st.RecordEmitted(
					peer.Named(),
					captureEmission(st, peer.Named(), &store.Emission{Children: []store.Child{{Obj: accepted}}}),
					nil,
				)
			}
			st.RecordEmitted(parent.Named(), captureEmission(st, parent.Named(), &store.Emission{Complete: true}), nil)
			st.UpdateStatus(
				raw.Named(),
				store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "failed successor"},
			)
			o := &Orchestrator{store: st}
			docs := outputByParent(o.collectManifests())[raw.Named()]
			{
				assert.FatalIf(
					t,
					survivor && (len(docs) != 1 || docs[0]["kind"] != "ConfigMap") || !survivor && len(docs) != 0,
					"retained output survivor=%t docs=%v",
					survivor,
					docs,
				)
			}
			{
				assert.FatalIf(
					t,
					!survivor && (st.GetObject(raw.Named()) == nil || st.GetDeclaration(raw.Named()) != nil),
					"%v",
					"coverage visibility and runnable authority were conflated",
				)
			}
		})
	}
}

func testCollectManifestsArtifactGeneratorsCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		emitted bool
		orphan  bool
		rival   bool
		skip    bool
	},
) {
	t.Helper()

	st := store.New()
	parser := base.New(st, nil)
	if testCaseInput.skip {
		parser.Configure(base.Options{SkipKinds: []string{manifest.KindExternalArtifact}})
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	var generatorDocs []map[string]any
	generators := make([]manifest.NamedResource, 0, 2)
	for _, name := range []string{"z", "a"} {
		doc := map[string]any{
			"apiVersion": "source.extensions.fluxcd.io/v1beta1",
			"kind":       "ArtifactGenerator",
			"metadata":   map[string]any{"name": name, "namespace": "apps"},
			"spec":       map[string]any{},
		}
		obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		assert.NoError(t, err)
		generators = append(generators, obj.Named())
		generatorDocs = append(generatorDocs, doc)
		if !testCaseInput.emitted {
			st.AddObject(obj, doc)
		}
		if testCaseInput.orphan {
			index.Records[obj.Named()] = manifest.DiscoveryRecord{Authored: obj.Named(), File: name + ".yaml"}
		}
	}
	nested := testutil.KustomizationID("apps", "nested")
	nestedDocs := []map[string]any{
		{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "nested-output", "namespace": "apps"},
		},
	}
	if testCaseInput.emitted {
		st.AddObject(parent)
		generatorDocs = append(
			generatorDocs,
			map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
				"kind":       manifest.KindKustomization,
				"metadata":   map[string]any{"name": nested.Name, "namespace": nested.Namespace},
				"spec":       map[string]any{},
			},
		)
		children, docs := parseChildrenForTest(parent.Named(), generatorDocs, manifest.ParseDocOptions{})
		st.RecordEmitted(
			parent.Named(),
			&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true},
			nil,
		)
		st.RecordEmitted(
			nested,
			&store.Emission{Manifests: testutil.DocumentOccurrences(nestedDocs), Complete: true},
			nil,
		)
	}
	outputs := map[string][]map[string]any{}
	shared := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "shared",
	}
	var byteWinner *store.SourceArtifact
	for _, generator := range generators {
		docs := []map[string]any{
			{
				"apiVersion": "source.toolkit.fluxcd.io/v1",
				"kind":       manifest.KindExternalArtifact,
				"metadata": map[string]any{
					"name":   "shared",
					"labels": map[string]any{"writer": generator.Name},
				},
			},
			{
				"apiVersion": "source.toolkit.fluxcd.io/v1",
				"kind":       manifest.KindExternalArtifact,
				"metadata":   map[string]any{"name": "own-" + generator.Name},
			},
		}
		children, accepted := testutil.ParseChildren(
			parser,
			st.GetObject(generator),
			docs,
			manifest.ParseDocOptions{DefaultNamespace: generator.Namespace},
		)
		payload := &store.SourceArtifact{
			Key: generator.Name,
			FS:  fstest.MapFS{"value": {Data: []byte(generator.Name)}},
		}
		st.RecordEmitted(
			generator,
			&store.Emission{
				Children:  children,
				Manifests: testutil.DocumentOccurrences(accepted),
				Complete:  true,
			},
			map[manifest.NamedResource]*store.SourceArtifact{shared: payload},
		)
		if generator.Name == "z" {
			byteWinner = payload
		}
		for _, child := range children {
			outputs[generator.Name] = append(outputs[generator.Name], child.Doc)
		}
	}
	testCollectManifestsArtifactGeneratorsCaseTail(
		t,
		&testCollectManifestsArtifactGeneratorsCaseTailInputs{
			testCaseInput: &testCaseInput,
			st:            &st,
			outputs:       &outputs,
			shared:        &shared,
			byteWinner:    &byteWinner,
			parent:        &parent,
			generatorDocs: &generatorDocs,
			nested:        &nested,
			nestedDocs:    &nestedDocs,
			index:         &index,
		},
	)
}

func testCollectManifestsArtifactGeneratorsCaseTail(
	t *testing.T,
	tailCaseInput *testCollectManifestsArtifactGeneratorsCaseTailInputs,
) {
	t.Helper()
	if (*tailCaseInput.testCaseInput).rival {
		rs := &manifest.ResourceSet{Name: "rival", Namespace: "apps"}
		(*tailCaseInput.st).
			AddObject(rs)
		rivalDoc := manifest.DeepCopyMap((*tailCaseInput.outputs)["z"][0])
		rivalDoc["metadata"].(map[string]any)["labels"] = map[string]any{"writer": "rival"}
		children, docs := parseChildrenForTest(
			rs.Named(),
			[]map[string]any{rivalDoc},
			manifest.ParseDocOptions{},
		)
		(*tailCaseInput.st).
			RecordEmitted(
				rs.Named(),
				&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true},
				nil,
			)
		_, declaration, attempt := (*tailCaseInput.st).GetEmission((*tailCaseInput.shared))
		attempt.Finish()
		assert.Diff(t, declaration, rivalDoc)
		assert.Equal(t, (*tailCaseInput.st).EmissionOwner((*tailCaseInput.shared)), rs.Named())
	}
	assert.Equal(
		t,
		(*tailCaseInput.st).GetArtifact((*tailCaseInput.shared), store.ArtifactContent),
		store.Artifact((*tailCaseInput.byteWinner)),
	)
	want := map[manifest.NamedResource][]map[string]any{}
	if (*tailCaseInput.testCaseInput).emitted {
		want[(*tailCaseInput.parent).Named()] = (*tailCaseInput.generatorDocs)
		want[(*tailCaseInput.nested)] = (*tailCaseInput.nestedDocs)
	}
	var generated []map[string]any
	if !(*tailCaseInput.testCaseInput).skip {
		for _, name := range []string{"a", "z"} {
			id := manifest.NamedResource{
				Group:     "source.extensions.fluxcd.io",
				Kind:      "ArtifactGenerator",
				Namespace: "apps",
				Name:      name,
			}
			docs := (*tailCaseInput.outputs)[name]
			if name == "a" || (*tailCaseInput.testCaseInput).rival {
				docs = docs[1:]
			}
			if (*tailCaseInput.testCaseInput).emitted {
				generated = append(generated, docs...)
			} else {
				want[id] = docs
			}
		}
	}
	if (*tailCaseInput.testCaseInput).emitted {
		want[(*tailCaseInput.parent).Named()] = append(generated, (*tailCaseInput.generatorDocs)...)
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
	(*tailCaseInput.st).
		AddObject(entry)
	bare := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "bare"}}
	children, docs, failures := base.New((*tailCaseInput.st), nil).
		ParseChildren(entry, testutil.DocumentOccurrences([]map[string]any{bare}), manifest.ParseDocOptions{})
	(*tailCaseInput.st).
		RecordEmitted(
			entry.Named(),
			&store.Emission{Children: children, Manifests: docs, Failures: failures, Complete: true},
			nil,
		)
	want[entry.Named()] = []map[string]any{bare}
	o := &Orchestrator{store: (*tailCaseInput.st), index: (*tailCaseInput.index)}
	assert.Diff(t, outputByParent(o.collectManifests()), want)
}

type testCollectManifestsArtifactGeneratorsCaseTailInputs struct {
	testCaseInput **struct {
		name    string
		emitted bool
		orphan  bool
		rival   bool
		skip    bool
	}
	st            **store.Store
	outputs       *map[string][]map[string]any
	shared        *manifest.NamedResource
	byteWinner    **store.SourceArtifact
	parent        **manifest.Kustomization
	generatorDocs *[]map[string]any
	nested        *manifest.NamedResource
	nestedDocs    *[]map[string]any
	index         **manifest.DiscoveryIndex
}
