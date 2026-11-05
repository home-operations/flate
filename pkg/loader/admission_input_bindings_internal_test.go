package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_InputGate(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: consumer, namespace: apps}\nspec: {dependsOn: [{apiVersion: kustomize.toolkit.fluxcd.io/v1, kind: Kustomization, name: gate}]}\n",
			),
		},
		"gate.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: gate, namespace: apps}\nspec: {path: ./gate}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{
		Roots: []manifest.NamedResource{(&manifest.ResourceSet{Name: "consumer", Namespace: "apps"}).Named()},
	}
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	gate := (&manifest.Kustomization{Name: "gate", Namespace: "apps"}).Named()
	assert.Equal(t, slices.Contains(proposal.Needed, gate), true)
	assert.Equal(t, slices.Contains(proposal.Selected, gate), false)
}

func TestAdmission_ChangedInputRequiresReaderHistory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {replicas: '2'}\n",
			),
		},
		"producer.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: templates, namespace: apps}\nspec:\n  resourcesTemplate: |\n    apiVersion: helm.toolkit.fluxcd.io/v2\n    kind: HelmRelease\n    metadata: {name: child}\n    spec: {interval: 1m, valuesFrom: [{kind: ConfigMap, name: values}]}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"input.yaml"})
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	producer := (&manifest.ResourceSet{Name: "templates", Namespace: "apps"}).Named()
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
	l.Options.HistoryOwners = []manifest.NamedResource{producer}
	proposal, err = l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), true)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
}

type admissionArtifactReads struct {
	admissionReader

	calls    int
	lists    int
	bySource map[manifest.NamedResource]int
}

//nolint:ireturn // The reader contract returns heterogeneous artifact implementations.
func (r *admissionArtifactReads) GetArtifact(id manifest.NamedResource, read store.ArtifactRead) store.Artifact {
	r.calls++
	if r.bySource != nil {
		r.bySource[id]++
	}
	return r.admissionReader.GetArtifact(id, read)
}

func (r *admissionArtifactReads) ListObjects(group, kind string) []manifest.BaseManifest {
	r.lists++
	return r.admissionReader.ListObjects(group, kind)
}

func TestAdmission_InputChangesQualifiedOnce(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"selected.yaml", "unrelated.yaml"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Changes = change.NewSet([]string{changed})
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			inputs := make([]string, 100)
			for i := range inputs {
				inputs[i] = "other-" + strings.Repeat("x", i) + ".yaml"
			}
			inputs[len(inputs)-1] = "selected.yaml"
			l.Index.Records[id] = manifest.DiscoveryRecord{File: "settings.yaml", Inputs: inputs}
			reads := &admissionArtifactReads{admissionReader: st}
			c := admissionClosure{loader: l, objects: reads}
			assert.Equal(t, c.affected(id, nil), changed == "selected.yaml")
			if reads.calls > 3 {
				t.Fatalf(
					"qualified one changed path %d times for %d recorded inputs; want at most 3 artifact reads",
					reads.calls,
					len(inputs),
				)
			}
		})
	}
}

func TestAdmission_SourceBindingsCaptureOncePerEvaluation(t *testing.T) {
	t.Parallel()
	_, stores, roots, source := optionalSupplierPair(t, "apps")
	sources := []manifest.NamedResource{source.Named()}
	for i := 1; i < 3; i++ {
		supplier := *source
		supplier.Name = fmt.Sprintf("supplier-%d", i)
		sources = append(sources, supplier.Named())
		for _, st := range stores {
			st.AddObject(&supplier)
			st.SetArtifact(
				supplier.Named(),
				st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact),
			)
		}
	}
	changes, err := change.Qualify(t.Context(), nil, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	var observations [2]manifest.DiscoveryIndex
	for _, source := range sources {
		visible := manifest.InputVisibility{
			Source:   source,
			Root:     "inputs",
			Complete: true,
			Inputs: map[manifest.NamedResource][]string{
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}: {"inputs/optional.yaml"},
			},
		}
		empty := visible
		empty.Inputs = nil
		observations[0].Visibility = append(observations[0].Visibility, visible)
		observations[1].Visibility = append(observations[1].Visibility, empty)
	}
	changes, err = changes.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			&observations[0],
			&observations[1],
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		Path:      "apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "unrelated", Optional: true},
			{Kind: manifest.KindConfigMap, Name: "optional", Optional: true},
		}},
	}
	l := New(stores[1])
	l.SourceRoot = roots[1]
	l.Options.Changes = changes
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Filter = change.NewFilter()
	for _, replaced := range []bool{false, true} {
		if replaced {
			for _, id := range sources {
				input := *stores[1].GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
				input.Key, input.Content = "replacement", "replacement"
				stores[1].SetArtifact(id, &input)
			}
		}
		var captured store.ReadSet
		captured.Init(stores[1], consumer.Named(), nil)
		reads := &admissionArtifactReads{admissionReader: &captured, bySource: map[manifest.NamedResource]int{}}
		c := admissionClosure{loader: l, objects: reads, incoming: store.Child{Obj: consumer}}
		c.proposal = &c.result
		proposal, err := c.evaluate(t.Context(), new(PrepareRequest{Invocation: true}))
		assert.NoError(t, err)
		assert.Equal(t, slices.Contains(proposal.Selected, consumer.Named()), !replaced)
		if len(reads.bySource) < len(sources) {
			t.Fatalf("captured %d source identities for %d suppliers", len(reads.bySource), len(sources))
		}
		for id, count := range reads.bySource {
			assert.Equal(t, count, 1)
			assert.Equal(t, slices.Contains(slices.Collect(captured.Inputs()), id), true)
		}
		assert.Equal(t, captured.Valid(), true)
		input := *stores[1].GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
		input.Key, input.Content = "invalidated", "invalidated"
		stores[1].SetArtifact(source.Named(), &input)
		assert.Equal(t, captured.Valid(), false)
		captured.Close()
	}
}

func TestAdmission_FrozenBindingsCaptureEachConsumer(t *testing.T) {
	t.Parallel()
	changes, stores, roots, source := optionalSupplierPair(t, "apps")
	changes, err := change.Qualify(t.Context(), changes, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	l := New(stores[1])
	l.SourceRoot = roots[1]
	l.Options.Changes = changes
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Filter = change.NewFilter()
	l.sideKnown, l.side = true, manifest.TreeHead
	l.freezeConstruction()
	input := stores[1].GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	var captured [2]store.ReadSet
	closers := make([]func(), 0, len(captured))
	defer func() {
		for _, closeResource := range slices.Backward(closers) {
			closeResource()
		}
	}()
	for i := range captured {
		consumer := &manifest.Kustomization{
			Name:      fmt.Sprintf("consumer-%d", i),
			Namespace: "apps",
			Path:      "apps",
			PostBuild: &kustomizev1.PostBuild{
				SubstituteFrom: []manifest.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "optional", Optional: true},
				},
			},
		}
		captured[i].Init(stores[1], consumer.Named(), nil)
		closers = append(closers, captured[i].Close)
		reads := &admissionArtifactReads{admissionReader: &captured[i], bySource: map[manifest.NamedResource]int{}}
		c := admissionClosure{loader: l, objects: reads, incoming: store.Child{Obj: consumer}}
		c.proposal = &c.result
		proposal, err := c.evaluate(t.Context(), new(PrepareRequest{Invocation: true}))
		assert.NoError(t, err)
		assert.Equal(t, slices.Contains(proposal.Selected, consumer.Named()), true)
		assert.Equal(t, reads.bySource[source.Named()], 1)
		assert.Equal(t, slices.Contains(slices.Collect(captured[i].Inputs()), source.Named()), true)
		assert.Equal(t, captured[i].Valid(), true)
	}
	replacement := *input
	replacement.Key, replacement.Content = "replacement", "replacement"
	stores[1].SetArtifact(source.Named(), &replacement)
	for i := range captured {
		assert.Equal(t, captured[i].Valid(), false)
	}
	c := admissionClosure{
		loader:      l,
		objects:     stores[1],
		comparisons: &admissionSources{bindings: map[manifest.NamedResource]*admissionSource{}},
	}
	evidence := c.boundSource(source.Named())
	assert.Equal(t, evidence.input, &replacement)
	assert.Equal(t, evidence.paired, false)
}

func TestAdmission_SourceBindingsTrackComparison(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	source := (&manifest.GitRepository{Name: "local", Namespace: "apps"}).Named()
	st.SetArtifact(source, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"before.yaml"})
	c := admissionClosure{loader: l, objects: st}
	c.proposal = &c.result
	before, _, _ := c.delta(source)
	assert.Equal(t, before.Contains("before.yaml"), true)
	_, err := c.evaluate(t.Context(), new(PrepareRequest{Invocation: true}))
	assert.NoError(t, err)
	l.Options.Changes = change.NewSet([]string{"after.yaml"})
	next := admissionClosure{
		loader:      l,
		objects:     st,
		comparisons: &admissionSources{bindings: map[manifest.NamedResource]*admissionSource{}},
	}
	after, _, _ := next.delta(source)
	assert.Equal(t, after.Contains("before.yaml"), false)
	assert.Equal(t, after.Contains("after.yaml"), true)
}

func TestAdmission_UnreferencedInputCollision(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"component_collision", "unreferenced_file"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testAdmissionUnreferencedInputCollisionCase(
				t,
				&name,
			)
		},
		)
	}
}

func testAdmissionUnreferencedInputCollisionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	var stores [2]*store.Store
	var loaders [2]*Loader
	for side := range stores {
		root := t.TempDir()
		files := fstest.MapFS{
			"flux/parent.yaml": {
				Data: []byte(testutil.KustomizationYAML("parent", "flux-system", "  path: ./decls\n")),
			},
			"decls/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
			"decls/child.yaml": {
				Data: []byte(testutil.KustomizationYAML("child", "flux-system", "  path: ./app\n")),
			},
			"app/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
			"app/cm.yaml": {
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: original}\n",
				),
			},
		}
		if side == 1 {
			if (*testCaseInput) == "component_collision" {
				files["unused/kustomization.yaml"] = &fstest.MapFile{
					Data: []byte(
						"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [cm.yaml]\n",
					),
				}
				files["unused/cm.yaml"] = files["app/cm.yaml"]
			} else {
				files["app/unreferenced.yaml"] = &fstest.MapFile{
					Data: []byte(
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unreferenced, namespace: apps}\n",
					),
				}
			}
		}
		st := store.New()
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files},
		)
		l := New(st)
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
		for _, dir := range []string{"flux", "decls", "app"} {
			_, err := l.Load(t.Context(), filepath.Join(root, dir), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
		}
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
		stores[side], loaders[side] = st, l
	}
	changes, err := change.Qualify(t.Context(), nil, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	for side, l := range loaders {
		l.Options.Changes = changes
		proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
		assert.NoError(t, err)
		if len(proposal.Needed) != 0 {
			t.Errorf("side %d admitted unrelated objects: %v", side, proposal.Needed)
		}
	}
}
