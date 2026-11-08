package loader

import (
	"fmt"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_GeneratedInputRetainsUnchangedConsumers(t *testing.T) {
	t.Parallel()
	for _, file := range []string{"apps/changed.yaml", "apps/kustomization.yaml", "apps/vars.yaml"} {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"parent.yaml": {
					Data: []byte(
						"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: parent, namespace: apps}\nspec:\n  path: ./apps\n  postBuild: {substituteFrom: [{kind: Secret, name: vars}]}\n",
					),
				},
				"apps/kustomization.yaml": {Data: []byte("resources: [changed.yaml, stable.yaml, vars.yaml]\n")},
				"apps/changed.yaml": {
					Data: []byte(
						"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: changed, namespace: apps}\nspec: {path: ./payload}\n",
					),
				},
				"apps/stable.yaml": {
					Data: []byte(
						"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: stable, namespace: apps}\nspec:\n  path: ./payload\n  postBuild: {substituteFrom: [{kind: Secret, name: vars}]}\n",
					),
				},
				"apps/vars.yaml": {
					Data: []byte(
						"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: declaration, namespace: apps}\nspec:\n  target: {name: vars}\n  data: [{secretKey: VALUE, remoteRef: {key: fixture}}]\n",
					),
				},
				"payload/kustomization.yaml": {Data: []byte("resources: []\n")},
			}})
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			l.Options.Changes = change.NewSet([]string{file})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			proposal, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
			assert.NoError(t, err)
			_, committed, err := l.commitSelection(t.Context(), t, proposal, selection.Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			parent := manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Name:      "parent",
				Namespace: "apps",
			}
			input := manifest.NamedResource{Kind: manifest.KindSecret, Name: "vars", Namespace: "apps"}
			declaration := manifest.NamedResource{
				Group:     "external-secrets.io",
				Kind:      "ExternalSecret",
				Name:      "declaration",
				Namespace: "apps",
			}
			proposal, err = prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{{
					Obj: manifest.NewSecret(input, nil, map[string]any{"VALUE": "current"}),
					Productions: []manifest.Production{
						{
							Owner:       parent,
							Declaration: declaration,
							Authored:    declaration,
							Source:      manifest.BootstrapSourceID,
							File:        "apps/vars.yaml",
						},
					},
				}}},
			)
			assert.NoError(t, err)
			want := file != "apps/changed.yaml"
			assert.Equal(t, slices.Contains(proposal.Dirty, input), want)
			stable := parent
			stable.Name = "stable"
			assert.Equal(t, slices.Contains(proposal.Selected, stable), want)
			assert.Equal(t, slices.Contains(proposal.Dirty, stable), want)
		})
	}
}

func TestAdmission_ClosedRootFrontier(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"published", "unpublished", "incoming"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testAdmissionClosedRootFrontierCase(t, &mode)
		},
		)
	}
}

func TestAdmission_KnownInputDoesNotOpenUnrelatedRenderer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: changed}\n",
			),
		},
		"unrelated.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated, namespace: apps}\nspec:\n  chartRef: {kind: OCIRepository, name: chart}\n  interval: 1m\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root

	l.Options.Changes = change.NewSet([]string{"input.yaml"})
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	renderer := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Name:      "unrelated",
		Namespace: "apps",
	}
	assert.Equal(t, slices.Contains(proposal.Selected, input), true)
	assert.Equal(t, slices.Contains(proposal.Needed, renderer), false)
}

func TestAdmission_UnclaimedInputDoesNotOpenUnrelatedRenderer(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"unowned only", "supplied dependency", "repository collision"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testAdmissionUnclaimedInput(t, scenario)
		})
	}
}

func testAdmissionUnclaimedInput(t *testing.T, scenario string) {
	t.Helper()
	root := t.TempDir()
	st := store.New()
	files := fstest.MapFS{
		"input.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: changed}\n",
			),
		},
		"unrelated.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated, namespace: apps}\nspec:\n  chartRef: {kind: OCIRepository, name: chart}\n  interval: 1m\n",
			),
		},
		"templated.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: templated, namespace: apps}\nspec: {path: './payload/${APP}'}\n",
			),
		},
		"producer.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: producer, namespace: apps}\nspec: {resourcesTemplate: unexecuted}\n",
			),
		},
	}
	dependency := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "external"}
	if scenario == "repository collision" {
		dependency.Name = "settings"
	}
	if scenario != "unowned only" {
		files["consumer.yaml"] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(
			"consumer",
			"apps",
			fmt.Sprintf(
				"  path: ./payload\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: %s}]\n",
				dependency.Name,
			),
		))}
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	if scenario != "unowned only" {
		l.AdmitDeclarations(
			[]store.Declaration{
				{
					Obj:    manifest.NewConfigMap(dependency, map[string]any{"VALUE": "supplied"}),
					Origin: manifest.SuppliedDeclaration,
				},
			},
		)
	}
	l.SourceRoot = root

	changed := []string{"unclaimed/notes.txt"}
	if scenario != "unowned only" {
		changed = append(changed, "consumer.yaml")
	}
	l.Options.Changes = change.NewSet(changed)
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	templated := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Name:      "templated",
		Namespace: "apps",
	}
	producer := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Name:      "producer",
		Namespace: "apps",
	}
	record := l.Index.Records[templated]
	record.Productions = []manifest.Production{{Owner: producer}}
	l.Index.Records[templated] = record
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	if scenario != "unowned only" {
		assertAdmissionSuppliedDependency(t, l, proposal, dependency)
	}
	assert.Equal(t, slices.Contains(proposal.Needed, templated), false)
	assert.Equal(t, slices.Contains(proposal.Selected, templated), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, templated), false)
	assertAdmissionUnrelatedMembership(t, l, proposal, producer)
}

func assertAdmissionUnrelatedMembership(
	t *testing.T,
	l *Loader,
	proposal *selection.Admission,
	producer manifest.NamedResource,
) {
	t.Helper()
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	renderer := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Name:      "unrelated",
		Namespace: "apps",
	}
	assert.Equal(t, slices.Contains(proposal.Selected, input), false)
	assert.Equal(t, slices.Contains(proposal.Needed, renderer), false)
	assert.Equal(t, slices.Contains(proposal.Selected, renderer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, renderer), false)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
	historyOwners := map[manifest.NamedResource][]manifest.SourceEvidence{
		producer: {
			{
				Source: l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).HistoryIdentity(
					manifest.BootstrapSourceID,
				),
				Path: "input.yaml",
			},
		},
	}
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{HistoryOwners: historyOwners, Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), true)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer), false)
}

func testAdmissionClosedRootFrontierCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"provider.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: provider, namespace: apps}\nspec:\n  path: ./provider\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
			),
		},
		"consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./consumer\n  dependsOn: [{name: provider}]\n",
			),
		},
		"settings.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: old}\n",
			),
		},
		"new-settings.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: new-settings, namespace: apps}\ndata: {VALUE: current}\n",
			),
		},
		"provider/kustomization.yaml": {Data: []byte("resources: []\n")},
		"consumer/kustomization.yaml": {Data: []byte("resources: []\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	provider := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Name:      "provider",
		Namespace: "apps",
	}
	consumer := provider
	consumer.Name = "consumer"
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	providerChild := l.IndexedObject(provider, l.Index.Records[provider])
	consumerChild := l.IndexedObject(consumer, l.Index.Records[consumer])
	assert.FatalIf(
		t,
		providerChild.Obj == nil || consumerChild.Obj == nil,
		"%s",
		"fixture declarations are unavailable",
	)
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{consumer}})

	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	l.roots.set(
		provider,
		&rootEvidence{
			identity: provider,
			scope:    selection.RootScopeFacts{NamespaceEligible: true, Candidate: true},
		},
	)
	st.AddObjects(
		[]manifest.BaseManifest{
			providerChild.Obj,
			consumerChild.Obj,
			manifest.NewConfigMap(settings, map[string]any{"VALUE": "old"}),
		},
	)
	fresh := settings
	fresh.Name = "new-settings"
	st.AddObject(manifest.NewConfigMap(fresh, map[string]any{"VALUE": "current"}))
	_, committed, err := requireSelection(
		t,
		l,
		[]manifest.NamedResource{provider, settings, manifest.BootstrapSourceID},
		false,
	)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	incoming := store.Child{Obj: st.GetObject(consumer)}
	switch *testCaseInput {
	case "unpublished":
		st.DeleteObject(provider)
	case "incoming":
		updated := st.GetObject(provider).(*manifest.Kustomization).Clone()
		updated.PostBuild.SubstituteFrom[0].Name = "new-settings"
		incoming.Obj = updated
	}
	var reads store.ReadSet
	reads.Init(st, consumer, nil)
	defer reads.Close()
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{incoming}, Reads: &reads},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, provider), (*testCaseInput) != "published")
	if (*testCaseInput) == "incoming" {
		settings.Name = "new-settings"
		assert.Equal(t, slices.Contains(proposal.Needed, settings), true)
	}
}

func assertAdmissionSuppliedDependency(
	t *testing.T,
	l *Loader,
	proposal *selection.Admission,
	dependency manifest.NamedResource,
) {
	t.Helper()
	consumer := testutil.KustomizationID("apps", "consumer")
	assert.Equal(t, slices.Contains(proposal.Selected, consumer), true)
	assert.Equal(t, slices.Contains(proposal.Needed, dependency), true)
	assert.Equal(t, slices.Contains(proposal.Selected, dependency), false)
	data := l.Store.ReferenceFacts(dependency, l.Index, manifest.NamedResource{}).Data
	if dependency.Name == "settings" {
		var hydrated bool
		for i := range proposal.Objects {
			child := &proposal.Objects[i]
			if child.Obj.Named() == dependency {
				hydrated = true
				l.Store.AddObject(child.Obj, child.Doc)
			}
		}
		assert.Equal(t, hydrated, true)
		data = l.Store.ReferenceFacts(dependency, l.Index, manifest.NamedResource{}).Data
	}
	valueResult, err := data.Value("VALUE")
	value, found := valueResult.Value, valueResult.Found
	assert.NoError(t, err)
	assert.Equal(t, found, true)
	want := "supplied"
	if dependency.Name == "settings" {
		want = "changed"
	}
	assert.Equal(t, value, want)
}
