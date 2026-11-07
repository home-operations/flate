package loader

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
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
		"selected/payload.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: changed}\n"),
		},
		"sibling/kustomization.yaml": {Data: []byte("resources: []\n")},
		"shared/kustomization.yaml": {
			Data: []byte("apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [external.yaml]\n"),
		},
		"shared/external.yaml": {
			Data: []byte(
				"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: credentials}\nspec: {target: {name: credentials}, dataFrom: [{extract: {key: credentials}}]}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root

	l.Options.Changes = change.NewSet([]string{"selected/payload.yaml"})
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	selected := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "flux-system",
		Name:      "selected",
	}
	sibling := selected
	sibling.Name = "sibling"
	assert.Equal(t, slices.Contains(proposal.Selected, selected), true)
	assert.Equal(t, slices.Contains(proposal.Needed, sibling), false)
}

func TestAdmission_MissingInputWaitsForKnownOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                        string
		complete, optional, generated, allowMissing bool
		production                                  string
	}{{name: "pending"}, {name: "complete", complete: true}, {name: "optional", complete: true, optional: true}, {name: "generated", complete: true, generated: true}, {name: "allowed", complete: true, allowMissing: true}, {name: "self", complete: true, production: "self"}, {name: "peer", complete: true, production: "peer"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmissionMissingInputWaitsForKnownOwnerCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestAdmission_DeclaredChartSourceNamespace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"flux/kustomization.yaml": {Data: []byte("namespace: apps\nresources: [owner.yaml]\n")},
		"flux/owner.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner}\nspec: {path: ./app, targetNamespace: apps}\n",
			),
		},
		"app/kustomization.yaml": {Data: []byte("resources: [source.yaml, release.yaml]\n")},
		"app/source.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart}\nspec: {url: oci://fixture.invalid/chart}\n",
			),
		},
		"app/release.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer}\nspec: {chartRef: {kind: OCIRepository, name: chart}}\n",
			),
		},
	}})
	opaque := &manifest.HelmRelease{Name: "opaque", Namespace: "apps"}
	st.AddObject(opaque)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})

	l.Options.Changes = change.NewSet([]string{"app/release.yaml"})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "chart",
	}
	record, known := l.Index.Lookup(id)
	if !known {
		t.Fatalf("declared chart source is missing: index=%+v", l.Index.Records)
	}
	assert.Equal(t, record.Authored.Namespace, "")
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, id), true)
	assert.Equal(t, slices.Contains(proposal.Needed, opaque.Named()), false)
}

func testAdmissionMissingInputWaitsForKnownOwnerCase(
	t *testing.T,
	testCaseInput *struct {
		name         string
		complete     bool
		optional     bool
		generated    bool
		allowMissing bool
		production   string
	},
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	kind := manifest.KindConfigMap
	if testCaseInput.generated {
		kind = manifest.KindSecret
	}
	consumer := &manifest.HelmRelease{
		Name:       "consumer",
		Namespace:  "apps",
		ValuesFrom: []manifest.ValuesReference{{Kind: kind, Name: "missing", Optional: testCaseInput.optional}},
	}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./owned"}
	opaque := &manifest.HelmRelease{Name: "opaque", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{consumer, owner, opaque})
	if testCaseInput.complete {
		output := &store.Emission{Complete: true}
		if testCaseInput.generated {
			output.Children = []store.Child{
				{
					Obj: &manifest.RawObject{
						APIVersion: "external-secrets.io/v1",
						Kind:       "ExternalSecret",
						Name:       "missing",
						Namespace:  "apps",
					},
				},
			}
		}
		st.RecordEmitted(owner.Named(), output, nil)
		st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	l := New(st)
	l.SourceRoot = root

	if testCaseInput.allowMissing {
		l.Options.AllowMissingKinds = []string{kind}
	}
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{consumer.Named()}})
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {
			Parent:      owner.Named(),
			Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}},
		},
	}}
	if testCaseInput.production != "" {
		producer := owner.Named()
		if testCaseInput.production == "self" {
			producer = consumer.Named()
		}
		bare := manifest.NamedResource{Kind: kind, Name: "missing"}
		l.Index.Records[bare] = manifest.DiscoveryRecord{Productions: []manifest.Production{{
			Owner:  producer,
			Source: manifest.BootstrapSourceID, Authored: bare, File: "missing.yaml", Path: "missing.yaml",
		}}}
	}
	occurrence := compactOccurrence(opaque, projectedProducerForTest(opaque))
	occurrence.Declaration.Source = root
	l.locations = map[manifest.NamedResource]authoredOccurrence{opaque.Named(): occurrence}
	l.Index.Records[opaque.Named()] = manifest.DiscoveryRecord{}
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
	assert.Equal(
		t,
		slices.Contains(
			proposal.Needed,
			opaque.Named(),
		),
		testCaseInput.complete && !testCaseInput.optional && !testCaseInput.generated && !testCaseInput.allowMissing &&
			testCaseInput.production != "self",
	)
	assert.Equal(t, slices.Contains(proposal.Selected, opaque.Named()), false)
	plain, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{{Obj: consumer}}},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(plain.Needed, owner.Named()), true)
	wantOpaque := testCaseInput.complete && !testCaseInput.optional && !testCaseInput.generated &&
		!testCaseInput.allowMissing
	assert.Equal(t, slices.Contains(plain.Needed, opaque.Named()), wantOpaque && testCaseInput.production != "self")
	assertAdmissionCapturedMembership(t, l, consumer, plain)
}

func assertAdmissionCapturedMembership(
	t *testing.T,
	l *Loader,
	consumer manifest.BaseManifest,
	plain *selection.Admission,
) {
	t.Helper()
	var reads store.ReadSet
	owner := l.Index.Records[consumer.Named()].Parent
	reads.Init(l.Store, owner, nil)
	defer reads.Close()
	for range 2 {
		captured, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
			Changes:    l.Options.Changes,
			Generation: []store.Child{{Obj: consumer}},
			Reads:      &reads,
		})
		assert.NoError(t, err)
		assert.Diff(t, captured.Needed, plain.Needed)
		assert.Diff(t, captured.Selected, plain.Selected)
		assert.Diff(t, captured.Dirty, plain.Dirty)
	}
	assert.Equal(t, reads.ID(), owner)
}
