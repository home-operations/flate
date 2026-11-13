package discovery

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestEntryQualification_NativePrerequisites(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [owner.yaml, sibling.yaml]\n")},
		"entry/owner.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"owner",
					"apps",
					"  path: ./entry\n  dependsOn: [{name: sibling, namespace: '${TEAM}'}]\n",
				),
			),
		},
		"entry/sibling.yaml": {
			Data: []byte(testutil.KustomizationYAML("sibling", "${TEAM}", "  path: ./sibling\n")),
		},
		"sibling/kustomization.yaml": {Data: []byte("resources: []\n")},
	}}
	st := store.New()
	d := newDiscoverer(&Config{Input: input, Path: filepath.Join(root, "entry"), Store: st, Trees: testTrees(t)})
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	assert.NoError(t, d.loadManifestsTest(t.Context(), root, true))
	assert.Equal(t, d.binding.Producer.ReportHidden(), true)
	owner := d.loader.CatalogObject(testutil.KustomizationID("apps", "owner"))
	assert.FatalIf(t, owner == nil, "owner construction declaration missing")
	for ref := range manifest.References(owner, manifest.ReferenceRelations) {
		if ref.Use == "dependsOn" {
			assert.Equal(t, ref.NamedResource, testutil.KustomizationID("${TEAM}", "sibling"))
		}
	}
	assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent), nil)
	for _, tc := range []struct {
		name                          string
		kind                          string
		optional, generated, accepted bool
		allowMissing, eligible        bool
	}{
		{name: "required selected ConfigMap", kind: manifest.KindConfigMap, allowMissing: true},
		{name: "optional ConfigMap", kind: manifest.KindConfigMap, optional: true, eligible: true},
		{name: "accepted generated Secret", kind: manifest.KindSecret, generated: true, accepted: true, eligible: true},
		{name: "unaccepted generated Secret", kind: manifest.KindSecret, generated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := &manifest.Kustomization{
				Name: "consumer", Namespace: "apps",
				PostBuild: &kustomizev1.PostBuild{
					SubstituteFrom: []manifest.SubstituteReference{
						{Kind: tc.kind, Name: "settings", Optional: tc.optional},
					},
				},
			}
			objects := store.New()
			objects.AddObject(candidate)
			if tc.generated {
				producer := &manifest.RawObject{
					APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Namespace: "apps", Name: "settings",
				}
				objects.RecordEmitted(candidate.Named(), &store.Emission{
					Children: []store.Child{{Obj: producer}}, Complete: tc.accepted,
				}, nil)
			}
			l := loader.New(objects)
			if tc.allowMissing {
				l.Options.Missing = depwait.NewMissingPolicy([]string{tc.kind})
			}
			var reads store.ReadSet
			reads.Init(objects, candidate.Named(), nil)
			defer reads.Close()
			eligibility := entryPrerequisites{loader: l, reads: &reads}
			assert.Equal(t, eligibility.eligible(candidate, nil), tc.eligible)
		})
	}
}

func TestEntryQualification_OrderedPrefixAndReplacement(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	first := &manifest.ResourceSet{Name: "first", Namespace: "apps"}
	later := &manifest.ResourceSet{Name: "later", Namespace: "apps"}
	st.AddObject(first)
	st.AddObject(later)
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	qualifyForTest(t, l, first.Named(), later.Named())
	a := &Admission{
		store:         st,
		input:         input,
		path:          filepath.Join(root, "entry"),
		qualification: &entryQualification{loader: l},
	}
	laterMatch := &manifest.Kustomization{Name: "a", Namespace: "apps"}
	laterMatch.Path = "./entry"
	st.RecordEmitted(later.Named(), &store.Emission{Children: []store.Child{{Obj: laterMatch}}, Complete: true}, nil)
	a.qualification.previous = kustomize.EntryEvidence{
		Owner:   later.Named(),
		Root:    later.Named(),
		Receipt: manifest.NewRenderReceipt("previous", func(a, b any) bool { return a == b }),
	}
	a.qualification.refresh = []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "later-input"},
	}
	candidate, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, candidate == nil, true)
	assert.Equal(t, binding, Binding{})
	assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{first.Named()})
	firstMatch := &manifest.Kustomization{Name: "z", Namespace: "apps"}
	firstMatch.Path = "./entry"
	st.RecordEmitted(first.Named(), &store.Emission{Children: []store.Child{{Obj: firstMatch}}, Complete: true}, nil)
	candidate, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, candidate == nil, "first authoritative application did not bind")
	assert.Equal(t, candidate.Named(), firstMatch.Named())
	binding = acceptEntryForTest(t, a, &binding)
	assert.Equal(t, binding.Producer, firstMatch.Named())
	assert.Equal(t, len(a.EntryProducers()), 0)
	assert.Equal(t, len(st.Warnings()), 0)
	unchanged, proposal, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, unchanged == nil, true)
	assert.Equal(t, proposal, Binding{})
	replacement := laterMatch.Clone()
	replacement.TargetNamespace = "next"
	st.RecordEmitted(later.Named(), &store.Emission{Children: []store.Child{{Obj: replacement}}, Complete: true}, nil)
	candidate, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, candidate == nil, "ordinary publication did not replace the binding")
	assert.Equal(t, candidate.Named(), laterMatch.Named())
	assert.Equal(t, len(st.Warnings()), 0)
	binding = acceptEntryForTest(t, a, &binding)
	assert.Equal(t, binding.Producer, laterMatch.Named())
	warnings := st.Warnings()
	assert.FatalIf(t, len(warnings) != 1, "conflicts=%+v", warnings)
	assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
	assert.Equal(t, warnings[0].Count, 1)
	st.RecordEmitted(later.Named(), &store.Emission{Children: []store.Child{}, Complete: true}, nil)
	assert.Equal(t, len(st.Warnings()), 0)
	candidate, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, candidate == nil, "complete withdrawal lost the remaining application")
	assert.Equal(t, candidate.Named(), firstMatch.Named())
	binding = acceptEntryForTest(t, a, &binding)
	assert.Equal(t, binding.Producer, firstMatch.Named())
	assert.Equal(t, len(st.Warnings()), 0)
}

func acceptEntryForTest(t *testing.T, admission *Admission, binding *Binding) Binding {
	t.Helper()
	accepted, err := admission.AcceptEntry(t.Context(), *binding)
	assert.NoError(t, err)
	return accepted
}

func TestEntryQualification_PreparationReuse(t *testing.T) {
	t.Parallel()
	fixture := newPreparedConstructionFixture(t, false)
	var recorder manifest.WorkRecorder
	fixture.cfg.Work = recorder.Scope()
	fixture.cfg.Store.SetWorkTree(manifest.TreeID{}, fixture.cfg.Work)
	fixture.cfg.Loader = loader.New(fixture.cfg.Store)
	configureSelection(
		t,
		fixture.cfg.Loader,
		selection.Request{
			Roots: []manifest.NamedResource{
				{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps"},
			},
		},
	)
	d := newDiscoverer(&fixture.cfg)
	t.Cleanup(d.Close)
	assert.NoError(t, d.loadManifestsTest(t.Context(), fixture.cfg.Input.LocalPath, false))
	q := d.admission().qualification
	commit := func() {
		var reads store.ReadSet
		reads.Init(fixture.cfg.Store, manifest.BootstrapSourceID, nil)
		reads.RequireQualification(fixture.parent.Named())
		walk := qualificationWalk{q: q, admission: d.admission(), store: fixture.cfg.Store, reads: &reads}
		committed, err := walk.commit(t.Context(), &selection.PrepareRequest{Invocation: true, Reads: &reads})
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
		reads.Retire()
		reads.ReleaseCapture()
	}
	commit()
	commit()
	assert.Equal(t, recorder.Scope().Admission().Counts.OwnerPrepares, uint64(1))
	replacement := *fixture.cfg.Input
	replacement.Content = "replacement"
	fixture.cfg.Store.SetArtifact(fixture.parent.SourceID(), &replacement)
	commit()
	assert.Equal(t, recorder.Scope().Admission().Counts.OwnerPrepares, uint64(2))
}
