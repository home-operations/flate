package discovery

import (
	"maps"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntryQualification_NativePrerequisites(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
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
	d := newDiscoverer(new(testConfig(t, Config{Input: input, Path: filepath.Join(root, "entry"), Store: st})))
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
		name                                    string
		kind                                    string
		optional, generated, accepted, rendered bool
		readable                                bool
		allowMissing, eligible                  bool
	}{
		{name: "required selected ConfigMap", kind: manifest.KindConfigMap, allowMissing: true},
		{name: "optional ConfigMap", kind: manifest.KindConfigMap, optional: true, eligible: true},
		{name: "accepted generated Secret", kind: manifest.KindSecret, generated: true, accepted: true},
		{name: "unaccepted rendered generated Secret", kind: manifest.KindSecret, generated: true, rendered: true},
		{name: "accepted rendered generated Secret", kind: manifest.KindSecret, generated: true, accepted: true, rendered: true, eligible: true},
		{name: "accepted readable generated Secret", kind: manifest.KindSecret, generated: true, accepted: true, readable: true, eligible: true},
		{name: "unaccepted generated Secret", kind: manifest.KindSecret, generated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := &manifest.Kustomization{
				Name: "consumer", Namespace: "apps",
				PostBuild: &kustomizev1.PostBuild{
					SubstituteFrom: []kustomizev1.SubstituteReference{
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
				children := []store.Child{{Obj: producer}}
				if tc.readable {
					children = append(
						children,
						store.Child{
							Obj: &manifest.Secret{
								Name:       "settings",
								Namespace:  "apps",
								StringData: map[string]any{"VALUE": "readable"},
							},
						},
					)
				}
				var receipt *manifest.RenderReceipt
				if tc.rendered {
					receipt = manifest.NewRenderReceipt("rendered", func(a, b any) bool { return a == b })
				}
				objects.RecordEmitted(candidate.Named(), &store.Emission{
					Children: children, Complete: tc.accepted, Render: receipt,
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
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
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
	assert.Diff(
		t,
		l.Index.ProducerKeys,
		map[manifest.NamedResource]manifest.NamedResource{firstMatch.Named(): binding.Key},
	)
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
	borrowed := l.Index
	borrowedReaders := maps.Clone(borrowed.Readers)
	borrowedRecords := maps.Clone(borrowed.Records)
	binding = acceptEntryForTest(t, a, &binding)
	assert.Equal(t, binding.Producer, laterMatch.Named())
	assert.Equal(t, borrowed == l.Index, false)
	assert.Diff(
		t,
		borrowed.ProducerKeys,
		map[manifest.NamedResource]manifest.NamedResource{firstMatch.Named(): binding.Key},
	)
	assert.Diff(
		t,
		l.Index.ProducerKeys,
		map[manifest.NamedResource]manifest.NamedResource{laterMatch.Named(): binding.Key},
	)
	assert.Diff(t, borrowed.Readers, borrowedReaders)
	assert.Diff(t, borrowed.Records, borrowedRecords)
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

func TestEntryQualification_CoverageRequiresExactRead(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, indexed string
		covered       bool
	}{
		{name: "read by entry", indexed: "entry/member.yaml", covered: true},
		{name: "same identity in sibling", indexed: "sibling/member.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte("resources: [member.yaml]\n")},
				"entry/member.yaml": {
					Data: []byte(testutil.KustomizationYAML("member", "apps", "  path: ./app\n")),
				},
				"sibling/member.yaml": {
					Data: []byte(testutil.KustomizationYAML("member", "apps", "  path: ./other\n")),
				},
			}}
			st := store.New()
			owner := &manifest.Kustomization{Name: "entry", Namespace: "apps", Path: "./entry"}
			member := &manifest.Kustomization{Name: "member", Namespace: "apps", Path: "./app"}
			st.AddObject(owner)
			st.RecordEmitted(owner.Named(), &store.Emission{
				Complete: true, Children: []store.Child{{Obj: member}},
			}, nil)
			l := loader.New(st)
			l.SourceRoot = root
			l.Index.Records[member.Named()] = manifest.DiscoveryRecord{Path: filepath.Join(root, tc.indexed)}
			q := &entryQualification{
				changedOnly: true, loader: l, session: &Session{},
				active: []manifest.NamedResource{member.Named()},
				matches: []applicationEvidence{{
					object: owner, visibleGeneration: true, source: entryResolution{artifact: input},
					application: manifest.EntryApplication{Path: owner.Path},
				}},
			}
			var reads store.ReadSet
			reads.Init(st, manifest.BootstrapSourceID, nil)
			t.Cleanup(reads.Close)
			walk := qualificationWalk{q: q, admission: &Admission{input: input}, store: st, reads: &reads}
			_, err := walk.visitActiveOperands(t.Context())
			assert.NoError(t, err)
			if tc.covered {
				assert.Equal(t, q.pending, false)
				assert.Equal(t, len(q.candidates), 0)
				return
			}
			assert.Equal(t, q.pending, true)
			assert.Diff(t, q.candidates, []manifest.NamedResource{member.Named()})
		})
	}
}

func TestEntryQualification_ChangedOnlyPreparesOrderedCandidates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"hint/kustomization.yaml": {Data: []byte("resources: [producer.yaml]\n")},
		"hint/producer.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: producer, namespace: apps}
spec:
  interval: 1m
  chartRef: {kind: OCIRepository, name: bootstrap}
  values: {instance: {apiVersion: fluxcd.controlplane.io/v1, kind: FluxInstance}}
`)},
		"later/kustomization.yaml": {Data: []byte("resources: []\n")},
	}}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	first := &manifest.Kustomization{Name: "first", Namespace: "apps", Path: "./hint"}
	later := &manifest.Kustomization{Name: "later", Namespace: "apps", Path: "./later"}
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	prepared := entryInputs{packagesToQualify: [][]selection.OccurrenceFacts{{
		{Identity: later.Named(), Object: later}, {Identity: first.Named(), Object: first},
	}}}
	t.Run("single operand", func(t *testing.T) {
		t.Parallel()
		singleStore := store.New()
		singleLoader := loader.New(singleStore)
		singleLoader.SourceRoot = root
		var reads store.ReadSet
		reads.Init(singleStore, manifest.BootstrapSourceID, nil)
		t.Cleanup(reads.Close)
		var producerReads atomic.Int64
		trackedInput := input.WithSourceView(entryReadFS{
			FS: input.FS, reads: map[string]*atomic.Int64{"hint/producer.yaml": &producerReads},
		})
		q := &entryQualification{
			loader: singleLoader, changedOnly: true,
			packages: [][]selection.OccurrenceFacts{prepared.packagesToQualify[0][1:]},
		}
		walk := qualificationWalk{q: q, admission: &Admission{input: trackedInput}, store: singleStore, reads: &reads}
		assert.NoError(t, walk.advanceOperands(t.Context()))
		assert.Diff(t, q.candidates, []manifest.NamedResource{first.Named()})
		assert.Equal(t, producerReads.Load(), 0)
	})

	hint, err := l.EntryProducerHint(t.Context(), input, first.Path)
	assert.NoError(t, err)
	assert.Equal(t, hint, true)
	q := &entryQualification{loader: l, changedOnly: true, packages: prepared.packagesToQualify}
	a := &Admission{store: st, input: input, path: filepath.Join(root, "entry"), entryDemand: true, qualification: q}
	qualifyForTest(t, l, first.Named(), later.Named())
	candidate, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, candidate == nil, true)
	assert.Equal(t, binding, Binding{})
	assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{first.Named()})

	st.UpdateStatus(first.Named(), store.StatusInfo{Status: store.StatusFailed})
	q.session = &Session{finalized: true}
	candidate, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, candidate == nil, true)
	assert.Equal(t, binding, Binding{})
	assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{later.Named()})

	match := &manifest.Kustomization{Name: "match", Namespace: "apps", Path: "./entry"}
	st.RecordEmitted(later.Named(), &store.Emission{Children: []store.Child{{Obj: match}}, Complete: true}, nil)
	q.session = nil
	candidate, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, candidate == nil, "entry candidate missing after the first operand failed")
	assert.Equal(t, candidate.Named(), match.Named())
	assert.Equal(t, acceptEntryForTest(t, a, &binding).Producer, match.Named())
}
