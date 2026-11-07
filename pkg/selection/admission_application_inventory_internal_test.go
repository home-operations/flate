package selection

import (
	"encoding/json"
	"iter"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_MaterializerApplicationInventoryAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []applicationInventoryCase{
		{"unknown", "unknown", false, true, true},
		{"known_owner", "known_owner", false, true, false},
		{"bound_entry", "bound_entry", true, false, false},
		{"static_positive", "static", true, false, false},
		{"retired_static", "static", false, true, true},
		{"complete_positive", "emission", true, false, false},
		{"complete_negative", "emission", false, false, true},
		{"partial_positive", "emission", false, true, true},
		{"stale", "emission", false, true, true},
		{"history_positive", "history", true, false, false},
		{"history_negative", "history", false, false, true},
		{"history_partial", "history", false, true, true},
		{"foreign", "emission", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testApplicationInventoryCase(t, &tc)
		})
	}
}

func applicationOccurrence(object manifest.BaseManifest, root string) OccurrenceFacts {
	source, path, hasPath := object.SourcePath()
	targets := manifest.ProducerTargets(object)
	occurrence := OccurrenceFacts{
		Declaration: OccurrenceID{Source: root}, Identity: object.Named(), Authored: object.Named(),
		Catalog: manifest.CatalogObject(object, object.Named()), Witness: object, Located: true, CatalogKnown: true,
		Source: source, Path: path, HasPath: hasPath, NeedsCandidates: manifest.ProducerNeedsCandidates(object),
		Roots: slices.Collect(manifest.RenderRoots(object)), Targets: targets, AuthoredTargets: targets,
	}
	occurrence.Structural, _ = occurrence.Catalog.(*manifest.Kustomization)
	if producer, ok := object.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok {
		policy := producer.EmissionPolicy()
		occurrence.Policy = &policy
	}
	return occurrence
}

type applicationInventoryCase struct {
	name, fixture                     string
	supported, wantProducer, wantPeer bool
}

func testApplicationInventoryCase(t *testing.T, tc *applicationInventoryCase) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{
		LocalPath: root,
		FS: fstest.MapFS{
			"entry/literal.yaml": {
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n"),
			},
		},
	}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), input)
	producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
	other := &manifest.HelmRelease{Name: "other", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{producer, other})
	application := &manifest.RawObject{
		APIVersion: "fluxcd.controlplane.io/v1",
		Kind:       "FluxInstance",
		Namespace:  "apps",
		Name:       "flux",
		Spec: map[string]any{
			"sync": map[string]any{
				"name": source.Name,
				"kind": source.Named().Kind,
				"url":  "https://fixture.invalid/repo",
				"ref":  "main",
				"path": "./entry",
			},
		},
	}
	if tc.name == "foreign" {
		foreign := &manifest.GitRepository{Name: "foreign", Namespace: "apps"}
		st.AddObject(foreign)
		st.SetArtifact(foreign.Named(), &store.SourceArtifact{LocalPath: t.TempDir(), FS: input.FS})
		application.Spec["sync"].(map[string]any)["name"] = foreign.Name
	}
	facts := &solverFacts{
		store: st, context: SourceContext{Root: root},
		index:       manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{}, roots: map[manifest.NamedResource]RootEvidence{},
	}

	occurrence := applicationOccurrence(producer, root)
	occurrence.Declaration.Source = root
	facts.occurrences[producer.Named()] = occurrence
	facts.index.Records[producer.Named()] = manifest.DiscoveryRecord{}
	facts.index.Records[other.Named()] = manifest.DiscoveryRecord{}
	peer := applicationOccurrence(other, root)
	peer.Declaration.Source = root
	facts.occurrences[other.Named()] = peer
	if tc.fixture == "bound_entry" {
		st.AddObject(manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", application.Named()))
	}
	if tc.fixture == "static" {
		fact := applicationOccurrence(application, root)
		fact.Declaration.Source = root
		facts.occurrences[application.Named()] = fact
		facts.index.Records[application.Named()] = manifest.DiscoveryRecord{
			Authored: application.Named(),
			Path:     filepath.Join(root, "entry", "application.yaml"),
			File:     "entry/application.yaml",
		}
		facts.roots[application.Named()] = RootEvidence{
			Identity: application.Named(),
			Scope:    RootScopeFacts{NamespaceEligible: true, Candidate: true},
		}
		current := application.Clone()
		if tc.name == "retired_static" {
			current.Spec = manifest.DeepCopyMap(application.Spec)
			current.Spec["sync"].(map[string]any)["path"] = "./elsewhere"
		}
		st.AddObject(current)
	}
	if tc.fixture == "known_owner" {
		entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).
			Named()
		facts.index.Records[entry] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: producer.Named()}},
		}
	}
	if tc.fixture == "emission" {
		children := []store.Child{{Obj: application}}
		if tc.name == "complete_negative" {
			children = nil
		}
		st.RecordEmitted(
			producer.Named(),
			&store.Emission{Children: children, Complete: tc.name != "partial_positive"},
			nil,
		)
		st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
		if tc.name == "stale" {
			fresh := producer.Clone()
			fresh.Values = map[string]any{"changed": true}
			st.AddObject(fresh)
			st.RecordEmitted(producer.Named(), &store.Emission{}, nil)
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusFailed})
		}
	}
	closure := solverClosure(t, st, facts)
	closure.context = facts.context
	if tc.supported {
		closure.proposal.reads = &closure.proposal.owned
		closure.proposal.reads.Init(st, manifest.NamedResource{}, nil)
		t.Cleanup(closure.proposal.reads.Retire)
		guard := &store.ReadSet{}
		guard.Init(st, application.Named(), nil)
		guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		t.Cleanup(func() { st.Retire(guard) })
		closure.proposal.entry = &EntryRoute{
			Application: &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
			Producer:    application.Named(), Reads: guard,
		}
	}
	if tc.fixture == "history" {
		document := map[string]any{
			"apiVersion": application.APIVersion,
			"kind":       application.Kind,
			"metadata":   map[string]any{"name": application.Name, "namespace": application.Namespace},
			"spec":       application.Spec,
		}
		data, err := json.Marshal(document)
		assert.NoError(t, err)
		closure.history = map[manifest.NamedResource]struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{producer.Named(): {Complete: tc.name != "history_partial", Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
			if tc.name != "history_negative" {
				yield(application.Named(), manifest.DiscoveryRecord{Application: string(data)})
			}
		}}}
		closure.readHistory()
	}
	if st.GetArtifact(source.Named(), store.ArtifactContent) == nil {
		t.Fatal("application fixture requires physical source bytes")
	}
	closure.materializers(
		&materializerDemand{
			target:      source.Named(),
			source:      root,
			application: filepath.Join(root, "entry"),
			required:    true,
		},
		manifest.NamedResource{},
		closure.materializerCandidates(false),
	)
	assert.Equal(t, closure.role(producer.Named()).needed, tc.wantProducer)
	assert.Equal(t, closure.role(producer.Named()).selected, false)
	assert.Equal(t, closure.role(producer.Named()).dirty, false)
	assert.Equal(t, closure.role(other.Named()).needed, tc.wantPeer)
}
