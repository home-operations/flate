package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDeclaration_Authority(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "input"}
			object := func(data map[string]any) manifest.BaseManifest {
				if kind == manifest.KindSecret {
					return manifest.NewSecret(id, nil, data)
				}
				return manifest.NewConfigMap(id, data)
			}
			s := New()
			first := object(map[string]any{"old": "old"})
			last := object(map[string]any{"value": "supplied"})
			for _, obj := range []manifest.BaseManifest{first, last} {
				s.AddDeclaration(
					Declaration{Obj: obj, Doc: testutil.ObjectDocument(obj), Origin: manifest.SuppliedDeclaration},
				)
			}
			assert.Equal(t, s.GetObject(id), last)
			assert.Equal(t, s.GetDeclaration(id), last)
			assert.Equal(t, len(s.ListObjects("", kind)), 1)
			assert.Equal(t, s.Policy(id).Root, false)
			assert.Equal(t, s.ReportHidden(id), true)
			if value, _ := s.ReferenceFacts(id, nil, false, manifest.NamedResource{}).Data.Value("old"); value.Found {
				t.Fatal("duplicate retained old key")
			}
			reads := &ReadSet{}
			reads.Init(s, testutil.KustomizationID("apps", "consumer"), nil)
			defer reads.Retire()
			assert.Equal(t, reads.GetObject(id), last)
			assert.Equal(t, reads.Policy(id), s.Policy(id))
			repository := object(map[string]any{"value": "repository"})
			s.AddObject(repository)
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, reads.Policy(id).Origin, manifest.SuppliedDeclaration)
			assert.Equal(t, s.Policy(id).Origin, manifest.RepositoryDeclaration)
			assert.Equal(t, s.GetObject(id), repository)
			assert.Equal(t, s.ReportHidden(id), false)
			before := &ReadSet{}
			before.Init(s, testutil.KustomizationID("apps", "other"), nil)
			defer before.Retire()
			before.GetObject(id)
			observed := false
			stop := s.AddListener(EventObjectAdded, func(got manifest.NamedResource, payload any) {
				if got == id {
					observed = true
					assert.Equal(t, payload, any(last))
					assert.Equal(t, s.GetObject(id), last)
				}
			}, false)
			assert.Equal(t, s.DeleteObject(id), true)
			stop()
			assert.Equal(t, observed, true)
			assert.Equal(t, before.Valid(), false)
			assert.Equal(t, s.GetObject(id), last)
			obj, _ := s.Snapshot(id, false)
			assert.Equal(t, obj, last)
			assert.Equal(t, len(s.ListObjects("", kind)), 1)
			assert.Equal(t, s.ReportHidden(id), true)
		})
	}
}

func TestDeclaration_SourceWithdrawal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		ancestor bool
	}{
		{"direct", false},
		{"ancestor", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			s.AddObject(parent)
			reference := &manifest.GitRepository{
				Name:      "source",
				Namespace: "apps",
				URL:       "https://fixture.invalid/reference",
			}
			repository := &manifest.GitRepository{
				Name:      "source",
				Namespace: "apps",
				URL:       "https://fixture.invalid/repository",
			}
			s.AddDeclaration(
				Declaration{
					Obj:    reference,
					Doc:    testutil.ObjectDocument(reference),
					Origin: manifest.SuppliedDeclaration,
				},
			)
			producer := parent
			if tc.ancestor {
				producer = &manifest.Kustomization{Name: "producer", Namespace: "apps"}
				s.RecordEmitted(parent.Named(), &Emission{Children: []Child{{Obj: producer}}, Complete: true}, nil)
			}
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: repository}}, Complete: true}, nil)
			s.SetArtifact(repository.Named(), &SourceArtifact{Key: "repository"})
			s.UpdateStatus(repository.Named(), StatusInfo{Status: StatusReady})
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			reads.GetObject(repository.Named())
			reads.GetArtifact(repository.Named(), ArtifactContent)
			reads.GetStatus(repository.Named())
			done := make(chan struct{})
			go func() { s.RecordEmitted(parent.Named(), &Emission{Complete: true}, nil); close(done) }()
			<-done
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, s.GetObject(reference.Named()), manifest.BaseManifest(reference))
			assert.Equal(t, s.GetArtifact(reference.Named(), ArtifactContent), Artifact(nil))
			_, known := s.GetStatus(reference.Named())
			assert.Equal(t, known, false)
			assert.Equal(t, len(s.ListObjects(manifest.SourceDomain, manifest.KindGitRepository)), 1)
			s.SetArtifact(reference.Named(), &SourceArtifact{Key: "reference"})
			s.UpdateStatus(reference.Named(), StatusInfo{Status: StatusReady})
			for range s.Sweep() {
			}
			if artifact, ok := s.GetArtifact(reference.Named(), ArtifactContent).(*SourceArtifact); !ok ||
				artifact.Key != "reference" {
				t.Fatal("reference acquisition was retired")
			}
		})
	}
}

func TestDeclaration_ProducerWithdrawal(t *testing.T) {
	t.Parallel()
	s := New()
	parent := &manifest.ResourceSet{Name: "parent", Namespace: "apps"}
	reference := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "supplied"}
	repository := reference.Clone()
	repository.Path = "repository"
	leaf := &manifest.ConfigMap{Name: "leaf", Namespace: "apps"}
	s.AddObject(parent)
	s.AddDeclaration(
		Declaration{Obj: reference, Doc: testutil.ObjectDocument(reference), Origin: manifest.SuppliedDeclaration},
	)
	s.RecordEmitted(parent.Named(), &Emission{Children: []Child{{Obj: repository}}, Complete: true}, nil)
	s.RecordEmitted(repository.Named(), &Emission{Children: []Child{{Obj: leaf}}, Complete: true}, nil)
	assert.Equal(t, s.GetObject(leaf.Named()), manifest.BaseManifest(leaf))
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	reads.GetObject(repository.Named())
	reads.GetObject(leaf.Named())
	done := make(chan struct{})
	go func() { s.RecordEmitted(parent.Named(), &Emission{Complete: true}, nil); close(done) }()
	<-done
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, s.GetDeclaration(reference.Named()), manifest.BaseManifest(reference))
	assert.Equal(t, s.Policy(reference.Named()).Reconcile, false)
	assert.Equal(t, len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)), 1)
	assert.Equal(t, s.GetObject(leaf.Named()), manifest.BaseManifest(nil))
	_, _, inventory := s.GetEmission(reference.Named())
	defer inventory.Finish()
	assert.Equal(t, len(inventory.Children), 0)
	s.RecordEmitted(parent.Named(), &Emission{Children: []Child{{Obj: repository}}, Complete: true}, nil)
	assert.Equal(t, s.Policy(reference.Named()).Reconcile, true)
	assert.Equal(t, s.GetObject(leaf.Named()), manifest.BaseManifest(leaf))
}

func TestPolicy_CapabilityReplacement(t *testing.T) {
	t.Parallel()
	raw := &manifest.RawObject{
		APIVersion: manifest.FluxKustomizeDomain + "/v1",
		Kind:       manifest.KindKustomization,
		Name:       "input",
		Namespace:  "apps",
	}
	first := &manifest.Kustomization{Name: "input", Namespace: "apps", Path: "first"}
	second := &manifest.Kustomization{Name: "input", Namespace: "apps", Path: "second"}
	for _, tc := range []struct {
		name            string
		before, after   manifest.BaseManifest
		beforeReconcile bool
		afterReconcile  bool
	}{
		{"raw_to_typed", raw, first, false, true},
		{"typed_to_raw", first, raw, true, false},
		{"typed_update", first, second, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			id := tc.before.Named()
			s.AddObject(tc.before)
			var before ReadSet
			before.Init(s, ownerID("reader"), nil)
			defer before.Retire()
			assert.Equal(t, before.Policy(id).Reconcile, tc.beforeReconcile)
			s.AddObject(tc.after)
			assert.Equal(t, before.Valid(), false)
			assert.Equal(t, before.Policy(id).Reconcile, tc.beforeReconcile)
			assert.Equal(t, s.Policy(id).Reconcile, tc.afterReconcile)
			var current ReadSet
			current.Init(s, ownerID("current"), nil)
			defer current.Retire()
			assert.Equal(t, current.Policy(id), s.Policy(id))
		})
	}
}
