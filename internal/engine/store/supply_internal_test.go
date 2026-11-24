package store

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDeclaration_Authority(t *testing.T) {
	t.Parallel()
	t.Run("child_equality", func(t *testing.T) {
		t.Parallel()
		obj := &manifest.GitRepository{Name: "source", Namespace: "apps"}
		doc := testutil.ObjectDocument(obj)
		id := obj.Named()
		repository := Child{Obj: obj, Doc: doc}
		for _, tc := range []struct {
			name     string
			admit    func(*Store)
			supplied bool
		}{
			{name: "repository", admit: func(s *Store) { s.AddObject(obj, doc) }},
			{name: "construction", admit: func(s *Store) { s.AddConstructionDeclaration(obj, doc) }},
			{
				name: "supplied",
				admit: func(s *Store) {
					s.AddDeclaration(Declaration{Obj: obj, Doc: doc, Role: SuppliedDeclaration})
				},
				supplied: true,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				s := New()
				tc.admit(s)
				declared, policy := s.Lookup(id)
				assert.Equal(t, declared, manifest.BaseManifest(obj))
				assert.Equal(t, policy.Reconcile, true)
				assert.Equal(t, policy.Supplied(), tc.supplied)
				assert.Equal(t, policy.RootCandidate(), tc.name != "construction")
				child := policy.Child(declared)
				child.Doc = doc
				assert.Equal(t, child.sameDeclaration(repository), !tc.supplied)

				var reads ReadSet
				reads.Init(s, ownerID("reader"), nil)
				t.Cleanup(reads.Retire)
				captured, capturedPolicy := reads.Lookup(id)
				assert.Equal(t, captured, declared)
				assert.Equal(t, capturedPolicy, policy)
				child = capturedPolicy.Child(captured)
				child.Doc = doc
				assert.Equal(t, child.sameDeclaration(repository), !tc.supplied)

				projected := obj.WithNamespace("target")
				child.Obj, child.Doc = projected, testutil.ObjectDocument(projected)
				projectedID := projected.Named()
				assert.Equal(t, child.Policy(projectedID), policy)
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				s.AddObject(owner)
				s.RecordEmitted(owner.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
				published, publishedPolicy := s.Lookup(projectedID)
				assert.Equal(t, published, projected)
				assert.Equal(t, publishedPolicy, policy)
			})
		}
	})
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "input"}
			object := func(data map[string]any) manifest.BaseManifest {
				if kind == manifest.KindSecret {
					return &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: data}
				}
				return &manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: data}
			}
			s := New()
			first := object(map[string]any{"old": "old"})
			last := object(map[string]any{"value": "supplied"})
			for _, obj := range []manifest.BaseManifest{first, last} {
				s.AddDeclaration(
					Declaration{Obj: obj, Doc: testutil.ObjectDocument(obj), Role: SuppliedDeclaration},
				)
			}
			assert.Equal(t, s.GetObject(id), last)
			assert.Equal(t, s.GetDeclaration(id), last)
			assert.Equal(t, len(s.ListObjects("", kind)), 1)
			supplied, policy := s.Lookup(id)
			assert.Equal(t, supplied, last)
			assert.Equal(t, policy.Supplied(), true)
			assert.Equal(t, policy.Root, false)
			assert.Equal(t, policy.RootCandidate(), true)
			assert.Equal(t, policy.Child(last).Policy(id), policy)
			assert.Equal(t, s.ReportHidden(id), true)
			if value, _ := s.ReferenceFacts(id, nil, manifest.NamedResource{}).Data.Value("old"); value.Found {
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
			assert.Equal(t, reads.Policy(id).Supplied(), true)
			assert.Equal(t, s.Policy(id).Root, true)
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

			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			s.AddObject(owner)
			for _, visible := range []bool{false, true} {
				s.RecordEmitted(owner.Named(), &Emission{
					Children: []Child{{Obj: last, Generated: true, VisibleGeneration: visible}},
					Complete: true,
				}, nil)
				obj, currentPolicy := s.Lookup(id)
				assert.Equal(t, obj, last)
				assert.Equal(t, currentPolicy.Supplied(), false)
				assert.Equal(t, currentPolicy.Root, true)
				assert.Equal(t, currentPolicy.Report, visible)
				var current ReadSet
				current.Init(s, testutil.KustomizationID("apps", "published"), nil)
				captured, capturedPolicy := current.Lookup(id)
				current.Retire()
				assert.Equal(t, captured, obj)
				assert.Equal(t, capturedPolicy, currentPolicy)
				assert.Equal(t, reads.Policy(id), policy)
			}
		})
	}
}

func TestDeclaration_ContentReplacement(t *testing.T) {
	t.Parallel()
	for _, role := range []DeclarationRole{RepositoryDeclaration, SuppliedDeclaration} {
		t.Run(fmt.Sprint(role), func(t *testing.T) {
			t.Parallel()
			st := New()
			obj := &manifest.Kustomization{Name: "input", Namespace: "apps"}
			first := testutil.ObjectDocument(obj)
			st.AddDeclaration(Declaration{Obj: obj, Doc: first, Role: role})
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Retire()
			_, _, before := reads.GetEmission(obj.Named())
			defer before.Finish()
			assert.Diff(t, before.inputDoc, first)
			second := manifest.DeepCopyMap(first)
			second["unmodeled"] = "replacement"
			st.AddDeclaration(Declaration{Obj: obj, Doc: second, Role: role})
			_, document, current := st.GetEmission(obj.Named())
			assert.Diff(t, document, second)
			assert.Diff(t, current.inputDoc, second)
			current.Finish()
			st.AddDeclaration(Declaration{Obj: obj, Doc: second, Role: role})
			_, document, unknown := st.GetEmission(obj.Named())
			assert.Diff(t, document, second)
			assert.Diff(t, unknown.inputDoc, second)
			unknown.Finish()
			st.AddDeclaration(Declaration{Obj: obj, Role: role})
			_, document, missing := st.GetEmission(obj.Named())
			assert.Equal(t, document == nil, true)
			assert.Equal(t, missing.inputDoc == nil, true)
			missing.Finish()
			assert.Diff(t, before.inputDoc, first)
			assert.Equal(t, before.inputDoc["unmodeled"], nil)
		})
	}
}

func TestDeclaration_SourceWithdrawal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		ancestor     bool
		construction bool
	}{
		{name: "direct"},
		{name: "ancestor", ancestor: true},
		{name: "direct/construction", construction: true},
		{name: "ancestor/construction", ancestor: true, construction: true},
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
			document := testutil.ObjectDocument(reference)
			if tc.construction {
				s.AddConstructionDeclaration(reference, document)
			} else {
				s.AddDeclaration(Declaration{Obj: reference, Doc: document, Role: SuppliedDeclaration})
			}
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
		Declaration{Obj: reference, Doc: testutil.ObjectDocument(reference), Role: SuppliedDeclaration},
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
	source := &manifest.GitRepository{Name: "input", Namespace: "apps"}
	rawSource := &manifest.RawObject{
		APIVersion: manifest.SourceDomain + "/v1",
		Kind:       manifest.KindGitRepository,
		Name:       source.Name,
		Namespace:  source.Namespace,
	}
	for _, tc := range []struct {
		name            string
		before, after   manifest.BaseManifest
		beforeReconcile bool
		afterReconcile  bool
		construction    bool
	}{
		{name: "raw_to_typed", before: raw, after: first, afterReconcile: true},
		{name: "typed_to_raw", before: first, after: raw, beforeReconcile: true},
		{name: "typed_update", before: first, after: second, beforeReconcile: true, afterReconcile: true},
		{name: "construction_source", before: rawSource, after: source, afterReconcile: true, construction: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			id := tc.before.Named()
			admit := func(obj manifest.BaseManifest) { s.AddObject(obj) }
			if tc.construction {
				admit = func(obj manifest.BaseManifest) { s.AddConstructionDeclaration(obj, nil) }
			}
			admit(tc.before)
			var before ReadSet
			before.Init(s, ownerID("reader"), nil)
			defer before.Retire()
			assert.Equal(t, before.Policy(id).Reconcile, tc.beforeReconcile)
			admit(tc.after)
			assert.Equal(t, before.Valid(), false)
			assert.Equal(t, before.Policy(id).Reconcile, tc.beforeReconcile)
			obj, policy := s.Lookup(id)
			assert.Equal(t, obj, tc.after)
			assert.Equal(t, policy.Reconcile, tc.afterReconcile)
			assert.Equal(t, policy.Supplied(), false)
			assert.Equal(t, policy.RootCandidate(), !tc.construction)
			assert.Equal(t, policy.Child(obj).Policy(id), policy)
			var current ReadSet
			current.Init(s, ownerID("current"), nil)
			defer current.Retire()
			assert.Equal(t, current.Policy(id), s.Policy(id))
		})
	}
}
