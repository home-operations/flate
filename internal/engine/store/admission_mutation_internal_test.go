package store

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_AdmissionMutationPreconditions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		all, present, producer bool
	}{
		{name: "all", all: true},
		{name: "empty-kind"},
		{name: "present-kind", present: true},
		{name: "producer-kind", producer: true},
		{name: "producer-all", all: true, producer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := manifest.BootstrapSourceID
			s.SetArtifact(source, &SourceArtifact{Content: "source", FS: fstest.MapFS{}})
			if tc.present {
				s.AddObject(&manifest.ConfigMap{Name: "existing", Namespace: "apps"})
			}
			var first, second manifest.BaseManifest = &manifest.ConfigMap{Name: "first", Namespace: "apps"},
				&manifest.ConfigMap{Name: "second", Namespace: "apps"}
			if tc.producer {
				first = &manifest.Kustomization{Name: "first", Namespace: "apps"}
				second = &manifest.Kustomization{Name: "second", Namespace: "apps"}
			}
			var reads, observer ReadSet
			reads.Init(s, ownerID("admission"), nil)
			observer.Init(s, ownerID("observer"), nil)
			defer reads.Retire()
			defer observer.Retire()
			id := first.Named()
			group, kind := id.Group, id.Kind
			if tc.all {
				group, kind = "", ""
			}
			for _, r := range []*ReadSet{&reads, &observer} {
				if tc.producer {
					r.SourceAdmission(first.Named(), source)
					r.SourceAdmission(second.Named(), source)
				}
				r.GetDeclaration(first.Named())
				r.GetDeclaration(second.Named())
				r.ListObjects(group, kind)
			}
			reads.Admit(Child{Obj: first, SourceSupport: source}, source, manifest.NamedResource{})
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, observer.Valid(), false)
			assert.Equal(t, reads.GetDeclaration(first.Named()), nil)
			reads.Admit(Child{Obj: second, SourceSupport: source}, source, manifest.NamedResource{})
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, s.GetDeclaration(second.Named()), second)
			for range s.Sweep() {
			}
			assert.Equal(t, reads.Valid(), true)
			published := make(chan struct{})
			go func() {
				s.AddObject(second.WithNamespace("foreign"))
				close(published)
			}()
			<-published
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestReadSet_AdmissionPreservesStaleEvidence(t *testing.T) {
	t.Parallel()
	for _, cause := range []string{"target", "listing", "source", "source-after-first", "present-object"} {
		t.Run(cause, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := manifest.BootstrapSourceID
			s.SetArtifact(source, &SourceArtifact{Content: "source", FS: fstest.MapFS{}})
			first := &manifest.ConfigMap{Name: "first", Namespace: "apps"}
			second := &manifest.ConfigMap{Name: "second", Namespace: "apps"}
			present := &manifest.ConfigMap{Name: "present", Namespace: "apps"}
			s.AddObject(present)
			var reads ReadSet
			reads.Init(s, ownerID("admission"), nil)
			defer reads.Retire()
			reads.GetDeclaration(first.Named())
			reads.GetDeclaration(second.Named())
			reads.GetDeclaration(present.Named())
			reads.GetArtifact(source, ArtifactContent)
			reads.ListObjects("", "")
			switch cause {
			case "target":
				s.AddObject(&manifest.ConfigMap{
					Name: "first", Namespace: "apps", Data: map[string]any{"value": "foreign"},
				})
			case "listing":
				s.AddObject(&manifest.ConfigMap{Name: "foreign", Namespace: "apps"})
			case "source":
				s.SetArtifact(source, &SourceArtifact{Content: "replacement", FS: fstest.MapFS{}})
			case "source-after-first", "present-object":
				reads.Admit(Child{Obj: first, SourceSupport: source}, source, manifest.NamedResource{})
				assert.Equal(t, reads.Valid(), true)
				if cause == "source-after-first" {
					s.SetArtifact(source, &SourceArtifact{Content: "replacement", FS: fstest.MapFS{}})
				} else {
					s.AddObject(&manifest.ConfigMap{
						Name: "present", Namespace: "apps", Data: map[string]any{"value": "foreign"},
					})
				}
			}
			reads.Admit(Child{Obj: second, SourceSupport: source}, source, manifest.NamedResource{})
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, s.GetDeclaration(second.Named()), nil)
		})
	}
}

func TestReadSet_AdmissionStableHiddenDeclaration(t *testing.T) {
	t.Parallel()
	for _, replacement := range []string{"source", "declaration"} {
		t.Run(replacement, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := manifest.BootstrapSourceID
			owner := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			repository := &manifest.ConfigMap{
				Name: "input", Namespace: "apps", Data: map[string]any{"key": "repository"},
			}
			supplied := &manifest.ConfigMap{
				Name: repository.Name, Namespace: repository.Namespace, Data: map[string]any{"key": "supplied"},
			}
			id := repository.Named()
			s.AddObject(owner)
			s.SetArtifact(source, &SourceArtifact{Content: "first", FS: fstest.MapFS{}})
			s.AddDeclaration(
				Declaration{
					Obj:    supplied,
					Doc:    testutil.ObjectDocument(supplied),
					Origin: manifest.SuppliedDeclaration,
				},
			)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Parent: owner.Named(), Productions: []manifest.Production{{Owner: owner.Named(), Source: source}}},
			}})
			assert.Equal(t, s.GetDeclaration(id), manifest.BaseManifest(supplied))
			assert.Equal(t, s.shardFor(id).cells[id].static, manifest.BaseManifest(nil))
			var initial ReadSet
			initial.Init(s, ownerID("initial"), nil)
			defer initial.Close()
			initial.Admit(Child{Obj: repository}, source, manifest.NamedResource{})
			_, _, emission := s.GetEmission(owner.Named())
			emission.Complete = true
			s.RecordEmitted(owner.Named(), &emission, nil)
			emission.Finish()

			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Close()
			assert.Equal(t, reads.GetDeclaration(id), manifest.BaseManifest(supplied))
			reads.GetArtifact(source, ArtifactContent)
			initial.Admit(Child{Obj: repository}, source, manifest.NamedResource{})
			assert.Equal(t, reads.Valid(), true)
			for range 2 {
				var retry ReadSet
				retry.Init(s, ownerID("retry"), nil)
				retry.GetDeclaration(id)
				same := *repository
				retry.Admit(Child{Obj: &same}, source, manifest.NamedResource{})
				assert.Equal(t, retry.Valid(), true)
				retry.Close()
				assert.Equal(t, reads.Valid(), true)
				assert.Equal(t, s.GetDeclaration(id), manifest.BaseManifest(supplied))
				assert.Equal(t, s.Policy(id).Origin, manifest.SuppliedDeclaration)
			}
			next := *repository
			if replacement == "declaration" {
				next.Data = map[string]any{"key": "replacement"}
			}
			changed := make(chan struct{})
			go func() {
				defer close(changed)
				if replacement == "source" {
					s.SetArtifact(source, &SourceArtifact{Content: "second", FS: fstest.MapFS{}})
					return
				}
				var update ReadSet
				update.Init(s, ownerID("replacement"), nil)
				defer update.Close()
				update.Admit(Child{Obj: &next}, source, manifest.NamedResource{})
			}()
			<-changed
			assert.Equal(t, reads.Valid(), false)
			var replacementReads ReadSet
			replacementReads.Init(s, ownerID("replacement-admission"), nil)
			defer replacementReads.Close()
			replacementReads.Admit(Child{Obj: &next}, source, manifest.NamedResource{})
			assert.Equal(t, replacementReads.Valid(), true)
			var current ReadSet
			current.Init(s, ownerID("current"), nil)
			defer current.Close()
			assert.Equal(t, current.GetDeclaration(id), manifest.BaseManifest(supplied))
			current.GetArtifact(source, ArtifactContent)
			current.Admit(Child{Obj: &next}, source, manifest.NamedResource{})
			assert.Equal(t, current.Valid(), true)
		})
	}
}
