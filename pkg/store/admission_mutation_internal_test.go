package store

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_AdmissionMutationPreconditions(t *testing.T) {
	t.Parallel()
	for _, listing := range []string{"all", "empty-kind", "present-kind"} {
		t.Run(listing, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := manifest.BootstrapSourceID
			s.SetArtifact(source, &SourceArtifact{Content: "source", FS: fstest.MapFS{}})
			if listing == "present-kind" {
				s.AddObject(&manifest.ConfigMap{Name: "existing", Namespace: "apps"})
			}
			first := &manifest.ConfigMap{Name: "first", Namespace: "apps"}
			second := &manifest.ConfigMap{Name: "second", Namespace: "apps"}
			var reads, observer ReadSet
			reads.Init(s, ownerID("admission"), nil)
			observer.Init(s, ownerID("observer"), nil)
			defer reads.Retire()
			defer observer.Retire()
			for _, r := range []*ReadSet{&reads, &observer} {
				r.GetDeclaration(first.Named())
				r.GetDeclaration(second.Named())
				kind := manifest.KindConfigMap
				if listing == "all" {
					kind = ""
				}
				r.ListObjects("", kind)
			}
			reads.Admit(Child{Obj: first, SourceSupport: source}, source, manifest.NamedResource{})
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, observer.Valid(), false)
			assert.Equal(t, reads.GetDeclaration(first.Named()), nil)
			reads.Admit(Child{Obj: second, SourceSupport: source}, source, manifest.NamedResource{})
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, s.GetDeclaration(second.Named()), manifest.BaseManifest(second))
			for range s.Sweep() {
			}
			assert.Equal(t, reads.Valid(), true)
			published := make(chan struct{})
			go func() {
				s.AddObject(&manifest.ConfigMap{Name: "foreign", Namespace: "apps"})
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
