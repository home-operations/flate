package store

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
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
