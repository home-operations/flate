package store

import (
	"fmt"
	"strconv"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestGetDeclaration_ClassifiedSourceLifecycle(t *testing.T) {
	t.Parallel()
	for _, preIndex := range []bool{false, true} {
		t.Run(strconv.FormatBool(preIndex), func(t *testing.T) {
			t.Parallel()
			testClassifiedSourceLifecycle(t, preIndex)
		})
	}
}

func testClassifiedSourceLifecycle(t *testing.T, preIndex bool) {
	t.Helper()
	s := New()
	source := manifest.BootstrapSourceID
	owner := &manifest.Kustomization{
		Name:      "parent",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      source.Kind,
			Name:      source.Name,
			Namespace: source.Namespace,
		},
	}
	raw := emissionChild("child", "raw")
	s.AddObject(owner)
	s.SetArtifact(source, &SourceArtifact{Content: "source", FS: fstest.MapFS{}})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		raw.Obj.Named(): {
			Parent:      owner.Named(),
			Productions: []manifest.Production{{Owner: owner.Named(), Source: source}},
		},
	}}
	if !preIndex {
		s.SetDiscoveryIndex(index)
	}
	var admission ReadSet
	admission.Init(s, manifest.NamedResource{}, nil)
	defer admission.Retire()
	assert.Equal(t, admission.SourceAdmission(raw.Obj.Named(), source), true)
	marked := raw
	marked.SourceSupport = source
	admission.Admit(marked, source, manifest.NamedResource{})
	if preIndex {
		s.SetDiscoveryIndex(index)
	}
	assert.Equal(t, admission.Valid(), true)
	observe := func(want manifest.BaseManifest) {
		t.Helper()
		assert.Equal(t, s.GetDeclaration(raw.Obj.Named()), want)
		var current ReadSet
		current.Init(s, ownerID("reader"), nil)
		defer current.Retire()
		assert.Equal(t, current.GetDeclaration(raw.Obj.Named()), want)
		assert.Equal(t, current.Valid(), true)
	}
	observe(raw.Obj)
	var before ReadSet
	before.Init(s, ownerID("before"), nil)
	defer before.Retire()
	before.GetDeclaration(raw.Obj.Named())
	input := emissionChild("input", "first")
	s.AddObject(input.Obj)
	var output ReadSet
	output.Init(s, owner.Named(), nil)
	output.GetObject(input.Obj.Named())
	effective := emissionChild("child", "effective")
	effective.Entry, effective.SourceSupport = &manifest.EntryApplication{}, source
	emission := Emission{Children: []Child{effective}, Complete: true, Fingerprint: "effective"}
	output.Bind(&emission)
	for entryIndex := range emission.entries {
		entry := emission.entries[entryIndex]
		if entry.child.Entry != nil || entry.child.SourceSupport != (manifest.NamedResource{}) {
			t.Fatal("prospective entry retained transient authority")
		}
	}
	s.RecordEmitted(owner.Named(), &emission, nil)
	observe(effective.Obj)
	assert.Equal(t, before.Valid(), false)
	assert.Equal(t, s.EmissionOwner(raw.Obj.Named()), owner.Named())
	assert.Equal(t, s.SourceAdmission(raw.Obj.Named(), source), false)
	s.AddObject(emissionChild("input", "replacement").Obj)
	assert.Equal(t, output.Valid(), false)
	nextValues := []Emission{
		{
			Complete: true,
		},
		{
			Failures: []manifest.DocOccurrence{
				{
					Err: fmt.Errorf(
						"failed"),
				},
			},
		},
		{
			Complete: true,
		},
	}
	for nextIndex := range nextValues {
		next := nextValues[nextIndex]

		s.RecordEmitted(owner.Named(), &next, nil)
		observe(nil)
		assert.Equal(t, s.SourceAdmission(raw.Obj.Named(), source), false)
		var retry ReadSet
		retry.Init(s, manifest.NamedResource{}, nil)
		retry.Admit(marked, source, manifest.NamedResource{})
		retry.Retire()
		observe(nil)
	}
	replay, ok := emission.Replay("effective")
	assert.Equal(t, ok, true)
	for childIndex := range replay.Children {
		child := replay.Children[childIndex]
		if child.Entry != nil || child.SourceSupport != (manifest.NamedResource{}) {
			t.Fatal("replay retained transient authority")
		}
	}
	s.RecordEmitted(owner.Named(), &Emission{Children: replay.Children, Complete: true}, nil)
	observe(effective.Obj)
	assert.Equal(t, s.SourceAdmission(raw.Obj.Named(), source), false)
}

func TestSourceAdmission_ConcurrentReplacement(t *testing.T) {
	t.Parallel()
	for _, replacement := range []string{"source", "owner", "omission"} {
		t.Run(replacement, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := manifest.BootstrapSourceID
			owner := &manifest.Kustomization{
				Name:      "parent",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind:      source.Kind,
					Name:      source.Name,
					Namespace: source.Namespace,
				},
			}
			raw := emissionChild("child", "raw")
			raw.SourceSupport = source
			s.AddObject(owner)
			s.SetArtifact(source, &SourceArtifact{Content: "first", FS: fstest.MapFS{}})
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Obj.Named(): {
					Parent:      owner.Named(),
					Productions: []manifest.Production{{Owner: owner.Named(), Source: source}},
				},
			}})
			var pending ReadSet
			pending.Init(s, manifest.NamedResource{}, nil)
			defer pending.Retire()
			assert.Equal(t, pending.SourceAdmission(raw.Obj.Named(), source), true)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if replacement == "source" {
					s.SetArtifact(source, &SourceArtifact{Content: "second", FS: fstest.MapFS{}})
					return
				}
				next := Emission{Complete: true}
				if replacement == "owner" {
					next.Children = []Child{emissionChild("child", "effective")}
				}
				var publishing ReadSet
				publishing.Init(s, owner.Named(), nil)
				publishing.Bind(&next)
				s.RecordEmitted(owner.Named(), &next, nil)
			}()
			<-done
			assert.Equal(t, pending.Valid(), false)
			pending.Admit(raw, source, manifest.NamedResource{})
			if got := s.GetObject(raw.Obj.Named()); got == raw.Obj {
				t.Fatal("stale qualification published original source bytes")
			}
			if replacement != "source" {
				assert.Equal(t, s.SourceAdmission(raw.Obj.Named(), source), false)
			}
		})
	}
}

func TestGetDeclaration_PreparedIdentityLifecycle(t *testing.T) {
	t.Parallel()
	for _, replacement := range []string{"source", "owner", "generation", "removal"} {
		t.Run(replacement, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := manifest.BootstrapSourceID
			owner := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := emissionChild("child", "prepared")
			id := child.Obj.Named()
			authored := id
			authored.Namespace = ""
			s.AddObject(owner)
			s.SetArtifact(source, &SourceArtifact{Content: "first", FS: fstest.MapFS{}})
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {
					Authored:     authored,
					Path:         "child.yaml",
					File:         "kustomization.yaml",
					Builder:      "kustomization.yaml",
					BuilderOwner: owner.Named(),
					Parent:       owner.Named(),
					Declaration:  child.Obj,
					Productions:  []manifest.Production{{Owner: owner.Named(), Source: source}},
				},
			}})
			var reads ReadSet
			reads.Init(s, manifest.NamedResource{}, nil)
			defer reads.Retire()
			child.SourceSupport = source
			assert.Equal(t, reads.SourceAdmission(id, source), true)
			reads.Admit(child, source, manifest.NamedResource{})
			assert.Equal(t, s.GetDeclaration(id), child.Obj)
			switch replacement {
			case "source":
				s.SetArtifact(source, &SourceArtifact{Content: "second", FS: fstest.MapFS{}})
			case "generation":
				s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
			case "removal":
				s.DeleteObject(owner.Named())
			default:
				next := owner.Clone()
				next.Path = "replacement"
				s.AddObject(next)
			}
			assert.Equal(t, s.GetDeclaration(id), nil)
		})
	}
}
