package store

import (
	"fmt"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestArtifact_ContentAndInventory(t *testing.T) {
	t.Parallel()
	for _, contentFirst := range []bool{false, true} {
		for _, clearContentFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("content_first=%t/clear_content_first=%t", contentFirst, clearContentFirst), func(t *testing.T) {
				t.Parallel()
				s := New()
				owner := &manifest.GitRepository{Name: "producer", Namespace: "apps"}
				dependency := manifest.NewConfigMap(manifest.NamedResource{Name: "input", Namespace: "apps"}, map[string]any{"key": "one"})
				s.AddObjects([]manifest.BaseManifest{owner, dependency})
				var first, second ReadSet
				first.Init(s, owner.Named(), nil)
				second.Init(s, owner.Named(), nil)
				defer first.Close()
				defer second.Close()
				first.GetObject(dependency.Named())
				_, _, inventory := first.GetEmission(owner.Named())
				defer inventory.Finish()
				child := emissionChild("generated", "one")
				child.Generated = true
				inventory.Children, inventory.Complete, inventory.Fingerprint = []Child{child}, true, "rendered"
				first.Bind(&inventory)
				content := &SourceArtifact{Reads: &first, Key: "source", Content: "one", FS: fstest.MapFS{}}
				var live Artifact
				stop := s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
					if id == owner.Named() {
						live, _ = value.(Artifact)
					}
				}, false)
				defer stop()
				if contentFirst {
					s.SetArtifact(owner.Named(), content)
				}
				s.RecordEmitted(owner.Named(), &inventory, nil)
				if !contentFirst {
					s.SetArtifact(owner.Named(), content)
				}
				s.Retire(&first)
				var captured ReadSet
				captured.Init(s, manifest.NamedResource{Kind: "Observer", Name: "observer"}, nil)
				defer captured.Close()
				if got := captured.GetArtifact(owner.Named(), ArtifactContent); got != content || live != content || first.retired.Load() {
					t.Fatalf("content=%v live=%v retired=%t", got, live, first.retired.Load())
				}
				var replay Artifact
				s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
					if id == owner.Named() {
						replay = value.(Artifact)
					}
				}, true)()
				_, _, current := s.GetEmission(owner.Named())
				defer current.Finish()
				if output, ok := current.Replay("rendered"); !ok || len(output.Children) != 1 || replay != content || !s.ReportHidden(child.Obj.Named()) {
					t.Fatal("content publication masked inventory or generated provenance")
				}
				clearContent := func() { s.SetArtifact(owner.Named(), nil) }
				clearInventory := func() {
					_, _, empty := second.GetEmission(owner.Named())
					defer empty.Finish()
					empty.Children, empty.Manifests, empty.Complete = nil, nil, true
					second.Bind(&empty)
					s.RecordEmitted(owner.Named(), &empty, nil)
				}
				if clearContentFirst {
					clearContent()
					if _, ok := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission); !ok || first.retired.Load() {
						t.Fatal("clearing content discarded retained inventory or retired its reads")
					}
					clearInventory()
				} else {
					clearInventory()
					if s.GetArtifact(owner.Named(), ArtifactContent) != content || first.retired.Load() {
						t.Fatal("replacing inventory discarded retained content or retired its reads")
					}
					clearContent()
				}
				if !first.retired.Load() || !second.Valid() || s.GetObject(child.Obj.Named()) != nil {
					t.Fatal("replaced output registrations or generated membership survived")
				}
				s.AddObject(manifest.NewConfigMap(dependency.Named(), map[string]any{"key": "two"}))
				if ids := slices.Collect(s.Sweep()); slices.Contains(ids, owner.Named()) {
					t.Fatalf("retired output scheduled its former owner: %v", ids)
				}
				if captured.Valid() {
					t.Fatal("clearing content did not invalidate a captured content reader")
				}
			})
		}
	}
}

func TestReportHidden_RepresentativeProvenance(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindHelmChart, manifest.KindOCIRepository, manifest.KindConfigMap} {
		for _, generated := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/generated=%t", kind, generated), func(t *testing.T) {
				t.Parallel()
				s := New()
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				s.AddObject(owner)
				id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "child"}
				child := &manifest.RawObject{Kind: kind, APIVersion: manifest.SourceDomain + "/v1", Name: id.Name, Namespace: id.Namespace}
				_, _, inventory := s.GetEmission(owner.Named())
				defer inventory.Finish()
				inventory.Children = []Child{{Obj: child, Generated: generated}}
				s.RecordEmitted(owner.Named(), &inventory, nil)
				if s.ReportHidden(id) != generated || !s.ReportHidden(manifest.BootstrapSourceID) {
					t.Fatal("report visibility did not follow generated provenance")
				}
				if info, hidden := s.ReportStatus(id); info.Status != "" || hidden != generated {
					t.Fatalf("unreported status = %+v hidden=%t", info, hidden)
				}
				s.UpdateStatus(id, StatusInfo{Status: StatusReady})
				if info, hidden := s.ReportStatus(id); info.Status != StatusReady || hidden != generated {
					t.Fatalf("reported status = %+v hidden=%t", info, hidden)
				}
				s.AddObject(child)
				_, hidden := s.ReportStatus(id)
				if s.ReportHidden(id) || hidden {
					t.Fatal("independent representative inherited auxiliary provenance")
				}
				s.DeleteObject(id)
				_, hidden = s.ReportStatus(id)
				if s.ReportHidden(id) != generated || hidden != generated {
					t.Fatal("removing the independent representative lost emitted provenance")
				}
			})
		}
	}
}

func TestRecordEmitted_ProvenanceOnlyChange(t *testing.T) {
	t.Parallel()
	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	child := manifest.NewConfigMap(manifest.NamedResource{Name: "child", Namespace: "apps"}, nil)
	s.AddObject(owner)
	for _, generated := range []bool{true, false, true} {
		_, _, inventory := s.GetEmission(owner.Named())
		inventory.Children = []Child{{Obj: child, Generated: generated}}
		s.RecordEmitted(owner.Named(), &inventory, nil)
		inventory.Finish()
		if s.GetObject(child.Named()) != child || s.ReportHidden(child.Named()) != generated {
			t.Fatal("equal child content bypassed its provenance transition")
		}
	}
}

func TestReadSet_CloseRetiresSourceAndInventory(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.GitRepository{Name: "producer", Namespace: "apps"}
	contentInput := manifest.NewConfigMap(manifest.NamedResource{Name: "content-input", Namespace: "apps"}, map[string]any{"VALUE": "first"})
	inventoryInput := manifest.NewConfigMap(manifest.NamedResource{Name: "inventory-input", Namespace: "apps"}, map[string]any{"VALUE": "first"})
	st.AddObjects([]manifest.BaseManifest{owner, contentInput, inventoryInput})
	var contentReads, inventoryReads ReadSet
	contentReads.Init(st, owner.Named(), nil)
	inventoryReads.Init(st, owner.Named(), nil)
	defer contentReads.Retire()
	defer inventoryReads.Retire()
	contentReads.GetObject(contentInput.Named())
	inventoryReads.GetObject(inventoryInput.Named())
	st.SetArtifact(owner.Named(), &SourceArtifact{Reads: &contentReads, Content: "source-generation", FS: fstest.MapFS{}})
	_, _, output := inventoryReads.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []Child{emissionChild("generated", "value")}, true
	inventoryReads.Bind(&output)
	st.RecordEmitted(owner.Named(), &output, nil)
	st.Retire(&contentReads)
	st.Retire(&inventoryReads)
	if contentReads.retired.Load() || inventoryReads.retired.Load() {
		t.Fatal("retirement discarded a retained output")
	}
	var closing ReadSet
	closing.Init(st, owner.Named(), nil)
	closing.Close()
	if !contentReads.retired.Load() || !inventoryReads.retired.Load() {
		t.Fatal("close left an independently retained output registered")
	}
	for _, input := range []*manifest.ConfigMap{contentInput, inventoryInput} {
		st.AddObject(manifest.NewConfigMap(input.Named(), map[string]any{"VALUE": "second"}))
	}
	if ids := slices.Collect(st.Sweep()); slices.Contains(ids, owner.Named()) {
		t.Fatalf("closed outputs scheduled their owner: %v", ids)
	}
}
