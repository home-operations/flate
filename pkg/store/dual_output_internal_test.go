package store

import (
	"fmt"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestArtifact_ContentAndInventory(t *testing.T) {
	t.Parallel()
	for _, contentFirst := range []bool{false, true} {
		for _, clearContentFirst := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("content_first=%t/clear_content_first=%t", contentFirst, clearContentFirst),
				func(t *testing.T) {
					t.Parallel()
					testArtifactContentAndInventoryCase(
						t,
						&testArtifactContentAndInventoryCaseInputs{
							contentFirst:      &contentFirst,
							clearContentFirst: &clearContentFirst,
						},
					)
				},
			)
		}
	}
}

func TestReportHidden_RepresentativeProvenance(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindHelmChart, manifest.KindOCIRepository, manifest.KindConfigMap} {
		for _, generated := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/generated=%t", kind, generated), func(t *testing.T) {
				t.Parallel()
				testReportHiddenRepresentativeProvenanceCase(
					t,
					&testReportHiddenRepresentativeProvenanceCaseInputs{kind: &kind, generated: &generated},
				)
			},
			)
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
		var previous ReadSet
		previous.Init(s, owner.Named(), nil)
		previous.Lookup(child.Named())
		_, _, inventory := s.GetEmission(owner.Named())
		inventory.Children = []Child{{Obj: child, Generated: generated}}
		s.RecordEmitted(owner.Named(), &inventory, nil)
		inventory.Finish()
		valid := previous.Valid()
		previous.Retire()
		if valid {
			t.Fatal("provenance transition did not invalidate the captured policy")
		}
		obj, policy := s.Lookup(child.Named())
		want := (Child{Obj: child, Generated: generated}).Policy(child.Named())
		if obj != child || policy != want || s.ReportHidden(child.Named()) != generated {
			t.Fatal("equal child content bypassed its provenance transition")
		}
		var reads ReadSet
		reads.Init(s, owner.Named(), nil)
		captured, capturedPolicy := reads.Lookup(child.Named())
		reads.Retire()
		if captured != obj || capturedPolicy != policy {
			t.Fatal("captured representative and policy disagree")
		}
	}
}

func TestReadSet_CloseRetiresSourceAndInventory(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.GitRepository{Name: "producer", Namespace: "apps"}
	contentInput := manifest.NewConfigMap(
		manifest.NamedResource{Name: "content-input", Namespace: "apps"},
		map[string]any{"VALUE": "first"},
	)
	inventoryInput := manifest.NewConfigMap(
		manifest.NamedResource{Name: "inventory-input", Namespace: "apps"},
		map[string]any{"VALUE": "first"},
	)
	st.AddObjects([]manifest.BaseManifest{owner, contentInput, inventoryInput})
	var contentReads, inventoryReads ReadSet
	contentReads.Init(st, owner.Named(), nil)
	inventoryReads.Init(st, owner.Named(), nil)
	defer contentReads.Retire()
	defer inventoryReads.Retire()
	contentReads.GetObject(contentInput.Named())
	inventoryReads.GetObject(inventoryInput.Named())
	st.SetArtifact(
		owner.Named(),
		&SourceArtifact{Reads: &contentReads, Content: "source-generation", FS: fstest.MapFS{}},
	)
	_, _, output := inventoryReads.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []Child{emissionChild("generated", "value")}, true
	inventoryReads.Bind(&output)
	st.RecordEmitted(owner.Named(), &output, nil)
	st.Retire(&contentReads)
	st.Retire(&inventoryReads)
	assert.FatalIf(
		t,
		contentReads.retired.Load() || inventoryReads.retired.Load(),
		"%s",
		"retirement discarded a retained output",
	)
	var closing ReadSet
	closing.Init(st, owner.Named(), nil)
	closing.Close()
	assert.FatalIf(
		t,
		!contentReads.retired.Load() || !inventoryReads.retired.Load(),
		"%s",
		"close left an independently retained output registered",
	)
	for _, input := range []*manifest.ConfigMap{contentInput, inventoryInput} {
		st.AddObject(manifest.NewConfigMap(input.Named(), map[string]any{"VALUE": "second"}))
	}
	{
		ids := slices.Collect(st.Sweep())
		assert.FatalIf(t, slices.Contains(ids, owner.Named()), "closed outputs scheduled their owner: %v", ids)
	}
}

type testArtifactContentAndInventoryCaseInputs struct {
	contentFirst      *bool
	clearContentFirst *bool
}

func testArtifactContentAndInventoryCase(t *testing.T, testCaseInput *testArtifactContentAndInventoryCaseInputs) {
	t.Helper()

	s := New()
	owner := &manifest.GitRepository{Name: "producer", Namespace: "apps"}
	dependency := manifest.NewConfigMap(
		manifest.NamedResource{Name: "input", Namespace: "apps"},
		map[string]any{"key": "one"},
	)
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
	if *testCaseInput.contentFirst {
		s.SetArtifact(owner.Named(), content)
	}
	s.RecordEmitted(owner.Named(), &inventory, nil)
	if !(*testCaseInput.contentFirst) {
		s.SetArtifact(owner.Named(), content)
	}
	s.Retire(&first)
	var captured ReadSet
	captured.Init(s, manifest.NamedResource{Kind: "Observer", Name: "observer"}, nil)
	defer captured.Close()
	if got := captured.GetArtifact(
		owner.Named(),
		ArtifactContent,
	); got != content || live != content ||
		first.retired.Load() {
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
	{
		output, ok := current.Replay(
			"rendered",
		)
		assert.FatalIf(t, !ok || len(output.Children) != 1 || replay != content ||
			!s.ReportHidden(child.Obj.Named()), "%s", "content publication masked inventory or generated provenance")
	}
	clearContent := func() { s.SetArtifact(owner.Named(), nil) }
	clearInventory := func() {
		_, _, empty := second.GetEmission(owner.Named())
		defer empty.Finish()
		empty.Children, empty.Manifests, empty.Complete = nil, nil, true
		second.Bind(&empty)
		s.RecordEmitted(owner.Named(), &empty, nil)
	}
	if *testCaseInput.clearContentFirst {
		clearContent()
		{
			_, ok := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
			assert.FatalIf(t, !ok ||
				first.retired.Load(), "%s", "clearing content discarded retained inventory or retired its reads")
		}
		clearInventory()
	} else {
		clearInventory()
		assert.FatalIf(
			t,
			s.GetArtifact(owner.Named(), ArtifactContent) != content || first.retired.Load(),
			"%s",
			"replacing inventory discarded retained content or retired its reads",
		)
		clearContent()
	}
	assert.FatalIf(
		t,
		!first.retired.Load() || !second.Valid() || s.GetObject(child.Obj.Named()) != nil,
		"%s",
		"replaced output registrations or generated membership survived",
	)
	s.AddObject(manifest.NewConfigMap(dependency.Named(), map[string]any{"key": "two"}))
	{
		ids := slices.Collect(s.Sweep())
		assert.FatalIf(
			t,
			slices.Contains(ids, owner.Named()),
			"retired output scheduled its former owner: %v",
			ids,
		)
	}
	assert.FatalIf(t, captured.Valid(), "%s", "clearing content did not invalidate a captured content reader")
}

type testReportHiddenRepresentativeProvenanceCaseInputs struct {
	kind      *string
	generated *bool
}

func testReportHiddenRepresentativeProvenanceCase(
	t *testing.T,
	testCaseInput *testReportHiddenRepresentativeProvenanceCaseInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      (*testCaseInput.kind),
		Namespace: "apps",
		Name:      "child",
	}
	child := &manifest.RawObject{
		Kind:       (*testCaseInput.kind),
		APIVersion: manifest.SourceDomain + "/v1",
		Name:       id.Name,
		Namespace:  id.Namespace,
	}
	_, _, inventory := s.GetEmission(owner.Named())
	defer inventory.Finish()
	inventory.Children = []Child{{Obj: child, Generated: (*testCaseInput.generated)}}
	s.RecordEmitted(owner.Named(), &inventory, nil)
	assert.FatalIf(
		t,
		s.ReportHidden(id) != (*testCaseInput.generated) || !s.ReportHidden(manifest.BootstrapSourceID),
		"%s",
		"report visibility did not follow generated provenance",
	)
	{
		info, hidden := s.ReportStatus(id)
		assert.FatalIf(
			t,
			info.Status != "" || hidden != (*testCaseInput.generated),
			"unreported status = %+v hidden=%t",
			info,
			hidden,
		)
	}
	s.UpdateStatus(id, StatusInfo{Status: StatusReady})
	{
		info, hidden := s.ReportStatus(id)
		assert.FatalIf(
			t,
			info.Status != StatusReady || hidden != (*testCaseInput.generated),
			"reported status = %+v hidden=%t",
			info,
			hidden,
		)
	}
	s.AddObject(child)
	_, hidden := s.ReportStatus(id)
	assert.FatalIf(
		t,
		s.ReportHidden(id) || hidden,
		"%s",
		"independent representative inherited auxiliary provenance",
	)
	s.DeleteObject(id)
	_, hidden = s.ReportStatus(id)
	assert.FatalIf(
		t,
		s.ReportHidden(id) != (*testCaseInput.generated) || hidden != (*testCaseInput.generated),
		"%s",
		"removing the independent representative lost emitted provenance",
	)
}
