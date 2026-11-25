package store

import (
	"cmp"
	"fmt"
	"strconv"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil/assert"
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
	for _, path := range []struct {
		name  string
		bound bool
	}{{name: "direct"}, {name: "bound", bound: true}} {
		t.Run("admission after omission snapshot/"+path.name, func(t *testing.T) {
			t.Parallel()
			testPublicationAdmissionRace(t, path.bound)
		})
	}
}

func testPublicationAdmissionRace(t *testing.T, bound bool) {
	t.Helper()
	s := New()
	source := manifest.BootstrapSourceID
	owner := &manifest.Kustomization{
		Name: "parent", Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: source.Kind, Name: source.Name, Namespace: source.Namespace,
		},
	}
	raw := emissionChild("child", "raw")
	raw.SourceSupport = source
	id, bare := raw.Obj.Named(), raw.Obj.Named()
	bare.Namespace = ""
	s.AddObject(owner)
	s.SetArtifact(source, &SourceArtifact{Content: "first", FS: fstest.MapFS{}})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {Productions: []manifest.Production{{
			Owner: owner.Named(), Source: source, Authored: bare, Path: "child.yaml", File: "child.yaml",
		}}},
	}})

	_, _, output := s.GetEmission(owner.Named())
	defer output.Finish()
	output.Complete = true
	if bound {
		var publication ReadSet
		publication.Init(s, owner.Named(), nil)
		defer publication.Retire()
		publication.Bind(&output)
	}
	output.origin = owner.Named()
	_, prev, generation := snapshotEmission(s.shardFor(owner.Named()), owner.Named())
	proposal := prepareEmission(&output, prev, nil, nil)
	targets := s.prepareReplacement(prev, proposal, nil)
	_, found := targets.region.position(id)
	assert.Equal(t, found, false)

	admitted := make(chan struct{})
	go func() {
		defer close(admitted)
		var reads ReadSet
		reads.Init(s, manifest.NamedResource{}, nil)
		defer reads.Retire()
		reads.Admit(raw, source, manifest.NamedResource{})
	}()
	<-admitted
	assert.Equal(t, s.GetDeclaration(id), raw.Obj)
	assert.Equal(t, proposal.Reads.Valid(), true)
	result := s.commitReplacement(prev, proposal, generation, &targets)
	assert.FatalIf(t, result != publicationRetry, "stale omission publication returned %v, want retry", result)
	assert.Equal(t, s.GetDeclaration(id), raw.Obj)

	s.RecordEmitted(owner.Named(), &output, nil)
	assert.Equal(t, output.accepted, true)
	assert.Equal(t, s.SourceAdmission(id, source), false)
	assert.Equal(t, s.GetDeclaration(id), nil)
	assert.Equal(t, s.shardFor(id).cells[id].retired, true)
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

type independentCoverageCase struct {
	name                               string
	source                             manifest.NamedResource
	admit, explicit, provisional, want bool
	republishAuthored                  bool
	bareCover, bareIndependent         bool
	bound, bareSelf                    bool
}

func TestSourceAdmission_IndependentCoverage(t *testing.T) {
	t.Parallel()
	otherSource := (&manifest.GitRepository{Name: "other", Namespace: "apps"}).Named()
	for _, tc := range []independentCoverageCase{
		{name: "publication preserves support", admit: true, want: true},
		{name: "omission permits first admission", want: true},
		{name: "inherited namespace permits first admission", bareCover: true, bareIndependent: true, want: true},
		{name: "exact coverage preserves inherited declaration", admit: true, bareIndependent: true, want: true},
		{name: "inherited omission from direct proposal", admit: true, bareCover: true},
		{name: "inherited omission from bound proposal", admit: true, bareCover: true, bound: true},
		{
			name:  "bare authored support clears provisional bootstrap",
			admit: true, provisional: true, bareIndependent: true, want: true,
		},
		{name: "bare self-owner preserves support", admit: true, bareIndependent: true, bareSelf: true, want: true},
		{name: "explicit declaration takes precedence", admit: true, explicit: true},
		{name: "omitted explicit declaration prevents admission", explicit: true},
		{name: "provisional bootstrap loses support", admit: true, provisional: true},
		{name: "other checkout loses published support", admit: true, source: otherSource, republishAuthored: true},
		{name: "other checkout prevents first admission", source: otherSource},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testIndependentCoverage(t, &tc)
		})
	}
}

func testIndependentCoverage(t *testing.T, tc *independentCoverageCase) {
	t.Helper()
	s := New()
	authoredSource := manifest.BootstrapSourceID
	source := cmp.Or(tc.source, authoredSource)
	owner := &manifest.Kustomization{
		Name: "supplier", Namespace: "apps", Path: "supplier",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: source.Kind, Name: source.Name, Namespace: source.Namespace,
		},
	}
	raw := emissionChild("child", "raw")
	id := raw.Obj.Named()
	s.AddObject(owner)
	s.SetArtifact(authoredSource, &SourceArtifact{Key: "authored", Content: "head"})
	s.SetArtifact(source, &SourceArtifact{Key: "checkout", Content: "head"})
	records, different := tc.discoveryRecords(id, owner.Named(), source)
	inheritedOmission := different.Obj != nil
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
	switch {
	case inheritedOmission:
		bare := id
		bare.Namespace = ""
		_, hasBareCell := s.shardFor(bare).cells[bare]
		assert.Equal(t, hasBareCell, false)
		assert.Equal(t, len(s.Productions(id, nil)), 1)
	case tc.bareIndependent:
		want := 2
		if tc.provisional {
			want++
		}
		assert.Equal(t, len(s.Productions(id, nil)), want)
	default:
		assert.Equal(t, RawDeclarationAuthority(s.index.Load(), id, false), true)
	}
	assert.Equal(t, s.SourceAdmission(id, source), true)
	admit := func(declaration Child, source manifest.NamedResource) {
		declaration.SourceSupport = source
		var reads ReadSet
		reads.Init(s, manifest.NamedResource{}, nil)
		defer reads.Retire()
		reads.Admit(declaration, source, manifest.NamedResource{})
	}
	if tc.admit {
		admit(raw, source)
		assert.Equal(t, s.GetDeclaration(id), raw.Obj)
	}
	if inheritedOmission {
		admit(different, source)
		assert.Equal(t, s.GetDeclaration(different.Obj.Named()), different.Obj)
	}

	_, _, output := s.GetEmission(owner.Named())
	defer output.Finish()
	output.Complete = true
	if tc.bound {
		var publication ReadSet
		publication.Init(s, owner.Named(), nil)
		defer publication.Retire()
		publication.Bind(&output)
	}
	s.RecordEmitted(owner.Named(), &output, nil)
	assert.Equal(t, output.accepted, true)
	assert.Equal(t, s.SourceAdmission(id, source), tc.want)
	assert.Equal(t, s.SourceAdmission(id, authoredSource), tc.want || source != authoredSource)
	var wantDeclaration manifest.BaseManifest
	if tc.want {
		wantDeclaration = raw.Obj
	}
	if tc.admit {
		assert.Equal(t, s.shardFor(id).cells[id].retired, !tc.want)
		assert.Equal(t, s.GetDeclaration(id), wantDeclaration)
	}
	if inheritedOmission {
		assert.Equal(t, s.GetDeclaration(different.Obj.Named()), different.Obj)
		assert.Equal(t, s.SourceAdmission(different.Obj.Named(), source), true)
		assert.Equal(t, s.shardFor(different.Obj.Named()).cells[different.Obj.Named()].retired, false)
	}
	admit(raw, source)
	assert.Equal(t, s.GetDeclaration(id), wantDeclaration)
	if tc.republishAuthored {
		admit(raw, authoredSource)
		assert.Equal(t, s.GetDeclaration(id), raw.Obj)
		assert.Equal(t, s.SourceAdmission(id, authoredSource), true)
		assert.Equal(t, s.SourceAdmission(id, source), false)
	}
	if tc.explicit {
		effective := emissionChild("child", "emitted")
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{effective}, Complete: true}, nil)
		assert.Equal(t, s.GetDeclaration(id), effective.Obj)
	}
}

func (tc *independentCoverageCase) discoveryRecords(
	id, owner, source manifest.NamedResource,
) (map[manifest.NamedResource]manifest.DiscoveryRecord, Child) {
	covered := manifest.Production{
		Owner:    owner,
		Source:   source,
		Authored: id,
		Path:     "supplier/child.yaml",
		File:     "supplier/child.yaml",
	}
	independent := manifest.Production{
		Source: manifest.BootstrapSourceID, Authored: id, Path: covered.Path, File: covered.File,
	}
	if tc.explicit {
		covered.Declaration = id
	}
	bare := id
	bare.Namespace = ""
	coverID, independentID := id, id
	if tc.bareCover || tc.bareIndependent {
		covered.Authored, independent.Authored = bare, bare
	}
	if tc.bareIndependent {
		independentID = bare
	}
	if tc.bareSelf {
		independent.Owner = bare
	}
	if tc.bareCover {
		coverID = bare
	}
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		coverID: {Productions: []manifest.Production{covered}},
	}
	if tc.provisional {
		record := records[id]
		record.Productions = append(record.Productions, manifest.Production{Source: source})
		records[id] = record
	}
	var different Child
	if tc.bareCover && !tc.bareIndependent {
		different = emissionChild("child", "different file")
		different.Obj = different.Obj.WithNamespace("other")
		different.Doc["metadata"].(map[string]any)["namespace"] = "other"
		records[different.Obj.Named()] = manifest.DiscoveryRecord{Productions: []manifest.Production{{
			Source: source, Authored: bare, Path: "other/child.yaml", File: "other/child.yaml",
		}}}
	} else if !tc.provisional || tc.bareIndependent {
		record := records[independentID]
		record.Productions = append(record.Productions, independent)
		records[independentID] = record
	}
	return records, different
}
