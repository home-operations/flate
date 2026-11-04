package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestProductions_ProjectedRawFamily(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                         string
		separateFile, separateSource bool
	}{
		{name: "shared file"}, {name: "different file", separateFile: true}, {name: "different source", separateSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			raw := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings"}
			target := raw
			target.Namespace = "apps"
			first := testutil.KustomizationID("apps", "first")
			second := testutil.KustomizationID("other", "second")
			p := manifest.Production{
				Owner:    first,
				Source:   manifest.BootstrapSourceID,
				Authored: raw,
				File:     "settings.yaml",
				Path:     "/settings.yaml",
			}
			other := p
			other.Owner = second
			selected := p
			if tc.separateFile {
				selected.File, selected.Path = "another.yaml", "/another.yaml"
			}
			if tc.separateSource {
				selected.Source = manifest.NamedResource{
					Group: manifest.SourceDomain,
					Kind:  manifest.KindGitRepository,
					Name:  "foreign",
				}
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw: {
					Productions: []manifest.Production{p, other},
				},
				target: {Productions: []manifest.Production{selected}},
			}}
			st.SetDiscoveryIndex(index)
			var reads ReadSet
			reads.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
			got := reads.Productions(target, index)
			want := []manifest.Production{p, other}
			if tc.separateFile || tc.separateSource {
				want = []manifest.Production{selected}
			}
			assert.Diff(t, got, want)
			assert.Diff(t, st.Productions(target, index), want)
		})
	}
}

func TestProductions_SelfPublication(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	child := emissionChild("output", "value")
	s.AddObject(producer)
	production := manifest.Production{Owner: producer.Named(), Source: manifest.BootstrapSourceID}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		producer.Named():  {Productions: []manifest.Production{production}},
		child.Obj.Named(): {Productions: []manifest.Production{production}},
	}})
	var self, observer ReadSet
	self.Init(s, producer.Named(), nil)
	observer.Init(s, ownerID("observer"), nil)
	defer self.Close()
	defer observer.Close()
	assert.Diff(t, self.Productions(producer.Named(), nil), []manifest.Production{production})
	assert.Diff(t, observer.Productions(producer.Named(), nil), []manifest.Production{production})
	_, _, next := self.GetEmission(producer.Named())
	defer next.Finish()
	next.Children, next.Complete = []Child{child}, true
	s.RecordEmitted(producer.Named(), &next, nil)
	if !self.Valid() || observer.Valid() || s.GetObject(child.Obj.Named()) == nil {
		t.Fatalf(
			"publication validity: self=%t observer=%t child=%v",
			self.Valid(),
			observer.Valid(),
			s.GetObject(child.Obj.Named()),
		)
	}
	changed := producer.Clone()
	changed.Path = "changed"
	s.AddObject(changed)
	assert.FatalIf(t, self.Valid(), "%s", "self-publication exemption ignored replacement of the declaration")
}

func TestReadSet_BoundInventoryPublication(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"self_first", "self_last", "borrowed", "remapped"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testReadSetBoundInventoryPublicationCase(t, &mode)
		},
		)
	}
}

func TestReadSet_MatchesProducerAvailability(t *testing.T) {
	t.Parallel()
	st := New()
	producer := ownerID("producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	child := emissionChild("input", "same")
	st.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true}, nil)
	var before, current ReadSet
	before.Init(st, ownerID("before"), nil)
	defer before.Close()
	assert.Equal(t, len(before.UnsettledProducers(child.Obj.Named())), 0)
	st.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true, Fingerprint: "next"}, nil)
	assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{before.ID()})
	assert.Equal(t, before.Valid(), false)
	current.Init(st, ownerID("current"), nil)
	defer current.Close()
	assert.Equal(t, current.Matches(&before), true)
	before.Retire()
	st.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true, Fingerprint: "latest"}, nil)
	assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{current.ID()})
	assert.Equal(t, current.Valid(), false)
}

func testReadSetBoundInventoryPublicationCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	raw, prospective := emissionChild("input", "raw"), emissionChild("input", "prospective")
	st.AddObjects([]manifest.BaseManifest{owner, raw.Obj})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		raw.Obj.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
	}})
	if (*testCaseInput) == "borrowed" {
		st.RecordEmitted(owner.Named(), &Emission{Children: []Child{prospective}, Complete: true}, nil)
	}
	var reads ReadSet
	reads.Init(st, owner.Named(), nil)
	defer reads.Retire()
	_, _, next := reads.GetEmission(owner.Named())
	defer next.Finish()
	children := []Child{{Obj: owner}, prospective}
	if (*testCaseInput) == "self_last" {
		children[0], children[1] = children[1], children[0]
	}
	if (*testCaseInput) != "borrowed" {
		next.Children = children
	}
	reads.Bind(&next)
	want := make([]manifest.NamedResource, len(reads.produced))
	for i := range reads.produced {
		entry := reads.produced[i]
		want[i] = entry.child.Obj.Named()
	}
	assert.Equal(t, next.entriesOwned, (*testCaseInput) != "borrowed")
	published := prospective
	switch *testCaseInput {
	case "borrowed":
		next.Children = children
	case "remapped":
		children = slices.Clone(children)
		published = emissionChild("input", "remapped")
		children[1] = published
		next.Children = children
	}
	next.Complete = true
	st.RecordEmitted(owner.Named(), &next, nil)
	got := make([]manifest.NamedResource, len(reads.produced))
	for i := range reads.produced {
		entry := reads.produced[i]
		got[i] = entry.child.Obj.Named()
	}
	assert.Diff(t, got, want)
	assert.Equal(t, reads.GetObject(raw.Obj.Named()), prospective.Obj)
	assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), nil)
	assert.Equal(t, st.GetDeclaration(raw.Obj.Named()), published.Obj)
	assert.Equal(t, st.GetObject(owner.Named()), manifest.BaseManifest(owner))
}
