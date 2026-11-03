package store

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

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
	{
		assert.FatalIf(t, self.Valid(), "%s", "self-publication exemption ignored replacement of the declaration")
	}
}

func TestReadSet_BoundDeclarationAuthority(t *testing.T) {
	t.Parallel()
	for _, support := range []string{"unindexed", "no_productions", "independent", "self", "covered"} {
		t.Run(support, func(t *testing.T) {
			t.Parallel()
			testReadSetBoundDeclarationAuthorityCase(
				t,
				&support,
			)
		},
		)
	}
}

func TestReadSet_RawOwnedSourceMaterializer(t *testing.T) {
	t.Parallel()
	st := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	source := &manifest.HelmRepository{Name: "chart", Namespace: "apps", URL: "https://charts.example"}
	st.AddObjects([]manifest.BaseManifest{parent, source})
	p := manifest.Production{
		Owner:    parent.Named(),
		Source:   manifest.BootstrapSourceID,
		Authored: source.Named(),
		Path:     "/input/source.yaml",
		File:     "source.yaml",
	}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		source.Named(): {Path: p.Path, File: p.File, Authored: p.Authored, Productions: []manifest.Production{p}},
	}})
	st.UpdateStatus(parent.Named(), StatusInfo{Status: StatusFailed, Message: "dependency unavailable"})
	var reads ReadSet
	reads.Init(st, source.Named(), nil)
	defer reads.Retire()
	if got := reads.Materializer(source.Named(), p); got != parent.Named() {
		t.Fatalf("raw owned source materializer=%s, want %s", got, parent.Named())
	}
	{
		assert.FatalIf(
			t,
			reads.GetDeclaration(source.Named()) != nil,
			"%s",
			"raw source acquired declaration authority before its failed parent emitted it",
		)
	}
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

func TestReadSet_DeclarationFreeze(t *testing.T) {
	t.Parallel()
	st := New()
	raw := &manifest.OCIRepository{Name: "covered", Namespace: "apps"}
	st.AddObject(raw)
	var before, after ReadSet
	before.Init(st, ownerID("before"), nil)
	after.Init(st, ownerID("after"), nil)
	defer before.Retire()
	defer after.Retire()
	assert.Equal(t, before.GetDeclaration(raw.Named()), manifest.BaseManifest(raw))
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Named(): {Productions: []manifest.Production{{Owner: ownerID("owner")}}},
			},
		},
	)
	{
		assert.FatalIf(t, after.Matches(&before), "%s", "matching reclassified the immutable pre-index declaration")
	}
	assert.Equal(t, before.GetDeclaration(raw.Named()), manifest.BaseManifest(raw))
	assert.Equal(t, after.GetDeclaration(raw.Named()), nil)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{})
	assert.Equal(t, st.GetDeclaration(raw.Named()), nil)
}

func TestReadRegistration_SelectedArtifact(t *testing.T) {
	t.Parallel()
	for _, emitted := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			read    ArtifactRead
			change  func(*SourceArtifact)
			invalid bool
		}{
			{"ignored", ArtifactContent | ArtifactRevision | ArtifactOrigin, func(a *SourceArtifact) {
				a.URL = "next"
				a.Size = 12
				a.Metadata = map[string]string{"ignored": "next"}
			}, false},
			{"revision-only-content", ArtifactRevision, func(a *SourceArtifact) { a.Content = "next" }, false},
			{"origin-only-revision", ArtifactOrigin, func(a *SourceArtifact) { a.Revision = "next" }, false},
			{"content-only-revision", ArtifactContent, func(a *SourceArtifact) { a.Revision = "next" }, false},
			{"revision", ArtifactContent | ArtifactRevision, func(a *SourceArtifact) { a.Revision = "next" }, true},
			{"content-only-origin", ArtifactContent, func(a *SourceArtifact) { a.Metadata = map[string]string{"org.opencontainers.image.revision": ""} }, false},
			{"origin-presence", ArtifactContent | ArtifactOrigin, func(a *SourceArtifact) { a.Metadata = map[string]string{"org.opencontainers.image.revision": ""} }, true},
			{"content", ArtifactContent, func(a *SourceArtifact) { a.Content = "next" }, true},
			{"union", ArtifactContent | ArtifactRevision | ArtifactOrigin, func(a *SourceArtifact) {
				a.Content = "next"
				a.Revision = "next"
				a.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
			}, true},
		} {
			t.Run(fmt.Sprintf("emitted=%t/%s", emitted, tc.name), func(t *testing.T) {
				t.Parallel()
				testReadRegistrationSelectedArtifactCase(
					t,
					&testReadRegistrationSelectedArtifactCaseInputs{emitted: &emitted, tc: &tc},
				)
			},
			)
		}
	}
	for _, read := range []ArtifactRead{ArtifactRevision, ArtifactOrigin} {
		t.Run(fmt.Sprintf("late-%d", read), func(t *testing.T) {
			t.Parallel()
			testReadRegistrationSelectedArtifactCase2(t, &read)
		},
		)
	}
}

func TestReadRegistration_SelectedDeclaration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		initial, next any
		key           string
		invalid       bool
	}{
		{"uid", "first", "next", "uid", true},
		{"non-string-uid", nil, 3, "uid", false},
		{"disable", nil, "disabled", "annotation", true},
		{"normalized-disable", "disabled", "DiSaBlEd", "annotation", false},
		{"enable", "disabled", nil, "annotation", true},
		{"ignored", nil, "next", "ignored", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			object := &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1",
				Kind:       "ArtifactGenerator",
				Name:       "generator",
				Namespace:  "apps",
			}
			document := func(value any) map[string]any {
				metadata := map[string]any{}
				if tc.key == "annotation" {
					metadata["annotations"] = map[string]any{"source.extensions.fluxcd.io/reconcile": value}
				} else {
					metadata[tc.key] = value
				}
				return map[string]any{"apiVersion": object.APIVersion, "metadata": metadata}
			}
			st.AddObject(object, document(tc.initial))
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Close()
			reads.GetObject(object.Named())
			next := document(tc.next)
			st.AddObject(object, next)
			assert.Equal(t, reads.Valid(), !tc.invalid)
			_, latest, emission := st.GetEmission(object.Named())
			defer emission.Finish()
			assert.Diff(t, latest, next)
		})
	}
}

func TestCapture_SupplierQueryIsolation(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			t.Parallel()
			st := New()
			sourceOwner, queryOwner := ownerID("source-owner"), ownerID("query-owner")
			st.AddObjects(
				[]manifest.BaseManifest{
					&manifest.Kustomization{Name: sourceOwner.Name, Namespace: sourceOwner.Namespace},
					&manifest.Kustomization{Name: queryOwner.Name, Namespace: queryOwner.Namespace},
				},
			)
			source, query := emissionChild("source", "same"), emissionChild("query", "same")
			payload := &SourceArtifact{Key: "family", Content: "generation", FS: fstest.MapFS{}}
			st.RecordEmitted(
				sourceOwner,
				&Emission{Children: []Child{source}, Complete: complete},
				map[manifest.NamedResource]*SourceArtifact{source.Obj.Named(): payload},
			)
			st.RecordEmitted(queryOwner, &Emission{Children: []Child{query}, Complete: complete}, nil)
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Close()
			reads.GetArtifact(source.Obj.Named(), ArtifactContent)
			reads.UnsettledProducers(query.Obj.Named())
			reads.Productions(query.Obj.Named(), nil)
			next := *payload
			next.Metadata = map[string]string{"ignored": "next"}
			st.RecordEmitted(
				sourceOwner,
				&Emission{Children: []Child{source, emissionChild("unrelated", "new")}, Complete: complete},
				map[manifest.NamedResource]*SourceArtifact{source.Obj.Named(): &next},
			)
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
			st.RecordEmitted(
				queryOwner,
				&Emission{
					Children:    []Child{query, emissionChild("query-sibling", "new")},
					Complete:    complete,
					Fingerprint: "query-publication",
				},
				nil,
			)
			assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{reads.ID()})
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestCapture_PartialSupplierInputStamp(t *testing.T) {
	t.Parallel()
	st := New()
	producer := ownerID("producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	sibling := emissionChild("sibling", "same")
	child := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	st.RecordEmitted(producer, &Emission{Children: []Child{sibling}}, nil)
	var reads ReadSet
	reads.Init(st, child.Named(), nil)
	defer reads.Close()
	reads.GetObject(sibling.Obj.Named())
	st.RecordEmitted(producer, &Emission{Children: []Child{sibling, {Obj: child}}}, nil)
	_, _, captured := reads.GetEmission(child.Named())
	defer captured.Finish()
	_, _, current := st.GetEmission(child.Named())
	defer current.Finish()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, captured.input, manifest.BaseManifest(child))
	assert.Equal(t, captured.inputHead, current.inputHead)
	st.RecordEmitted(
		producer,
		&Emission{Children: []Child{sibling, {Obj: child}, emissionChild("unrelated", "new")}},
		nil,
	)
	_, _, repeated := reads.GetEmission(child.Named())
	defer repeated.Finish()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, repeated.inputHead, captured.inputHead)
	assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
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

func TestReadRegistration_StableWithdrawalNoABA(t *testing.T) {
	t.Parallel()
	st := New()
	producer := ownerID("producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	publish := func(child Child) {
		st.RecordEmitted(producer, &Emission{Children: []Child{child}}, nil)
	}
	first, second := emissionChild("input", "first"), emissionChild("input", "second")
	publish(first)
	var earlier, current ReadSet
	earlier.Init(st, ownerID("earlier"), nil)
	defer earlier.Close()
	earlier.GetObject(first.Obj.Named())
	publish(second)
	current.Init(st, ownerID("current"), nil)
	defer current.Close()
	assert.Equal(t, current.GetObject(first.Obj.Named()), second.Obj)
	assert.Equal(t, current.Valid(), true)
	st.RecordEmitted(producer, &Emission{Children: []Child{emissionChild("other", "unrelated")}}, nil)
	assert.Equal(t, current.Valid(), true)
	st.RecordEmitted(producer, &Emission{Complete: true}, nil)
	assert.Equal(t, st.GetObject(first.Obj.Named()), manifest.BaseManifest(nil))
	{
		assert.FatalIf(t, current.Valid(), "%s", "withdrawal reused a captured stable-publication stamp")
	}
	assert.Equal(t, earlier.Valid(), false)
}

func TestReadSet_ProducedArtifact(t *testing.T) {
	t.Parallel()
	for _, consumed := range []bool{false, true} {
		t.Run(strconv.FormatBool(consumed), func(t *testing.T) {
			t.Parallel()
			s := New()
			parent := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(parent)
			child := emissionChild("output", "first")
			id := child.Obj.Named()
			s.AddObject(child.Obj, child.Doc)
			previous := &SourceArtifact{Content: "first", Revision: "first", FS: fstest.MapFS{}}
			s.SetArtifact(id, previous)
			var reads ReadSet
			reads.Init(s, parent.Named(), nil)
			if consumed {
				reads.GetArtifact(id, ArtifactContent)
			}
			_, _, next := reads.GetEmission(parent.Named())
			defer next.Finish()
			next.Children, next.Complete = []Child{child}, true
			reads.Bind(&next)
			{
				got := reads.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin)
				assert.FatalIf(t, got != previous, "prospective output lost the complete descriptor: %v", got)
			}
			s.RecordEmitted(
				parent.Named(),
				&next,
				map[manifest.NamedResource]*SourceArtifact{
					id: {Content: "second", Revision: "second", FS: fstest.MapFS{}},
				},
			)
			if reads.Valid() == consumed {
				t.Fatalf("self-publication validity=%v with prior consumption=%v", reads.Valid(), consumed)
			}
		})
	}
}

func TestCapture_DirectAndNamedReads(t *testing.T) {
	t.Parallel()
	st := New()
	first, other := emissionChild("first", "first"), emissionChild("other", "other")
	st.AddObject(first.Obj)
	st.AddObject(other.Obj)
	var reads ReadSet
	reads.Init(st, ownerID("reader"), nil)
	defer reads.Close()
	input := Input{Ref: manifest.DependencyRef{NamedResource: first.Obj.Named()}}
	assert.Equal(t, reads.Capture(&input), first.Obj)
	assert.Equal(t, reads.last == nil, true)
	assert.Equal(t, reads.GetObject(first.Obj.Named()), first.Obj)
	assert.Equal(t, reads.last == &input, true)
	second := Input{Ref: manifest.DependencyRef{NamedResource: other.Obj.Named()}}
	assert.Equal(t, reads.Capture(&second), other.Obj)
	assert.Equal(t, reads.last == &input, true)
	assert.Equal(t, reads.GetObject(other.Obj.Named()), other.Obj)
	assert.Equal(t, reads.last == &second, true)
	replacement := Input{Ref: manifest.DependencyRef{NamedResource: other.Obj.Named()}}
	assert.Equal(t, reads.Capture(&replacement), other.Obj)
	assert.Equal(t, reads.last == &replacement, true)
	st.AddObject(emissionChild("first", "changed").Obj)
	assert.Equal(t, reads.Valid(), false)
}

func testReadSetBoundDeclarationAuthorityCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	raw := emissionChild("input", "raw")
	prospective := emissionChild("input", "prospective")
	st.AddObjects([]manifest.BaseManifest{owner, raw.Obj})
	proofs := []manifest.Production{{Owner: owner.Named()}}
	switch *testCaseInput {
	case "no_productions":
		proofs = nil
	case "independent":
		proofs = append(proofs, manifest.Production{})
	case "self":
		proofs = append(proofs, manifest.Production{Owner: raw.Obj.Named()})
	}
	if (*testCaseInput) != "unindexed" {
		st.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					raw.Obj.Named(): {Productions: proofs},
				},
			},
		)
	}
	var reads ReadSet
	reads.Init(st, owner.Named(), nil)
	defer reads.Retire()
	_, _, next := reads.GetEmission(owner.Named())
	defer next.Finish()
	next.Children = []Child{prospective}
	reads.Bind(&next)
	assert.Equal(t, reads.GetObject(raw.Obj.Named()), prospective.Obj)
	assert.Diff(t, reads.input(raw.Obj.Named()).view.child.Doc, prospective.Doc)
	var want manifest.BaseManifest
	if (*testCaseInput) != "covered" {
		want = prospective.Obj
	}
	assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), want)
	assert.Equal(t, st.GetObject(raw.Obj.Named()), raw.Obj)
	view := reads.input(raw.Obj.Named()).view
	{
		assert.FatalIf(t, view.accepted || view.owner != nil, "%s", "prospective child manufactured accepted authority")
	}
	st.RecordEmitted(owner.Named(), &next, nil)
	assert.Equal(t, st.GetDeclaration(raw.Obj.Named()), prospective.Obj)
	assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), want)
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
	for i, entry := range reads.produced {
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
	for i, entry := range reads.produced {
		got[i] = entry.child.Obj.Named()
	}
	assert.Diff(t, got, want)
	assert.Equal(t, reads.GetObject(raw.Obj.Named()), prospective.Obj)
	assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), nil)
	assert.Equal(t, st.GetDeclaration(raw.Obj.Named()), published.Obj)
	assert.Equal(t, st.GetObject(owner.Named()), manifest.BaseManifest(owner))
}

type testReadRegistrationSelectedArtifactCaseInputs struct {
	emitted *bool
	tc      *struct {
		name    string
		read    ArtifactRead
		change  func(*SourceArtifact)
		invalid bool
	}
}

func testReadRegistrationSelectedArtifactCase(
	t *testing.T,
	testCaseInput *testReadRegistrationSelectedArtifactCaseInputs,
) {
	t.Helper()

	s := New()
	child := emissionChild("selected", "same")
	producer := ownerID("producer")
	var direct *SourceArtifact
	if *testCaseInput.emitted {
		direct = &SourceArtifact{Key: "direct", Content: "direct", FS: fstest.MapFS{}}
		s.AddObject(child.Obj)
		s.SetArtifact(child.Obj.Named(), direct)
	}
	s.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	publish := func(a *SourceArtifact) {
		if *testCaseInput.emitted {
			s.RecordEmitted(
				producer,
				&Emission{Children: []Child{child}, Complete: true},
				map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): a},
			)
		} else {
			s.SetArtifact(child.Obj.Named(), a)
		}
	}
	first := &SourceArtifact{
		Key:      "family",
		Content:  "generation",
		Revision: "first",
		FS:       fstest.MapFS{"file": {Data: []byte("same")}},
	}
	publish(first)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Close()
	assert.Equal(t, reads.GetArtifact(child.Obj.Named(), testCaseInput.tc.read), Artifact(first))
	next := *first
	testCaseInput.tc.change(&next)
	publish(&next)
	assert.Equal(t, s.GetArtifact(child.Obj.Named(), ArtifactContent), Artifact(&next))
	assert.Equal(t, reads.GetArtifact(child.Obj.Named(), testCaseInput.tc.read), Artifact(first))
	changes := slices.Collect(s.Sweep())
	if testCaseInput.tc.invalid {
		assert.Diff(t, changes, []manifest.NamedResource{reads.ID()})
	} else {
		assert.Equal(t, len(changes), 0)
	}
	assert.Equal(t, reads.Valid(), !testCaseInput.tc.invalid)
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	var replay ReadSet
	replay.Init(s, reads.ID(), nil)
	defer replay.Close()
	assert.Equal(t, replay.Matches(&reads), !testCaseInput.tc.invalid)
	if *testCaseInput.emitted {
		s.RecordEmitted(producer, &Emission{Complete: true}, nil)
		assert.Equal(t, s.GetArtifact(child.Obj.Named(), ArtifactContent), Artifact(direct))
		assert.Equal(t, replay.Valid(), false)
	}
}

func testReadRegistrationSelectedArtifactCase2(
	t *testing.T,
	testCaseInput *ArtifactRead,
) {
	t.Helper()

	s := New()
	id := ownerID("source")
	first := &SourceArtifact{Key: "family", Content: "generation", Revision: "first", FS: fstest.MapFS{}}
	s.SetArtifact(id, first)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Close()
	reads.GetArtifact(id, ArtifactContent)
	next := *first
	next.Revision = "next"
	next.Metadata = map[string]string{"org.opencontainers.image.revision": ""}
	s.SetArtifact(id, &next)
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, reads.GetArtifact(id, ArtifactContent|(*testCaseInput)), Artifact(first))
	assert.Equal(t, reads.Valid(), false)
}
