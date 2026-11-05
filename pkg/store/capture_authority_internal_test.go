package store

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func TestCapture_RawSupportExcludesReadiness(t *testing.T) {
	t.Parallel()
	st := New()
	owner := ownerID("producer")
	source := manifest.BootstrapSourceID
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	st.SetArtifact(source, &SourceArtifact{Key: "content"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		id: {
			Productions: []manifest.Production{
				{Owner: owner, Source: source, Authored: id, File: "settings.yaml", Path: "/settings.yaml"},
			},
		},
	}})
	st.UpdateStatus(owner, StatusInfo{Status: StatusPending, Reason: "", Message: "pending"})
	var reads ReadSet
	reads.Init(st, ownerID("consumer"), nil)
	for range reads.Promotions(id, nil) {
	}
	reads.GetStatus(owner)
	reads.Admit(
		Child{Obj: &manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "raw"}}},
		source,
		owner,
	)
	st.UpdateStatus(owner, StatusInfo{Status: StatusFailed, Reason: "", Message: "render failed"})
	assert.FatalIf(
		t,
		reads.Valid() || st.GetObject(id) == nil,
		"%s",
		"readiness transition must invalidate its reader without withdrawing source bytes",
	)
	st.RecordEmitted(owner, &Emission{Complete: true}, nil)
	assert.FatalIf(t, st.GetObject(id) != nil, "%s", "producer replacement retained unsupported source bytes")
}

func TestReadSet_DeclarationAuthority(t *testing.T) {
	t.Parallel()
	t.Run("reuse after authority changes", func(t *testing.T) {
		t.Parallel()
		s := New()
		owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
		s.AddObject(owner)
		supplied := &manifest.OCIRepository{Name: "input", Namespace: "apps", URL: "oci://supplied.example"}
		s.AddDeclaration(Declaration{Obj: supplied, Origin: manifest.SuppliedDeclaration})
		replacement := &manifest.OCIRepository{
			Name:      supplied.Name,
			Namespace: supplied.Namespace,
			URL:       "oci://repository.example",
		}
		latest := &manifest.OCIRepository{
			Name:      supplied.Name,
			Namespace: supplied.Namespace,
			URL:       "oci://latest.example",
		}
		var reads ReadSet
		reads.Init(s, ownerID("reused"), nil)
		t.Cleanup(reads.Retire)
		var current manifest.BaseManifest = supplied
		for _, next := range []struct {
			object manifest.BaseManifest
			change func()
		}{
			{replacement, func() {
				s.RecordEmitted(owner.Named(), &Emission{Children: []Child{{Obj: replacement}}, Complete: true}, nil)
			}},
			{latest, func() {
				s.RecordEmitted(owner.Named(), &Emission{Children: []Child{{Obj: latest}}, Complete: true}, nil)
			}},
			{supplied, func() { s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil) }},
		} {
			for range 2 {
				object, policy := reads.Lookup(supplied.Named())
				assert.Equal(t, object, current)
				assert.Equal(t, policy, s.Policy(supplied.Named()))
			}
			captured := reads.input(supplied.Named())
			next.change()
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, reads.GetObject(supplied.Named()), current)
			assert.Equal(t, reads.input(supplied.Named()), captured)
			reads.Retire()
			for range s.Sweep() {
			}
			reads.Init(s, ownerID("reused"), nil)
			assert.Equal(t, reads.last == nil, true)
			assert.Equal(t, reads.GetDeclaration(supplied.Named()), next.object)
			assert.Equal(t, reads.Valid(), true)
			current = next.object
		}
		assert.Equal(t, reads.Policy(supplied.Named()).Origin, manifest.SuppliedDeclaration)
	})
	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	supplier := &manifest.Kustomization{Name: "supplier", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "covered", Namespace: "apps", URL: "oci://same.example"}
	s.AddObjects([]manifest.BaseManifest{owner, supplier, raw})
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
			},
		},
	)
	var pending ReadSet
	pending.Init(s, ownerID("waiting"), nil)
	t.Cleanup(pending.Retire)

	assert.FatalIf(
		t,
		pending.GetDeclaration(raw.Named()) != nil || pending.GetObject(raw.Named()) != raw,
		"%s",
		"raw visibility supplied authority",
	)

	s.RecordEmitted(supplier.Named(), &Emission{Children: []Child{{Obj: raw}}, Complete: true}, nil)
	invalidated := slices.Collect(s.Sweep())
	assert.FatalIf(
		t,
		!slices.Contains(invalidated, pending.ID()) || pending.GetDeclaration(raw.Named()) != nil,
		"%s",
		"arrival failed to wake or changed the first captured view",
	)
	var accepted ReadSet
	accepted.Init(s, ownerID("accepted"), nil)
	t.Cleanup(accepted.Retire)

	assert.FatalIf(
		t,
		accepted.GetDeclaration(raw.Named()) != raw || accepted.Matches(&pending),
		"%s",
		"same bytes concealed authority arrival",
	)
	assert.Equal(t, compareCapturedContent(t, &accepted, &pending, raw.Named()), false)

	s.RecordEmitted(supplier.Named(), &Emission{Complete: true}, nil)
	assert.FatalIf(
		t,
		accepted.Valid() || s.GetObject(raw.Named()) != raw || s.GetDeclaration(raw.Named()) != nil,
		"%s",
		"withdrawal lost raw identity or retained authority",
	)
	var withdrawn ReadSet
	withdrawn.Init(s, ownerID("withdrawn"), nil)
	t.Cleanup(withdrawn.Retire)

	assert.FatalIf(
		t,
		withdrawn.Matches(&accepted) || pending.Valid(),
		"%s",
		"same bytes concealed withdrawal or reused authority after ABA",
	)
	assert.Equal(t, compareCapturedContent(t, &withdrawn, &accepted, raw.Named()), false)

	s.RecordEmitted(supplier.Named(), &Emission{Children: []Child{{Obj: raw}}, Complete: true}, nil)
	var supplied ReadSet
	supplied.Init(s, ownerID("supplied"), nil)
	t.Cleanup(supplied.Retire)
	supplied.GetDeclaration(raw.Named())
	s.AddObject(raw)
	assert.FatalIf(t, supplied.Valid(), "%s", "rooting failed to invalidate the supplier capture")
	var rooted ReadSet
	rooted.Init(s, ownerID("rooted"), nil)
	t.Cleanup(rooted.Retire)

	assert.FatalIf(
		t,
		!rooted.Matches(&supplied) || rooted.GetDeclaration(raw.Named()) != raw,
		"%s",
		"equal effective content could not rebind rooted authority",
	)
	assert.Equal(t, compareCapturedContent(t, &rooted, &supplied, raw.Named()), true)
}

func TestReadSet_BoundDeclarationAuthority(t *testing.T) {
	t.Parallel()
	for _, support := range []string{
		"unindexed", "no_productions", "independent", "self", "covered", "self builder", "self external builder",
		"classified external builder", "preclassified external builder", "independent external builder",
		"self unchanged external builder", "classified unchanged external builder", "preclassified unchanged external builder",
		"transformed external builder", "projected external builder",
	} {
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
	assert.FatalIf(
		t,
		reads.GetDeclaration(source.Named()) != nil,
		"%s",
		"raw source acquired declaration authority before its failed parent emitted it",
	)
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
	assert.FatalIf(t, after.Matches(&before), "%s", "matching reclassified the immutable pre-index declaration")
	assert.Equal(t, before.GetDeclaration(raw.Named()), manifest.BaseManifest(raw))
	assert.Equal(t, after.GetDeclaration(raw.Named()), nil)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{})
	assert.Equal(t, st.GetDeclaration(raw.Named()), nil)
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
	proofs := make([]manifest.Production, 1, 2)
	proofs[0] = manifest.Production{Owner: owner.Named()}
	switch *testCaseInput {
	case "no_productions":
		proofs = nil
	case "independent", "independent external builder":
		proofs = append(proofs, manifest.Production{})
	case "self", "self builder", "self external builder", "classified external builder",
		"preclassified external builder", "self unchanged external builder", "classified unchanged external builder",
		"preclassified unchanged external builder", "transformed external builder", "projected external builder":
		proofs = append(proofs, manifest.Production{Owner: raw.Obj.Named()})
	}
	builder := manifest.NamedResource{}
	switch *testCaseInput {
	case "self builder":
		builder = raw.Obj.Named()
	case "self external builder", "classified external builder", "preclassified external builder",
		"independent external builder", "self unchanged external builder", "classified unchanged external builder",
		"preclassified unchanged external builder", "transformed external builder", "projected external builder":
		builder = owner.Named()
	}
	var admission ReadSet
	if *testCaseInput == "classified external builder" || *testCaseInput == "preclassified external builder" ||
		*testCaseInput == "classified unchanged external builder" || *testCaseInput == "preclassified unchanged external builder" {
		st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Key: "source"})
		admission.Init(st, owner.Named(), nil)
		defer admission.Retire()
	}
	if *testCaseInput == "preclassified external builder" ||
		*testCaseInput == "preclassified unchanged external builder" {
		admission.Admit(
			Child{Obj: raw.Obj, Doc: raw.Doc, SourceSupport: manifest.BootstrapSourceID},
			manifest.BootstrapSourceID, manifest.NamedResource{},
		)
	}
	record := manifest.DiscoveryRecord{Productions: proofs, BuilderOwner: builder}
	switch *testCaseInput {
	case "self unchanged external builder",
		"classified unchanged external builder",
		"preclassified unchanged external builder",
		"transformed external builder",
		"projected external builder":
		record.Authored, record.File, record.Builder = raw.Obj.Named(), "input.yaml", "kustomization.yaml"
	}
	switch *testCaseInput {
	case "transformed external builder":
		record.File = record.Builder
	case "projected external builder":
		record.Authored = owner.Named()
	}
	if (*testCaseInput) != "unindexed" {
		st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			raw.Obj.Named(): record,
		}})
	}
	if *testCaseInput == "classified external builder" || *testCaseInput == "classified unchanged external builder" {
		admission.Admit(
			Child{Obj: raw.Obj, Doc: raw.Doc, SourceSupport: manifest.BootstrapSourceID},
			manifest.BootstrapSourceID, manifest.NamedResource{},
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
	switch *testCaseInput {
	case "covered", "self external builder", "classified external builder", "preclassified external builder",
		"transformed external builder", "projected external builder":
	default:
		want = prospective.Obj
	}
	assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), want)
	assert.Equal(t, st.GetObject(raw.Obj.Named()), raw.Obj)
	view := reads.input(raw.Obj.Named()).view
	assert.FatalIf(t, view.accepted || view.owner != nil, "%s", "prospective child manufactured accepted authority")
	st.RecordEmitted(owner.Named(), &next, nil)
	assert.Equal(t, st.GetDeclaration(raw.Obj.Named()), prospective.Obj)
	assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), want)
}

func TestReadSet_MatchesContentDeclaration(t *testing.T) {
	t.Parallel()
	st := New()
	first := &manifest.Kustomization{Name: "input", Namespace: "apps", Path: "before"}
	st.AddObject(first)
	var previous, objectOnly, current ReadSet
	previous.Init(st, ownerID("before"), nil)
	objectOnly.Init(st, ownerID("object-only"), nil)
	current.Init(st, ownerID("current"), nil)
	t.Cleanup(previous.Retire)
	t.Cleanup(objectOnly.Retire)
	t.Cleanup(current.Retire)
	id := first.Named()
	previous.GetDeclaration(id)
	objectOnly.GetObject(id)
	assert.Equal(t, compareCapturedContent(t, &previous, &objectOnly, id), false)
	assert.Equal(t, compareCapturedContent(t, &objectOnly, &previous, id), true)
	second := first.Clone()
	second.Path = "after"
	st.AddObject(second)
	current.GetDeclaration(id)
	assert.Equal(t, compareCapturedContent(t, &current, &previous, id), false)
}

func TestReadSet_MatchesContentRetired(t *testing.T) {
	t.Parallel()
	st := New()
	child := emissionChild("input", "same")
	id := child.Obj.Named()
	st.AddObject(child.Obj, child.Doc)
	st.SetArtifact(id, &SourceArtifact{Content: "same"})
	st.UpdateStatus(id, StatusInfo{Status: StatusPending})
	var previous, current ReadSet
	previous.Init(st, ownerID("before"), nil)
	current.Init(st, ownerID("current"), nil)
	t.Cleanup(previous.Retire)
	t.Cleanup(current.Retire)
	capture := func(reads *ReadSet) {
		reads.GetDeclaration(id)
		reads.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin)
		reads.Productions(id, nil)
		reads.ListObjects(id.Group, id.Kind)
		reads.GetStatus(id)
	}
	capture(&previous)
	st.UpdateStatus(id, StatusInfo{Status: StatusFailed, Message: "readiness changed"})
	assert.Equal(t, previous.Valid(), false)
	capture(&current)
	tasks := task.NewBounded(2)
	retiring, swept := make(chan struct{}), make(chan struct{})
	var matches [2]bool
	tasks.Go(t.Context(), "retire", func(context.Context) {
		close(retiring)
		previous.Retire()
		for range st.Sweep() {
		}
		close(swept)
	})
	tasks.Go(t.Context(), "compare", func(context.Context) {
		tasks.YieldSlot(func() { <-retiring })
		matches[0] = current.MatchesContent(&previous, id)
		tasks.YieldSlot(func() { <-swept })
		matches[1] = current.MatchesContent(&previous, id)
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), int64(0))
	assert.Equal(t, matches, [2]bool{true, true})
	assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
	key := manifest.NamedResource{Group: id.Group, Kind: id.Kind}
	assert.Equal(t, compareCapturedContent(t, &current, &previous, key), true)
	for input := previous.inputs; input != nil; input = input.next {
		for kind := objectFacet; kind <= bucketFacet; kind++ {
			if cell := input.cell(kind); cell != nil && cell.head != nil {
				for active := cell.head.first.Load(); active != nil; active = active.next.Load() {
					assert.Equal(t, active.owner == &previous, false)
				}
			}
		}
	}
	st.AddObject(emissionChild("input", "changed").Obj)
	assert.Equal(t, current.Valid(), false)
	assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
	var replacement ReadSet
	replacement.Init(st, ownerID("replacement"), nil)
	t.Cleanup(replacement.Retire)
	replacement.GetDeclaration(id)
	assert.Equal(t, compareCapturedContent(t, &replacement, &previous, id), false)
}

func TestRetire_AcceptedBorrowers(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"binding", "reconciliation", "source", "emission"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := New()
			owner := &manifest.GitRepository{Name: "owner", Namespace: "apps"}
			child := emissionChild("input", "before")
			st.AddObjects([]manifest.BaseManifest{owner, child.Obj})
			var reads ReadSet
			reads.Init(st, owner.Named(), nil)
			t.Cleanup(reads.Close)
			reads.GetObject(child.Obj.Named())
			switch name {
			case "binding":
				reads.Accept(AssessmentEntryBinding)
			case "reconciliation":
				reads.Accept(AssessmentReconciled)
			case "source":
				st.SetArtifact(owner.Named(), &SourceArtifact{Content: "same", Reads: &reads})
			case "emission":
				_, _, output := reads.GetEmission(owner.Named())
				output.Complete = true
				reads.Bind(&output)
				st.RecordEmitted(owner.Named(), &output, nil)
				output.Finish()
			}
			st.Retire(&reads)
			for range st.Sweep() {
			}
			assert.Equal(t, reads.retired.Load(), false)
			assert.Equal(t, reads.Valid(), true)
			st.AddObject(emissionChild("input", "after").Obj)
			assert.Equal(t, reads.Valid(), false)
			reads.Close()
			assert.Equal(t, reads.retired.Load(), true)
		})
	}
}

func TestReadSet_MatchesContentAbsentDeclaration(t *testing.T) {
	t.Parallel()
	st := New()
	id := ownerID("absent")
	var previous, current, objectOnly ReadSet
	previous.Init(st, ownerID("before"), nil)
	current.Init(st, ownerID("current"), nil)
	objectOnly.Init(st, ownerID("object-only"), nil)
	t.Cleanup(previous.Retire)
	t.Cleanup(current.Retire)
	t.Cleanup(objectOnly.Retire)
	assert.Equal(t, previous.GetDeclaration(id), nil)
	assert.Equal(t, current.GetDeclaration(id), nil)
	assert.Equal(t, objectOnly.GetObject(id), nil)
	assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
	assert.Equal(t, compareCapturedContent(t, &current, &objectOnly, id), false)
	st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
}
