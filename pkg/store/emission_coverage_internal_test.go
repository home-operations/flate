package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_ShadowedCoverageAncestorReplacement(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"withdrawn", "accepted-child", "other-source-current"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedShadowedCoverageAncestorReplacementCase(
				t,
				&mode,
			)
		},
		)
	}
}

func testRecordEmittedShadowedCoverageAncestorReplacementCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	s.AddObject(&manifest.GitRepository{Name: source.Name, Namespace: source.Namespace})
	s.SetArtifact(source, &SourceArtifact{Key: "stable", Content: "stable", LocalPath: "source"})
	producer := func(name string, binding manifest.NamedResource) Child {
		t.Helper()
		doc := map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization,
			"metadata": map[string]any{"name": name, "namespace": "apps"},
			"spec": map[string]any{
				"path": name, "sourceRef": map[string]any{"kind": binding.Kind, "name": binding.Name},
			},
		}
		parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		obj, authoritative := parsed.Object, parsed.Document
		assert.NoError(t, err)
		return Child{Obj: obj, Doc: authoritative}
	}
	currentSource := source
	if (*testCaseInput) == "other-source-current" {
		currentSource.Name = "other-source"
		s.AddObject(&manifest.GitRepository{Name: currentSource.Name, Namespace: currentSource.Namespace})
		s.SetArtifact(
			currentSource,
			&SourceArtifact{Key: "other-stable", Content: "other-stable", LocalPath: "other-source"},
		)
	}
	p, k, l := producer("p", source), producer("k", currentSource), producer("l", source)
	m, extra := emissionChild("m", "value"), emissionChild("extra", "value")
	var gp Child
	if (*testCaseInput) == "other-source-current" {
		gp = producer("gp", source)
		s.AddObject(gp.Obj)
	} else {
		s.AddObject(p.Obj)
	}
	s.AddObject(l.Obj)

	publish := func(owner Child, children ...Child) {
		t.Helper()
		obj, _, attempt := s.GetEmission(owner.Obj.Named())
		defer attempt.Finish()
		if obj == nil {
			t.Fatalf("%v unavailable before publication", owner.Obj.Named())
		}
		attempt.Children, attempt.Manifests, attempt.Complete = children, nil, true
		for _, child := range children {
			attempt.Manifests = append(attempt.Manifests, manifest.DocOccurrence{Document: child.Doc})
		}
		s.RecordEmitted(owner.Obj.Named(), &attempt, nil)
	}
	check := func(stage string, child Child, want bool) {
		t.Helper()
		id := child.Obj.Named()
		{
			got := s.GetObject(id) != nil
			assert.ErrorIf(t, got != want, "%s: %v GetObject visible=%t, want %t", stage, id, got, want)
		}
		{
			got := slices.ContainsFunc(
				s.ListObjects("", ""),
				func(obj manifest.BaseManifest) bool { return obj.Named() == id },
			)
			assert.ErrorIf(t, got != want, "%s: %v ListObjects visible=%t, want %t", stage, id, got, want)
		}
	}
	if (*testCaseInput) == "other-source-current" {
		publish(gp, p)
	}
	publish(l, m)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		l.Obj.Named(): {
			Productions: []manifest.Production{
				{
					Owner:    k.Obj.Named(),
					Source:   source,
					Authored: l.Obj.Named(),
					Path:     "source/l.yaml",
					File:     "l.yaml",
				},
			},
		},
	}})
	publish(p, k)
	for _, child := range []Child{k, l, m} {
		check("initial", child, true)
	}
	if t.Failed() {
		t.FailNow()
	}
	t.Log("initial K/L/M visibility preconditions passed")
	if (*testCaseInput) == "accepted-child" {
		publish(k, l)
	} else {
		publish(k)
	}
	retained := (*testCaseInput) != "withdrawn"
	check("completed K", k, true)
	check("completed K", l, retained)
	check("completed K", m, retained)
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("completed K visibility preconditions passed: L/M retained=%t", retained)
	if (*testCaseInput) == "other-source-current" {
		publish(gp, p, extra)
	} else {
		publish(p, k, extra)
	}
	check("changed ancestor", k, true)
	check("changed ancestor", extra, true)
	check("changed ancestor", l, retained)
	check("changed ancestor", m, retained)
}

func TestRecordEmitted_CoverageAndStaticBoundary(t *testing.T) {
	t.Parallel()

	for _, support := range []string{"producer-only", "bootstrap-orphan", "bootstrap-orphan-equal", "same-source-root", "other-source-root", "self-declared-root", "other-source-only", "declaration-only"} {
		t.Run(support, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedCoverageAndStaticBoundaryCase(
				t,
				&support,
			)
		},
		)
	}
	t.Run(
		"anchoring_read_order",
		func(t *testing.T) { t.Parallel(); testRecordEmittedCoverageAndStaticBoundaryAnchoringReadOrder(t) },
	)
	for _, mode := range []string{"rooted_representative", "deleted_static"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedCoverageAndStaticBoundaryCase2(
				t,
				&mode,
			)
		},
		)
	}
	t.Run(
		"never_emitted_static",
		func(t *testing.T) { t.Parallel(); testRecordEmittedCoverageAndStaticBoundaryNeverEmittedStatic(t) },
	)
	t.Run(
		"replayed_bootstrap_seed",
		func(t *testing.T) { t.Parallel(); testRecordEmittedCoverageAndStaticBoundaryReplayedBootstrapSeed(t) },
	)

	for _, mode := range []string{"dependent-chain", "coverage-cycle", "boundary-support", "other-source-current"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedCoverageAndStaticBoundaryCase3(
				t,
				&mode,
			)
		},
		)
	}
}

func TestRecordEmitted_DescendantRetainsBootstrapAuthority(t *testing.T) {
	t.Parallel()

	s := New()
	seed := &manifest.Kustomization{Name: "generator", Namespace: "apps"}
	middle := &manifest.Kustomization{Name: "middle", Namespace: "apps"}
	applier := &manifest.Kustomization{Name: "sync", Namespace: "apps"}
	stale := emissionChild("stale", "seed")
	s.AddObjects([]manifest.BaseManifest{seed, stale.Obj})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		seed.Named():      {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
		stale.Obj.Named(): {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
	}})
	s.RecordEmitted(seed.Named(), &Emission{Children: []Child{{Obj: middle}}, Complete: true}, nil)
	s.RecordEmitted(middle.Named(), &Emission{Children: []Child{{Obj: applier}}, Complete: true}, nil)
	s.RecordEmitted(applier.Named(), &Emission{Children: []Child{{Obj: seed}, stale}, Complete: true}, nil)
	for _, obj := range []manifest.BaseManifest{seed, middle, applier, stale.Obj} {
		if s.GetObject(obj.Named()) != obj {
			t.Fatalf("descendant publication lost its bootstrap authority: %v", obj.Named())
		}
	}
	s.RecordEmitted(applier.Named(), &Emission{Children: []Child{{Obj: seed}}, Complete: true}, nil)
	assert.FatalIf(t, s.GetObject(stale.Obj.Named()) != nil, "%s", "withdrawal revived an unrelated bootstrap seed")
	s.RecordEmitted(seed.Named(), &Emission{Complete: true}, nil)
	assert.FatalIf(
		t,
		s.GetObject(middle.Named()) != nil || s.GetObject(applier.Named()) != nil ||
			s.GetObject(seed.Named()) != seed,
		"%s",
		"producer withdrawal retained emitted descendants or retired its own authority",
	)
	s.RecordEmitted(seed.Named(), &Emission{Children: []Child{{Obj: middle}}, Complete: true}, nil)
	assert.FatalIf(
		t,
		s.GetObject(applier.Named()) != applier,
		"%s",
		"producer replay did not restore its emitted descendant",
	)
	independent := &manifest.Kustomization{Name: "z-root", Namespace: "apps"}
	s.AddObject(independent)
	s.RecordEmitted(independent.Named(), &Emission{Children: []Child{{Obj: seed}}, Complete: true}, nil)
	s.RecordEmitted(independent.Named(), &Emission{Complete: true}, nil)
	for _, obj := range []manifest.BaseManifest{seed, middle, applier} {
		if s.GetObject(obj.Named()) != nil {
			t.Fatalf("withdrawn independent producer retained a detached cycle: %v", obj.Named())
		}
	}
}

func testRecordEmittedCoverageAndStaticBoundaryCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	p := ownerID("p")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	seed := emissionChild("seed", "old")
	s.AddObject(seed.Obj)
	source := manifest.BootstrapSourceID
	productions := []manifest.Production{
		{Owner: p, Source: source, Authored: seed.Obj.Named(), Path: "source/seed.yaml", File: "seed.yaml"},
	}
	provisional := (*testCaseInput) == "bootstrap-orphan" ||
		(*testCaseInput) == "bootstrap-orphan-equal"
	if provisional {
		productions[0].Owner = manifest.NamedResource{}
	}
	independent := !provisional && (*testCaseInput) != "producer-only"
	anchor := source
	if (*testCaseInput) == "other-source-root" || (*testCaseInput) == "other-source-only" {
		anchor.Name = "other-source"
	}
	if independent {
		root := manifest.Production{
			Source:   anchor,
			Authored: seed.Obj.Named(),
			Path:     "source/seed.yaml",
			File:     "seed.yaml",
		}
		if (*testCaseInput) == "self-declared-root" {
			root.Owner = seed.Obj.Named()
		}
		if (*testCaseInput) == "declaration-only" {
			root.Declaration = ownerID("declaration")
		}
		productions = append(productions, root)
		if (*testCaseInput) == "other-source-only" || (*testCaseInput) == "declaration-only" {
			productions = productions[1:]
		}
	}
	testRecordEmittedCoverageAndStaticBoundaryCaseCompletion(
		t,
		&testRecordEmittedCoverageAndStaticBoundaryCaseCompletionState{
			testCaseInput: testCaseInput,
			s:             s,
			p:             &p,
			seed:          &seed,
			productions:   &productions,
			provisional:   &provisional,
			independent:   &independent,
		},
	)
}

func testRecordEmittedCoverageAndStaticBoundaryAnchoringReadOrder(t *testing.T) {
	t.Helper()

	s := New()
	p := ownerID("p")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	child := emissionChild("anchored", "value")
	id := child.Obj.Named()
	s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true}, nil)
	s.RecordEmitted(p, &Emission{Complete: true}, nil)
	s.lockAll()
	defer s.unlockAll()
	sh := s.shardFor(id)
	cell := sh.cells[id]
	assert.Equal(t, cell.hidden, true)
	cell.rooted, cell.static = true, child.Obj
	sh.cells[id] = cell
	assert.Equal(t, sh.objectLocked(id), nil)
	var view capture
	obj, _ := sh.captureLocked(id, new(cell), &view)
	assert.Equal(t, obj, nil)
	assert.Equal(t, view.child.Obj, nil)
	assert.Equal(t, view.owner, (*producerSlot)(nil))
	cell.hidden = false
	sh.cells[id] = cell
	assert.Equal(t, sh.objectLocked(id), child.Obj)
	obj, _ = sh.captureLocked(id, new(cell), &view)
	assert.Equal(t, obj, child.Obj)
	assert.Equal(t, view.child.Obj, child.Obj)
	assert.Equal(t, view.accepted, true)
}

func testRecordEmittedCoverageAndStaticBoundaryCase2(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	p := ownerID("p")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	seed, child := emissionChild("seed", "static"), emissionChild("seed", "emitted")
	s.AddObject(seed.Obj)
	s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true}, nil)
	assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
	if (*testCaseInput) == "rooted_representative" {
		s.AddObject(child.Obj)
		q := ownerID("q")
		s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
		other := emissionChild("seed", "other")
		s.RecordEmitted(q, &Emission{Children: []Child{other}, Complete: true}, nil)
		assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), manifest.NamedResource{})
		warnings := s.Warnings()
		assert.FatalIf(t, len(warnings) != 1, "static conflict warnings: %+v", warnings)
		assert.Equal(t, warnings[0].Resource, seed.Obj.Named())
		assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
		assert.Equal(t, warnings[0].Count, 1)
		assert.Equal(t, warnings[0].Message, "conflicting producers; using static object")
		assert.Diff(t, warnings[0].Detail, []string{p.String(), q.String()})
		assert.FatalIf(t, s.GetObject(seed.Obj.Named()) != seed.Obj, "%s", "rooted read lost static authority")
		checkEffectiveObject(t, s, seed.Obj.Named(), &seed)
		replacement := emissionChild("seed", "replacement")
		s.AddObject(replacement.Obj, replacement.Doc)
		checkEffectiveObject(t, s, seed.Obj.Named(), &replacement)
		_, document, replaced := s.GetEmission(seed.Obj.Named())
		assert.Diff(t, document, replacement.Doc)
		replaced.Finish()
		assert.FatalIf(t, !s.DeleteObject(seed.Obj.Named()), "%s", "static authority was not deleted")
		checkEffectiveObject(t, s, seed.Obj.Named(), &other)
		return
	}
	assert.FatalIf(
		t,
		!s.DeleteObject(seed.Obj.Named()) || s.GetObject(seed.Obj.Named()) != child.Obj,
		"%s",
		"static deletion lost live emitted support",
	)
	assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
	checkEffectiveObject(t, s, seed.Obj.Named(), &child)
}

func testRecordEmittedCoverageAndStaticBoundaryNeverEmittedStatic(t *testing.T) {
	t.Helper()

	s := New()
	p := ownerID("p")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	seed := emissionChild("seed", "static")
	s.AddObject(seed.Obj)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		seed.Obj.Named(): {Productions: []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}}},
	}})
	_, _, attempt := s.GetEmission(p)
	attempt.Complete = true
	s.RecordEmitted(p, &attempt, nil)
	attempt.Finish()
	assert.FatalIf(
		t,
		s.GetObject(seed.Obj.Named()) != nil,
		"%s",
		"empty membership exposed hidden static prediction",
	)
}

func testRecordEmittedCoverageAndStaticBoundaryReplayedBootstrapSeed(t *testing.T) {
	t.Helper()

	s := New()
	p := ownerID("p")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	seed := emissionChild("seed", "same")
	s.AddObject(seed.Obj)
	next := &Emission{
		Children:  []Child{seed},
		Manifests: testutil.DocumentOccurrences([]map[string]any{seed.Doc}),
		Complete:  true,
	}
	s.RecordEmitted(p, next, nil)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		seed.Obj.Named(): {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
	}})
	s.RecordEmitted(p, next, nil)
	s.RecordEmitted(p, &Emission{Complete: true}, nil)
	assert.FatalIf(
		t,
		s.GetObject(seed.Obj.Named()) != nil,
		"%s",
		"equal replay failed to retire the bootstrap seed",
	)
}

func testRecordEmittedCoverageAndStaticBoundaryCase3(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	p, q, k, l := ownerID("p"), ownerID("q"), ownerID("k"), ownerID("l")
	child := Child{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}
	leaf := &manifest.Kustomization{Name: l.Name, Namespace: l.Namespace}
	tail := emissionChild("tail", "static")
	s.AddObjects([]manifest.BaseManifest{
		&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace},
		&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace}, leaf, tail.Obj,
	})
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		l:                {Productions: []manifest.Production{{Owner: k, Source: manifest.BootstrapSourceID}}},
		tail.Obj.Named(): {Productions: []manifest.Production{{Owner: l, Source: manifest.BootstrapSourceID}}},
	}
	if (*testCaseInput) == "coverage-cycle" {
		s.AddObject(child.Obj)
		records[k] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: l, Source: manifest.BootstrapSourceID}},
		}
	}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
	publish := func(owner manifest.NamedResource, present bool) {
		next := &Emission{Complete: true}
		if present {
			next.Children = []Child{child}
		}
		s.RecordEmitted(owner, next, nil)
	}
	check := func(want bool) {
		t.Helper()
		for _, id := range []manifest.NamedResource{k, l, tail.Obj.Named()} {
			sh := s.shardFor(id)
			assert.ErrorIf(
				t,
				(s.GetObject(id) != nil) != want || sh.cells[id].hidden == want,
				"%v support/visibility disagree, want live=%t",
				id,
				want,
			)
			assert.ErrorIf(
				t,
				sh.cells[id].static != nil && sh.cells[id].retired == want,
				"%v retirement disagrees, want live=%t",
				id,
				want,
			)
		}
	}
	publish(p, true)
	if (*testCaseInput) == "other-source-current" {
		_, _, attempt := s.GetEmission(k)
		attempt.binding.source.Name = "other-source"
		attempt.Complete = true
		s.RecordEmitted(k, &attempt, nil)
		attempt.Finish()
	}
	check(true)
	if (*testCaseInput) == "boundary-support" {
		publish(q, true)
	}
	publish(p, false)
	check((*testCaseInput) == "boundary-support")
	if (*testCaseInput) == "other-source-current" {
		s.AddObject(child.Obj)
		check(true)
		assert.FatalIf(t, !s.DeleteObject(k), "%s", "independent anchor was not removed")
		check(false)
	}
	if (*testCaseInput) == "boundary-support" {
		publish(q, false)
		check(false)
		for _, order := range [][]manifest.NamedResource{{p, q}, {q, p}} {
			publish(order[0], true)
			publish(order[1], true)
			check(true)
			publish(order[0], false)
			check(true)
			publish(order[1], false)
			check(false)
		}
	}
}

type testRecordEmittedCoverageAndStaticBoundaryCaseCompletionState struct {
	testCaseInput *string
	s             *Store
	p             *manifest.NamedResource
	seed          *Child
	productions   *[]manifest.Production
	provisional   *bool
	independent   *bool
}

func testRecordEmittedCoverageAndStaticBoundaryCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedCoverageAndStaticBoundaryCaseCompletionState,
) {
	t.Helper()
	scope.s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				scope.seed.Obj.Named(): {Productions: (*scope.productions)},
			},
		},
	)
	_, _, attempt := scope.s.GetEmission((*scope.p))
	actual := emissionChild("seed", "changed")
	if (*scope.testCaseInput) == "bootstrap-orphan-equal" {
		actual = emissionChild("seed", "old")
	}
	attempt.Children = []Child{actual}
	attempt.Manifests = testutil.DocumentOccurrences([]map[string]any{actual.Doc})
	attempt.Complete = true
	var events []manifest.BaseManifest
	unsub := scope.s.AddListener(EventObjectAdded, func(id manifest.NamedResource, data any) {
		if id == scope.seed.Obj.Named() {
			obj, _ := data.(manifest.BaseManifest)
			events = append(events, obj)
		}
	}, false)
	scope.s.RecordEmitted((*scope.p), &attempt, nil)
	unsub()
	assert.Equal(t, scope.s.EmissionOwner(scope.seed.Obj.Named()), (*scope.p))
	checkEffectiveObject(t, scope.s, scope.seed.Obj.Named(), &actual)
	if *scope.independent {
		assert.FatalIf(
			t,
			scope.s.GetObject(scope.seed.Obj.Named()) != actual.Obj,
			"%s",
			"accepted output did not replace independent static content",
		)
		_, _, empty := scope.s.GetEmission((*scope.p))
		empty.Children, empty.Manifests = nil, nil
		empty.Complete = true
		accept(t, scope.s, (*scope.p), &empty)
		empty.Finish()
		assert.Equal(t, scope.s.EmissionOwner(scope.seed.Obj.Named()), manifest.NamedResource{})
		checkEffectiveObject(t, scope.s, scope.seed.Obj.Named(), scope.seed)
		{
			got := scope.s.Productions(
				scope.seed.Obj.Named(),
				nil,
			)
			assert.FatalIf(t, scope.s.GetObject(scope.seed.Obj.Named()) != scope.seed.Obj || len(got) != 1 ||
				got[0] != (*scope.productions)[len((*scope.productions))-1], "complete empty retired independent source/input anchor: %v", got)
		}
		return
	}
	if *scope.provisional {
		{
			got := scope.s.GetObject(
				scope.seed.Obj.Named(),
			)
			assert.FatalIf(t, got != actual.Obj || len(events) != 1 ||
				events[0] != actual.Obj, "bootstrap replacement not published: %v, events %v", got, events)
		}
		{
			got := scope.s.Productions(scope.seed.Obj.Named(), nil)
			assert.FatalIf(t, len(got) != 1 || got[0].Owner != (*scope.p), "retired bootstrap support: %v", got)
		}
	}
	_, _, empty := scope.s.GetEmission((*scope.p))
	empty.Children = nil
	empty.Manifests = nil
	empty.Complete = true
	accept(t, scope.s, (*scope.p), &empty)
	assert.FatalIf(
		t,
		scope.s.GetObject(scope.seed.Obj.Named()) != nil ||
			len(scope.s.Productions(scope.seed.Obj.Named(), nil)) != 0,
		"%s",
		"complete empty resurrected covered prediction",
	)
	checkEffectiveObject(t, scope.s, scope.seed.Obj.Named(), &Child{})
	scope.s.AddObject(scope.seed.Obj)
	checkEffectiveObject(t, scope.s, scope.seed.Obj.Named(), scope.seed)
	assert.FatalIf(
		t,
		scope.s.GetObject(scope.seed.Obj.Named()) == nil,
		"%s",
		"explicit external anchor could not restore covered identity",
	)
}
