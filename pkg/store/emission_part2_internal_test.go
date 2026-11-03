package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_WithdrawalVisibility(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, lateFirst := range []bool{false, true} {
			for _, shared := range []bool{false, true} {
				t.Run(fmt.Sprintf("workers=%d/lateFirst=%t/shared=%t", workers, lateFirst, shared), func(t *testing.T) {
					t.Parallel()
					testRecordEmittedWithdrawalVisibilityCase(
						t,
						&testRecordEmittedWithdrawalVisibilityCaseInputs{
							workers:   &workers,
							lateFirst: &lateFirst,
							shared:    &shared,
						},
					)
				},
				)
			}
		}
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestRecordEmitted_ContributionContext(t *testing.T) {
	t.Parallel()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	for _, id := range []manifest.NamedResource{p, q} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	childWithLabels := func(owner string) Child {
		t.Helper()
		labels := map[string]any{}
		for _, key := range []string{"kustomize.toolkit.fluxcd.io/name", "kustomize.toolkit.fluxcd.io/namespace", "helm.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/namespace"} {
			labels[key] = owner
		}
		doc := map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1",
			"kind":       manifest.KindKustomization,
			"metadata":   map[string]any{"name": "shared", "namespace": "apps", "labels": labels},
			"spec":       map[string]any{"path": "app"},
		}
		obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		{
			assert.NoError(t, err)
		}
		return Child{Obj: obj, Doc: authoritative}
	}
	a, b := childWithLabels("p"), childWithLabels("q")
	first := sourceBinding{
		source: manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Name:      "first",
			Namespace: "apps",
		},
	}
	second := first
	second.source.Name = "second"
	accept(
		t,
		s,
		p,
		&Emission{
			Children:  []Child{a},
			Manifests: testutil.DocumentOccurrences([]map[string]any{a.Doc}),
			Complete:  true,
			binding:   first,
		},
	)
	accept(
		t,
		s,
		q,
		&Emission{
			Children:  []Child{b},
			Manifests: testutil.DocumentOccurrences([]map[string]any{b.Doc}),
			Complete:  true,
			binding:   second,
		},
	)
	for _, contribution := range []struct {
		owner manifest.NamedResource
		child Child
	}{{p, a}, {q, b}} {
		_, _, inventory := s.GetEmission(contribution.owner)
		{
			diff := cmp.Diff(
				inventory.Children,
				[]Child{contribution.child},
				cmp.AllowUnexported(manifest.Kustomization{}),
				cmpopts.IgnoreUnexported(Child{}),
				cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
			)
			assert.FatalIf(t, diff != "", "emission children changed (-want +got): %s", diff)
		}
		inventory.Finish()
	}
	changed := childWithLabels("q")
	changed.Doc["metadata"].(map[string]any)["labels"].(map[string]any)["semantic"] = "different"
	s.RecordEmitted(q, &Emission{Children: []Child{changed}, Complete: true}, nil)
	{
		warnings := s.Warnings()
		assert.FatalIf(
			t,
			len(warnings) != 1 || warnings[0].Category != manifest.WarnConflictingProducers,
			"semantic metadata conflict warning: %+v",
			warnings,
		)
	}
	fresh := emissionChild("fresh", "value")
	accept(
		t,
		s,
		p,
		&Emission{
			Children:  []Child{fresh},
			Manifests: testutil.DocumentOccurrences([]map[string]any{fresh.Doc}),
			binding:   second,
		},
	)
	oldProductions := s.Productions(a.Obj.Named(), nil)
	{
		assert.FatalIf(t, !slices.ContainsFunc(
			oldProductions,
			func(p manifest.Production) bool { return p.Owner == ownerID("p") && p.Source == first.source },
		), "%s", "partial update rebound predecessor context")
	}
	{
		got := s.Productions(fresh.Obj.Named(), nil)
		assert.FatalIf(
			t,
			len(got) != 1 || got[0].Source != second.source,
			"%s",
			"partial update lost new source context",
		)
	}
	_, _, attempt := s.GetEmission(p)
	attempt.Unreadable = []manifest.NamedResource{fresh.Obj.Named()}
	var notifications int
	unsub := s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == p {
			notifications++
		}
	}, false)
	accept(t, s, p, &attempt)
	unsub()
	_, _, accepted := s.GetEmission(p)
	{
		assert.FatalIf(
			t,
			notifications != 1 || !slices.Equal(accepted.Unreadable, attempt.Unreadable),
			"%s",
			"context-only refresh was discarded",
		)
	}
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("validity_memo/%d", workers), func(t *testing.T) {
			t.Parallel()
			testRecordEmittedContributionContextCase(
				t,
				&testRecordEmittedContributionContextCaseInputs{
					p:               &p,
					q:               &q,
					childWithLabels: &childWithLabels,
					workers:         &workers,
				},
			)
		},
		)
	}
}

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

type testRecordEmittedWithdrawalVisibilityCaseInputs struct {
	workers   *int
	lateFirst *bool
	shared    *bool
}

func testRecordEmittedWithdrawalVisibilityCase(
	t *testing.T,
	testCaseInput *testRecordEmittedWithdrawalVisibilityCaseInputs,
) {
	t.Helper()

	s := New()
	p, k, l, q := ownerID("p"), ownerID("k"), ownerID("l"), ownerID("a")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	child := Child{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}
	cycle := Child{Obj: &manifest.Kustomization{Name: l.Name, Namespace: l.Namespace}}
	data := emissionChild("shared", "value")
	late := emissionChild("late", "value")
	payload := &SourceArtifact{
		Key:     "late-generated",
		Content: "late-generated",
		Digest:  "late-digest",
		FS:      fstest.MapFS{"file": {Data: []byte("late bytes")}},
	}
	accept(t, s, p, &Emission{Children: []Child{child}, Complete: true})
	accept(
		t,
		s,
		k,
		&Emission{
			Children:  []Child{cycle, data},
			Manifests: testutil.DocumentOccurrences([]map[string]any{data.Doc}),
			Complete:  true,
		},
	)
	accept(t, s, l, &Emission{Children: []Child{child}, Complete: true})
	independent := emissionChild("shared", "independent")
	if *testCaseInput.shared {
		s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
		accept(
			t,
			s,
			q,
			&Emission{
				Children:  []Child{independent},
				Manifests: testutil.DocumentOccurrences([]map[string]any{independent.Doc}),
				Complete:  true,
			},
		)
	}
	for _, id := range []manifest.NamedResource{k, l, data.Obj.Named()} {
		s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	}
	s.SetArtifact(data.Obj.Named(), &SourceArtifact{Key: "live-data"})
	{
		assert.FatalIf(
			t,
			s.GetArtifact(data.Obj.Named(), ArtifactContent) == nil,
			"%s",
			"fixture data artifact was not retained before withdrawal",
		)
	}
	captured, lateRelease, withdrawRelease := make(
		chan struct{},
	), make(
		chan struct{},
	), make(
		chan struct{},
	)
	lateDone, withdrawDone := make(chan struct{}), make(chan struct{})
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		_, _, attempt := s.GetEmission(k)
		attempt.Children = []Child{cycle, data, late}
		attempt.Manifests = testutil.DocumentOccurrences([]map[string]any{data.Doc, late.Doc})
		attempt.Complete = true
		close(captured)
		<-lateRelease
		s.RecordEmitted(
			k,
			&attempt,
			map[manifest.NamedResource]*SourceArtifact{
				late.Obj.Named(): payload,
				data.Obj.Named(): payload,
			},
		)
		attempt.Finish()
		close(lateDone)
	})
	wg.Go(func() {
		<-start
		<-captured
		<-withdrawRelease
		accept(t, s, p, &Emission{Complete: true})
		close(withdrawDone)
	})
	for i := 2; i < (*testCaseInput.workers); i++ {
		wg.Go(func() {
			<-start
			<-captured
			owner := ownerID(fmt.Sprintf("independent-%d", i))
			s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			value := emissionChild(owner.Name, "independent")
			accept(t, s, owner, &Emission{Children: []Child{value}, Complete: true})
		})
	}
	close(start)
	<-captured
	if *testCaseInput.lateFirst {
		close(lateRelease)
		<-lateDone
		close(withdrawRelease)
	} else {
		close(withdrawRelease)
		<-withdrawDone
		close(lateRelease)
	}
	wg.Wait()
	hidden := []manifest.NamedResource{k, l, late.Obj.Named()}
	if !(*testCaseInput.shared) {
		hidden = append(hidden, data.Obj.Named())
	} else {
		checkEffectiveObject(t, s, data.Obj.Named(), independent)
		assert.Equal(t, s.EmissionOwner(data.Obj.Named()), q)
	}
	{
		assert.FatalIf(
			t,
			(*testCaseInput.shared) && s.GetArtifact(data.Obj.Named(), ArtifactContent) == payload,
			"%s",
			"hidden producer supplied bytes to the independently visible declaration",
		)
	}
	testRecordEmittedWithdrawalVisibilityCaseCompletion(t, &testRecordEmittedWithdrawalVisibilityCaseCompletionState{testCaseInput: testCaseInput, s: s, k: &k, l: &l, data: &data, late: &late, payload: payload, hidden: &hidden})
}

type testRecordEmittedContributionContextCaseInputs struct {
	p               *manifest.NamedResource
	q               *manifest.NamedResource
	childWithLabels *func(owner string) Child
	workers         *int
}

func testRecordEmittedContributionContextCase(
	t *testing.T,
	testCaseInput *testRecordEmittedContributionContextCaseInputs,
) {
	t.Helper()

	s := New()
	for _, id := range []manifest.NamedResource{(*testCaseInput.p), (*testCaseInput.q)} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	a, b := (*testCaseInput.childWithLabels)("p"), (*testCaseInput.childWithLabels)("q")
	s.RecordEmitted((*testCaseInput.p), &Emission{Children: []Child{a}, Complete: true}, nil)
	id := a.Obj.Named()
	_, _, attempt := s.GetEmission(id)
	attempt.Complete = true
	s.RecordEmitted(id, &attempt, nil)
	accepted := s.GetArtifact(id, ArtifactContent).(*Emission)
	s.RecordEmitted((*testCaseInput.q), &Emission{Children: []Child{b}, Complete: true}, nil)
	checkReplay := func(want bool) {
		t.Helper()
		seen := false
		s.AddListener(EventArtifactUpdated, func(event manifest.NamedResource, _ any) {
			if event == id {
				seen = true
			}
		}, true)()
		{
			assert.ErrorIf(t, seen != want, "artifact replay = %t, want %t", seen, want)
		}
	}
	checkReplay(true)
	{
		assert.FatalIf(
			t,
			accepted.validated == nil || accepted.validated.Load() == nil || *accepted.validated.Load() != b.Obj,
			"%s",
			"listener replay did not memoize the equal representative",
		)
	}
	validated := accepted.validated.Load()
	start := make(chan struct{})
	var readers sync.WaitGroup
	for range *testCaseInput.workers {
		readers.Go(func() {
			<-start
			for range 8 {
				{
					assert.ErrorIf(
						t,
						s.GetArtifact(id, ArtifactContent) != accepted,
						"%s",
						"equivalent input invalidated current artifact",
					)
				}
				checkReplay(true)
			}
		})
	}
	close(start)
	readers.Wait()
	{
		assert.FatalIf(
			t,
			accepted.validated.Load() != validated,
			"%s",
			"memo hits replaced the validated representative cell",
		)
	}
	unequalDoc := manifest.DeepCopyMap(b.Doc)
	unequalDoc["spec"].(map[string]any)["path"] = "different"
	unequal, doc, _, err := manifest.ParseDoc(unequalDoc, manifest.ParseDocOptions{})
	{
		assert.NoError(t, err)
	}
	s.RecordEmitted((*testCaseInput.q), &Emission{Children: []Child{{Obj: unequal, Doc: doc}}, Complete: true}, nil)
	{
		assert.FatalIf(
			t,
			s.GetArtifact(id, ArtifactContent) != nil,
			"%s",
			"unequal representative reused a validated artifact",
		)
	}
	checkReplay(false)
	{
		validated := accepted.validated.Load()
		assert.FatalIf(t, validated == nil || *validated != b.Obj, "%s", "unequal input entered the validity memo")
	}
	replacement := *accepted
	replacement.input, replacement.inputDoc = unequal, doc
	s.RecordEmitted(id, &replacement, nil)
	current := s.GetArtifact(id, ArtifactContent).(*Emission)
	{
		assert.FatalIf(t, current == accepted || current.validated == nil || current.validated == accepted.validated ||
			current.validated.Load() != nil, "%s", "new emission retained its predecessor's memo")
	}
	s.RecordEmitted((*testCaseInput.q), &Emission{Children: []Child{b}, Complete: true}, nil)
	{
		assert.FatalIf(
			t,
			s.GetArtifact(id, ArtifactContent) != nil,
			"%s",
			"old memo authorized the replacement emission",
		)
	}
	checkReplay(false)
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
		obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		{
			assert.NoError(t, err)
		}
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

type testRecordEmittedWithdrawalVisibilityCaseCompletionState struct {
	testCaseInput *testRecordEmittedWithdrawalVisibilityCaseInputs
	s             *Store
	k             *manifest.NamedResource
	l             *manifest.NamedResource
	data          *Child
	late          *Child
	payload       *SourceArtifact
	hidden        *[]manifest.NamedResource
}

func testRecordEmittedWithdrawalVisibilityCaseCompletion(t *testing.T, scope *testRecordEmittedWithdrawalVisibilityCaseCompletionState) {
	t.Helper()
	for _, id := range *scope.hidden {
		scope.s.UpdateFailure(id, manifest.NewError(manifest.ErrInput, nil, "hidden successor"))
		scope.s.SetArtifact(id, &SourceArtifact{Key: "late"})
		assert.Equal(t, scope.s.EmissionOwner(id), manifest.NamedResource{})
		{
			assert.FatalIf(
				t,
				scope.s.GetObject(id) != nil || scope.s.GetArtifact(id, ArtifactContent) != nil,
				"%s",
				"hidden payload leaked",
			)
		}
		{
			obj, _ := scope.s.Get[*manifest.Kustomization](id)
			assert.FatalIf(t, obj != nil, "%s", "typed getter leaked")
		}
		{
			_, ok := scope.s.GetStatus(id)
			assert.FatalIf(t, ok, "%s", "hidden Ready leaked")
		}
		var observation Observation
		obj, observation := scope.s.Snapshot(id, true)
		info := observation.StatusInfo
		conditions := observation.Conditions
		{
			assert.FatalIf(
				t,
				obj != nil || info != (StatusInfo{}) || len(conditions) != 0 || observation.Statusful,
				"%s",
				"hidden snapshot leaked",
			)
		}
		if obj, _, attempt := scope.s.GetEmission(id); obj != nil {
			attempt.Finish()
			t.Fatal("hidden attempt leaked")
		}
		{
			assert.FatalIf(t, len(scope.s.Productions(id, nil)) != 0, "%s", "hidden productions leaked")
		}
		{
			_, ok := scope.s.FailedResources()[id]
			assert.FatalIf(t, ok, "%s", "hidden failure leaked")
		}
		scope.s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
		{
			_, ok := scope.s.GetStatus(id)
			assert.FatalIf(t, ok, "%s", "hidden late Ready leaked")
		}
		checkEffectiveObject(t, scope.s, id, Child{})
	}
	{
		assert.FatalIf(t, len(
			scope.s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
		) != (*scope.testCaseInput.workers)-1+boolInt((*scope.testCaseInput.shared)), "%s", "typed listing leaked hidden cycle")
	}
	for event := EventObjectAdded; event <= EventArtifactUpdated; event++ {
		scope.s.AddListener(event, func(id manifest.NamedResource, _ any) {
			{
				assert.ErrorIf(t, slices.Contains((*scope.hidden), id), "%s", "hidden replay leaked")
			}
		}, true)()
	}
	{
		assert.FatalIf(t, len(scope.s.Warnings()) != 0, "%s", "hidden payload contributed a conflict warning")
	}
	scope.s.AddObject(&manifest.Kustomization{Name: (*scope.k).Name, Namespace: (*scope.k).Namespace})
	{
		assert.FatalIf(
			t,
			scope.s.GetObject((*scope.k)) == nil || scope.s.GetObject((*scope.l)) == nil || scope.s.GetObject((*scope.late).Obj.Named()) == nil,
			"%s",
			"external anchor did not restore supported descendants",
		)
	}
	{
		assert.FatalIf(
			t,
			scope.s.GetArtifact((*scope.late).Obj.Named(), ArtifactContent) != scope.payload,
			"%s",
			"reanchoring lost privately completed generated payload",
		)
	}
	for _, id := range *scope.hidden {
		obj := scope.s.GetObject(id)
		{
			snapshot, _ := scope.s.Snapshot(id, false)
			assert.ErrorIf(t, snapshot != obj, "%s", "reanchored snapshot differs from effective object")
		}
		rich, _, attempt := scope.s.GetEmission(id)
		assert.Equal(t, rich, obj)
		attempt.Finish()
		{
			artifact := scope.s.GetArtifact(
				id,
				ArtifactContent,
			)
			assert.ErrorIf(t, artifact != nil &&
				((id != (*scope.late).Obj.Named() && id != (*scope.data).Obj.Named()) || artifact != scope.payload), "%s", "reanchoring restored retained artifact")
		}
		{
			_, ok := scope.s.GetStatus(id)
			assert.ErrorIf(t, ok, "%s", "reanchoring restored retained Ready")
		}
	}
	{
		assert.FatalIf(t, !scope.s.DeleteObject((*scope.k)), "%s", "independent cycle anchor was not removed")
	}
	for _, id := range *scope.hidden {
		{
			assert.ErrorIf(
				t,
				scope.s.GetObject(id) != nil || scope.s.GetArtifact(id, ArtifactContent) != nil,
				"%s",
				"removed anchor retained a detached cycle",
			)
		}
	}
	{
		assert.FatalIf(
			t,
			(*scope.testCaseInput.shared) && scope.s.GetObject((*scope.data).Obj.Named()) == nil,
			"%s",
			"removed cycle anchor lost surviving Q support",
		)
	}
}
