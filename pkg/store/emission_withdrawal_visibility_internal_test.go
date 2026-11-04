package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/fstest"

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
	testRecordEmittedWithdrawalVisibilityCaseCompletion(
		t,
		&testRecordEmittedWithdrawalVisibilityCaseCompletionState{
			testCaseInput: testCaseInput,
			s:             s,
			k:             &k,
			l:             &l,
			data:          &data,
			late:          &late,
			payload:       payload,
			hidden:        &hidden,
		},
	)
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

func testRecordEmittedWithdrawalVisibilityCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedWithdrawalVisibilityCaseCompletionState,
) {
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
	scope.s.AddObject(&manifest.Kustomization{Name: scope.k.Name, Namespace: scope.k.Namespace})
	{
		assert.FatalIf(
			t,
			scope.s.GetObject((*scope.k)) == nil || scope.s.GetObject((*scope.l)) == nil ||
				scope.s.GetObject(scope.late.Obj.Named()) == nil,
			"%s",
			"external anchor did not restore supported descendants",
		)
	}
	{
		assert.FatalIf(
			t,
			scope.s.GetArtifact(scope.late.Obj.Named(), ArtifactContent) != scope.payload,
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
				((id != scope.late.Obj.Named() && id != scope.data.Obj.Named()) || artifact != scope.payload), "%s", "reanchoring restored retained artifact")
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
			(*scope.testCaseInput.shared) && scope.s.GetObject(scope.data.Obj.Named()) == nil,
			"%s",
			"removed cycle anchor lost surviving Q support",
		)
	}
}
