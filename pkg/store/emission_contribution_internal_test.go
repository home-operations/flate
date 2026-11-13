package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

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
		for _, producer := range []manifest.BaseManifest{&manifest.Kustomization{}, &manifest.HelmRelease{}, &manifest.ResourceSet{}} {
			for _, key := range producer.EmissionPolicy().OwnerKeys {
				labels[key] = owner
			}
		}
		doc := map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1",
			"kind":       manifest.KindKustomization,
			"metadata":   map[string]any{"name": "shared", "namespace": "apps", "labels": labels},
			"spec":       map[string]any{"path": "app"},
		}
		parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		obj, authoritative := parsed.Object, parsed.Document
		assert.NoError(t, err)
		return Child{Obj: obj, Doc: authoritative}
	}
	a, b := childWithLabels("p"), childWithLabels("q")
	assert.Equal(t, semanticEqual(a, b), true)
	assert.Equal(t, semanticObjectsEqual(Child{Obj: a.Obj}, Child{Obj: b.Obj}), true)
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
	assert.Equal(t, semanticEqual(a, changed), false)
	changedObject, err := manifest.ParseDoc(changed.Doc, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.Equal(t, semanticObjectsEqual(Child{Obj: a.Obj}, Child{Obj: changedObject.Object}), false)
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
	assert.FatalIf(t, !slices.ContainsFunc(
		oldProductions,
		func(p manifest.Production) bool { return p.Owner == ownerID("p") && p.Source == first.source },
	), "%s", "partial update rebound predecessor context")
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
	assert.FatalIf(
		t,
		notifications != 1 || !slices.Equal(accepted.Unreadable, attempt.Unreadable),
		"%s",
		"context-only refresh was discarded",
	)
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
		assert.ErrorIf(t, seen != want, "artifact replay = %t, want %t", seen, want)
	}
	checkReplay(true)
	assert.FatalIf(
		t,
		accepted.validated == nil || accepted.validated.Load() == nil || *accepted.validated.Load() != b.Obj,
		"%s",
		"listener replay did not memoize the equal representative",
	)
	validated := accepted.validated.Load()
	start := make(chan struct{})
	var readers sync.WaitGroup
	for range *testCaseInput.workers {
		readers.Go(func() {
			<-start
			for range 8 {
				assert.ErrorIf(
					t,
					s.GetArtifact(id, ArtifactContent) != accepted,
					"%s",
					"equivalent input invalidated current artifact",
				)
				checkReplay(true)
			}
		})
	}
	close(start)
	readers.Wait()
	assert.FatalIf(
		t,
		accepted.validated.Load() != validated,
		"%s",
		"memo hits replaced the validated representative cell",
	)
	unequalDoc := manifest.DeepCopyMap(b.Doc)
	unequalDoc["spec"].(map[string]any)["path"] = "different"
	parsed2, err := manifest.ParseDoc(unequalDoc, manifest.ParseDocOptions{})
	unequal, doc := parsed2.Object, parsed2.Document
	assert.NoError(t, err)
	s.RecordEmitted((*testCaseInput.q), &Emission{Children: []Child{{Obj: unequal, Doc: doc}}, Complete: true}, nil)
	assert.FatalIf(
		t,
		s.GetArtifact(id, ArtifactContent) != nil,
		"%s",
		"unequal representative reused a validated artifact",
	)
	checkReplay(false)
	{
		validated := accepted.validated.Load()
		assert.FatalIf(t, validated == nil || *validated != b.Obj, "%s", "unequal input entered the validity memo")
	}
	replacement := *accepted
	replacement.input, replacement.inputDoc = unequal, doc
	s.RecordEmitted(id, &replacement, nil)
	current := s.GetArtifact(id, ArtifactContent).(*Emission)
	assert.FatalIf(t, current == accepted || current.validated == nil || current.validated == accepted.validated ||
		current.validated.Load() != nil, "%s", "new emission retained its predecessor's memo")
	s.RecordEmitted((*testCaseInput.q), &Emission{Children: []Child{b}, Complete: true}, nil)
	assert.FatalIf(
		t,
		s.GetArtifact(id, ArtifactContent) != nil,
		"%s",
		"old memo authorized the replacement emission",
	)
	checkReplay(false)
}
