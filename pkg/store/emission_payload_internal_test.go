package store

import (
	"archive/tar"
	"context"
	"fmt"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRecordEmitted_PayloadPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		reverse, persistent bool
	}{
		{name: "memory/a-first"},
		{name: "memory/z-first", reverse: true},
		{name: "persistent/a-first", persistent: true},
		{name: "persistent/z-first", persistent: true, reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedPayloadPrecedenceCase(t, &tc)
		},
		)
	}
}

func TestRecordEmitted_PayloadProvenance(t *testing.T) {
	t.Parallel()
	s := New()
	p := ownerID("producer")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	child := emissionChild("target", "same")
	target := child.Obj.Named()
	payload := &SourceArtifact{
		Kind:    target.Kind,
		Key:     "key",
		Content: "key",
		Digest:  "digest",
		FS:      fstest.MapFS{"file": {Data: []byte("bytes")}},
	}
	_, _, attempt := s.GetEmission(p)
	defer attempt.Finish()
	attempt.Children = []Child{child}
	attempt.Fingerprint = "same"
	s.RecordEmitted(p, &attempt, map[manifest.NamedResource]*SourceArtifact{target: payload})
	assert.FatalIf(
		t,
		attempt.Fingerprint != "" || s.GetArtifact(p, ArtifactContent).(*Emission).Fingerprint != "",
		"%s",
		"partial payload publication retained successful fingerprint eligibility",
	)
	attempt.Children, attempt.Complete = []Child{child}, true
	attempt.Fingerprint = "same"
	s.RecordEmitted(p, &attempt, nil)
	assert.FatalIf(
		t,
		s.GetArtifact(target, ArtifactContent) != payload,
		"%s",
		"same-attempt nil finalization removed payload",
	)
	_, _, replay := s.GetEmission(p)
	defer replay.Finish()
	replay.Complete = true
	s.RecordEmitted(p, &replay, nil)
	assert.FatalIf(t, s.GetArtifact(target, ArtifactContent) != payload, "%s", "borrowed replay removed payload")
	s.RecordEmitted(
		p,
		&Emission{
			Children:    []Child{child},
			Manifests:   replay.Manifests,
			Fingerprint: replay.Fingerprint,
			Complete:    true,
		},
		nil,
	)
	assert.FatalIf(
		t,
		s.GetArtifact(target, ArtifactContent) != nil,
		"%s",
		"fresh declaration-only replacement retained bytes",
	)
	s.RecordEmitted(
		p,
		&Emission{Children: []Child{child}, Fingerprint: "same", Complete: true},
		map[manifest.NamedResource]*SourceArtifact{target: payload},
	)
	other := emissionChild("other", "new")
	_, _, replacement := s.GetEmission(p)
	defer replacement.Finish()
	replacement.Children, replacement.Complete = []Child{child, other}, true
	s.RecordEmitted(p, &replacement, map[manifest.NamedResource]*SourceArtifact{other.Obj.Named(): payload})
	assert.FatalIf(
		t,
		s.GetArtifact(target, ArtifactContent) != nil ||
			s.GetArtifact(other.Obj.Named(), ArtifactContent) != payload,
		"%s",
		"new attempt borrowed an earlier payload for its fresh declaration-only child",
	)
}

func TestRecordEmitted_PayloadUpdate(t *testing.T) {
	t.Parallel()
	s := New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(owner)
	child := emissionChild("generated", "same")
	target := child.Obj.Named()
	first := &SourceArtifact{
		Key:     "first",
		Content: "first",
		Digest:  "first",
		FS:      fstest.MapFS{"file": {Data: []byte("first")}},
	}
	second := &SourceArtifact{
		Key:     "second",
		Content: "second",
		Digest:  "second",
		FS:      fstest.MapFS{"file": {Data: []byte("second")}},
	}
	_, _, initial := s.GetEmission(owner.Named())
	initial.Children, initial.Manifests, initial.Fingerprint, initial.Complete = []Child{
		child,
	}, testutil.DocumentOccurrences(
		[]map[string]any{child.Doc},
	), "same", true
	s.RecordEmitted(owner.Named(), &initial, map[manifest.NamedResource]*SourceArtifact{target: first})
	var updates []*SourceArtifact
	defer s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
		if id == target {
			updates = append(updates, value.(*SourceArtifact))
		}
	}, false)()
	_, _, next := s.GetEmission(owner.Named())
	defer next.Finish()
	next.Complete = true
	s.RecordEmitted(owner.Named(), &next, map[manifest.NamedResource]*SourceArtifact{target: second})
	assert.FatalIf(
		t,
		!slices.Equal(updates, []*SourceArtifact{second}),
		"%s",
		"payload-only replacement did not notify the whole descriptor exactly once",
	)
	obj, doc, input := s.GetEmission(target)
	defer input.Finish()
	assert.Equal(t, obj, child.Obj)
	assert.Diff(t, doc, child.Doc)
	assert.Equal(t, s.GetArtifact(target, ArtifactContent), Artifact(second))
	equal := *second
	equal.FS = fstest.MapFS{"file": {Data: []byte("second")}}
	s.RecordEmitted(owner.Named(), &next, map[manifest.NamedResource]*SourceArtifact{target: &equal})
	assert.FatalIf(
		t,
		!slices.Equal(updates, []*SourceArtifact{second}),
		"%s",
		"semantically equal payload with distinct backing notified readers",
	)
	s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
	assert.FatalIf(
		t,
		s.GetArtifact(target, ArtifactContent) != nil || !slices.Equal(updates, []*SourceArtifact{second, nil}),
		"%s",
		"last payload withdrawal did not remove authority and notify readers",
	)
}

func TestRecordEmitted_PayloadWarnings(t *testing.T) {
	t.Parallel()
	for _, equal := range []bool{false, true} {
		t.Run(fmt.Sprintf("equal=%t", equal), func(t *testing.T) {
			t.Parallel()
			s := New()
			a, z, zz := ownerID("a"), ownerID("z"), ownerID("zz")
			for _, id := range []manifest.NamedResource{a, z, zz} {
				s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			}
			child := emissionChild("generated", "same")
			target := child.Obj.Named()
			s.RecordEmitted(zz, &Emission{Children: []Child{child}, Complete: true}, nil)
			first := &SourceArtifact{
				Key:      "a",
				Content:  "a",
				Digest:   "a",
				Revision: "a",
				Metadata: map[string]string{"origin": "a"},
				FS:       fstest.MapFS{"file": {Data: []byte("a")}},
			}
			last := &SourceArtifact{
				Key:      "a",
				Content:  "a",
				Digest:   "a",
				Revision: "a",
				Metadata: map[string]string{"origin": "a"},
				FS:       fstest.MapFS{"file": {Data: []byte("a")}},
			}
			if !equal {
				last.Key, last.Digest, last.Revision = "z", "z", "z"
				last.Metadata, last.FS = map[string]string{"origin": "z"}, fstest.MapFS{"file": {Data: []byte("z")}}
			}
			s.RecordEmitted(
				a,
				&Emission{Children: []Child{child}, Complete: true},
				map[manifest.NamedResource]*SourceArtifact{target: first},
			)
			s.RecordEmitted(
				z,
				&Emission{Children: []Child{child}, Complete: true},
				map[manifest.NamedResource]*SourceArtifact{target: last},
			)
			var want []manifest.Warning
			if !equal {
				want = []manifest.Warning{
					{
						Resource: target,
						Subject:  manifest.DiagnosticFor(target, nil),
						Related:  []manifest.NamedResource{a, z, zz, zz},
						Category: manifest.WarnConflictingProducers,
						Count:    1,
						Message:  "conflicting producers; using " + zz.String(),
						Detail:   []string{a.String(), z.String(), zz.String()},
					},
				}
			}
			assert.Diff(t, s.Warnings(), want)
			assert.Equal(t, s.EmissionOwner(target), zz)
			assert.Equal(t, s.GetArtifact(target, ArtifactContent), Artifact(last))
		})
	}
}

func testRecordEmittedPayloadPrecedenceCase(t *testing.T, testCaseInput *struct {
	name       string
	reverse    bool
	persistent bool
},
) {
	t.Helper()

	s := New()
	a, z, zz := ownerID("a"), ownerID("z"), ownerID("zz")
	for _, id := range []manifest.NamedResource{a, z, zz} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	firstChild, lastChild, declaration := emissionChild(
		"generated",
		"a",
	), emissionChild(
		"generated",
		"z",
	), emissionChild(
		"generated",
		"zz",
	)
	target := declaration.Obj.Named()
	root := ""
	if testCaseInput.persistent {
		root = t.TempDir()
	}
	cache, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cache.Close()) })
	acquire := func(value string) *SourceArtifact {
		t.Helper()
		input, descriptor, key, acquireErr := cache.Acquire(
			t.Context(),
			sourcecache.ArchiveKey{Content: value, Policy: "fixed"},
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			func(_ context.Context, target sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				writer := tar.NewWriter(target)
				if addFSErr := writer.AddFS(fstest.MapFS{"file": {Data: []byte(value)}}); addFSErr != nil {
					return sourcecache.ProducerResult{}, addFSErr
				}
				return sourcecache.ProducerResult{}, writer.Close()
			},
		)
		assert.NoError(t, acquireErr)
		return &SourceArtifact{
			Kind:      target.Kind,
			URL:       "https://" + value + ".test",
			LocalPath: "/" + value,
			LocalRoot: "/root-" + value,
			Key:       key,
			Digest:    descriptor.Annotations["flate.tree-digest"],
			Revision:  "revision-" + value,
			Metadata:  map[string]string{"origin": value},
			FS:        input,
		}
	}
	first, last := acquire("a"), acquire("z")
	first.Size, last.Size, last.InputAlias = 1, 2, true
	peer := New()
	peer.AddObject(&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace})
	peer.RecordEmitted(
		a,
		&Emission{Children: []Child{firstChild}, Complete: true},
		map[manifest.NamedResource]*SourceArtifact{target: first},
	)
	owners, payloads := []manifest.NamedResource{a, z}, []*SourceArtifact{first, last}
	children := []Child{firstChild, lastChild}
	if testCaseInput.reverse {
		slices.Reverse(owners)
		slices.Reverse(payloads)
		slices.Reverse(children)
	}
	testRecordEmittedPayloadPrecedenceCaseCompletion(
		t,
		&testRecordEmittedPayloadPrecedenceCaseCompletionState{
			s:           &s,
			a:           &a,
			z:           &z,
			zz:          &zz,
			declaration: &declaration,
			target:      &target,
			err:         &err,
			first:       &first,
			last:        &last,
			peer:        &peer,
			owners:      &owners,
			payloads:    &payloads,
			children:    &children,
		},
	)
}

type testRecordEmittedPayloadPrecedenceCaseCompletionState struct {
	s           **Store
	a           *manifest.NamedResource
	z           *manifest.NamedResource
	zz          *manifest.NamedResource
	declaration *Child
	target      *manifest.NamedResource
	err         *error
	first       **SourceArtifact
	last        **SourceArtifact
	peer        **Store
	owners      *[]manifest.NamedResource
	payloads    *[]*SourceArtifact
	children    *[]Child
}
