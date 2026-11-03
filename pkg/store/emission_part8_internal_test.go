package store

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

func TestRecordEmitted_CrossGroupSeedDocumentArrival(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("publisher")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
		"metadata": map[string]any{"name": "shared", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://charts.example"},
	}
	obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	{
		assert.NoError(t, err)
	}
	s.AddObject(obj)
	foreignDoc := manifest.DeepCopyMap(doc)
	foreignDoc["apiVersion"] = "example.test/v1"
	foreign, _, _, err := manifest.ParseDoc(foreignDoc, manifest.ParseDocOptions{})
	{
		assert.NoError(t, err)
	}
	s.AddObject(foreign)
	var arrivals int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == obj.Named() {
			arrivals++
		}
	}, false)
	defer unsub()
	s.RecordEmitted(
		p,
		captureEmission(
			s,
			p,
			&Emission{
				Children:  []Child{{Obj: obj, Doc: doc}, {Obj: foreign, Doc: foreignDoc}},
				Manifests: testutil.DocumentOccurrences([]map[string]any{foreignDoc, doc}),
				Complete:  true,
			},
		),
		nil,
	)
	{
		assert.FatalIf(t, arrivals != 0, "equal seed acquired document with %d arrivals", arrivals)
	}
	_, _, accepted := s.GetEmission(p)
	{
		assert.FatalIf(
			t,
			len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, foreignDoc) ||
				!manifest.ValueEqual(
					accepted.Manifests[1].Document,
					doc,
				),
			"%s",
			"suppressed arrival lost authoritative document",
		)
	}
	{
		got := s.Productions(obj.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "accepted support = %v", got)
	}
	next := manifest.DeepCopyMap(doc)
	next["apiVersion"] = manifest.SourceDomain + "/v1beta2"
	s.RecordEmitted(
		p,
		captureEmission(
			s,
			p,
			&Emission{
				Children:  []Child{{Obj: obj, Doc: next}, emissionChild("other", "value")},
				Manifests: testutil.DocumentOccurrences([]map[string]any{next}),
			},
		),
		nil,
	)
	{
		assert.FatalIf(t, arrivals != 1, "changed authoritative document arrivals = %d, want 1", arrivals)
	}
	_, _, accepted = s.GetEmission(p)
	{
		assert.FatalIf(t, len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, next) ||
			!manifest.ValueEqual(
				accepted.Manifests[1].Document,
				foreignDoc,
			), "partial version update lost cross-group documents: %v", accepted.Manifests)
	}
	{
		got := s.Productions(foreign.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "cross-group support = %v", got)
	}
}

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
	{
		assert.FatalIf(
			t,
			attempt.Fingerprint != "" || s.GetArtifact(p, ArtifactContent).(*Emission).Fingerprint != "",
			"%s",
			"partial payload publication retained successful fingerprint eligibility",
		)
	}
	attempt.Children, attempt.Complete = []Child{child}, true
	attempt.Fingerprint = "same"
	s.RecordEmitted(p, &attempt, nil)
	{
		assert.FatalIf(
			t,
			s.GetArtifact(target, ArtifactContent) != payload,
			"%s",
			"same-attempt nil finalization removed payload",
		)
	}
	_, _, replay := s.GetEmission(p)
	defer replay.Finish()
	replay.Complete = true
	s.RecordEmitted(p, &replay, nil)
	{
		assert.FatalIf(t, s.GetArtifact(target, ArtifactContent) != payload, "%s", "borrowed replay removed payload")
	}
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
	{
		assert.FatalIf(
			t,
			s.GetArtifact(target, ArtifactContent) != nil,
			"%s",
			"fresh declaration-only replacement retained bytes",
		)
	}
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
	{
		assert.FatalIf(
			t,
			s.GetArtifact(target, ArtifactContent) != nil ||
				s.GetArtifact(other.Obj.Named(), ArtifactContent) != payload,
			"%s",
			"new attempt borrowed an earlier payload for its fresh declaration-only child",
		)
	}
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
	{
		assert.FatalIf(
			t,
			!slices.Equal(updates, []*SourceArtifact{second}),
			"%s",
			"payload-only replacement did not notify the whole descriptor exactly once",
		)
	}
	obj, doc, input := s.GetEmission(target)
	defer input.Finish()
	assert.Equal(t, obj, child.Obj)
	assert.Diff(t, doc, child.Doc)
	assert.Equal(t, s.GetArtifact(target, ArtifactContent), Artifact(second))
	equal := *second
	equal.FS = fstest.MapFS{"file": {Data: []byte("second")}}
	s.RecordEmitted(owner.Named(), &next, map[manifest.NamedResource]*SourceArtifact{target: &equal})
	{
		assert.FatalIf(
			t,
			!slices.Equal(updates, []*SourceArtifact{second}),
			"%s",
			"semantically equal payload with distinct backing notified readers",
		)
	}
	s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
	{
		assert.FatalIf(
			t,
			s.GetArtifact(target, ArtifactContent) != nil || !slices.Equal(updates, []*SourceArtifact{second, nil}),
			"%s",
			"last payload withdrawal did not remove authority and notify readers",
		)
	}
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

func TestGetEmission_CompleteFingerprint(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			t.Parallel()
			s := New()
			owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(owner)
			_, _, next := s.GetEmission(owner.Named())
			next.Complete, next.Fingerprint = true, "same"
			s.RecordEmitted(owner.Named(), &next, nil)
			sh := s.shardFor(owner.Named())
			sh.mu.Lock()
			accepted := *sh.slots[owner.Named()].current.Load()
			accepted.Complete = complete
			sh.slots[owner.Named()].current.Store(&accepted)
			sh.mu.Unlock()
			_, _, attempt := s.GetEmission(owner.Named())
			defer attempt.Finish()
			want := ""
			if complete {
				want = "same"
			}
			output, replayable := attempt.Replay("same")
			assert.Equal(t, replayable, complete)
			assert.Equal(t, output.Fingerprint, want)
			assert.Equal(t, attempt.Complete, false)
		})
	}
}

func TestGetEmission_AuthoritativeDocument(t *testing.T) {
	t.Parallel()
	for _, batch := range []bool{false, true} {
		t.Run(strconv.FormatBool(batch), func(t *testing.T) {
			t.Parallel()
			testGetEmissionAuthoritativeDocumentCase(t, &batch)
		},
		)
	}
}

func TestGetEmission_SelectedBinding(t *testing.T) {
	t.Parallel()
	s := New()
	parent := ownerID("parent")
	s.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
	obj := &manifest.RawObject{
		APIVersion: "source.extensions.fluxcd.io/v1beta1",
		Kind:       "ArtifactGenerator",
		Name:       "generator",
		Namespace:  "apps",
	}
	child := Child{Obj: obj, Doc: map[string]any{"apiVersion": obj.APIVersion, "kind": obj.Kind}}
	first := sourceBinding{
		source:   manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "first"},
		artifact: &SourceArtifact{Key: "first"},
	}
	second := sourceBinding{
		source: manifest.NamedResource{
			Group: manifest.SourceDomain,
			Kind:  manifest.KindGitRepository,
			Name:  "second",
		},
		artifact: &SourceArtifact{Key: "second"},
	}
	s.RecordEmitted(parent, &Emission{Children: []Child{child}, Complete: true, binding: first}, nil)
	_, _, attempt := s.GetEmission(obj.Named())
	defer attempt.Finish()
	attempt.Fingerprint, attempt.Complete = "success", true
	s.RecordEmitted(obj.Named(), &attempt, nil)
	accepted := s.GetArtifact(obj.Named(), ArtifactContent)
	{
		assert.FatalIf(t, accepted == nil, "%s", "fresh inventory unavailable")
	}
	s.RecordEmitted(
		parent,
		&Emission{Children: []Child{child}, Complete: true, binding: first, Fingerprint: "changed"},
		nil,
	)
	_, doc, equivalent := s.GetEmission(obj.Named())
	defer equivalent.Finish()
	{
		assert.FatalIf(t, equivalent.inputHead.binding != first || !manifest.ValueEqual(doc, child.Doc) ||
			func() bool { output, ok := equivalent.Replay("success"); return !ok || output.Fingerprint != "success" }() ||
			s.GetArtifact(
				obj.Named(),
				ArtifactContent,
			) != accepted, "%s", "equivalent declaration storage invalidated accepted work")
	}
	s.RecordEmitted(parent, &Emission{Children: []Child{child}, Complete: true, binding: second}, nil)
	_, doc, rebound := s.GetEmission(obj.Named())
	defer rebound.Finish()
	{
		assert.FatalIf(
			t,
			rebound.inputHead.binding != second || !manifest.ValueEqual(doc, child.Doc) || rebound.Fingerprint != "" ||
				s.GetArtifact(
					obj.Named(),
					ArtifactContent,
				) != nil,
			"%s",
			"selected document and binding did not invalidate old replay together",
		)
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
			sourcecache.Policy{MaxAge: sourcecache.Forever},
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

func testGetEmissionAuthoritativeDocumentCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	raw := &manifest.RawObject{
		APIVersion: "source.extensions.fluxcd.io/v1beta1",
		Kind:       "ArtifactGenerator",
		Name:       "generator",
		Namespace:  "apps",
		Spec:       map[string]any{"value": "first"},
	}
	doc := map[string]any{"metadata": map[string]any{"uid": "authored"}, "spec": raw.Spec}
	added := 0
	defer s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == raw.Named() {
			added++
		}
	}, false)()
	s.AddObject(raw, doc)
	{
		assert.FatalIf(t, added != 1, "initial admission events = %d, want 1", added)
	}
	if *testCaseInput {
		s.AddObjects([]manifest.BaseManifest{raw.Clone()})
	} else {
		s.AddObject(raw.Clone())
	}
	{
		assert.FatalIf(t, added != 1, "equal object-only admission events = %d, want 1", added)
	}
	_, retained, attempt := s.GetEmission(raw.Named())
	attempt.Finish()
	{
		assert.FatalIf(t, !manifest.ValueEqual(retained, doc), "%s", "equal object-only admission discarded document")
	}
	changed := raw.Clone()
	changed.Spec["value"] = "second"
	if *testCaseInput {
		s.AddObjects([]manifest.BaseManifest{changed})
	} else {
		s.AddObject(changed)
	}
	{
		assert.FatalIf(t, added != 2, "changed object-only admission events = %d, want 2", added)
	}
	_, retained, attempt = s.GetEmission(raw.Named())
	attempt.Finish()
	{
		assert.FatalIf(t, retained != nil, "%s", "changed object-only admission retained stale authority")
	}
	replacement := map[string]any{"metadata": map[string]any{"uid": "replacement"}}
	var captured ReadSet
	captured.Init(s, ownerID("document-reader"), nil)
	captured.GetObject(raw.Named())
	s.AddObject(changed, replacement)
	{
		assert.FatalIf(
			t,
			captured.Valid(),
			"%s",
			"meaningful document-only replacement kept the captured generator valid",
		)
	}
	captured.Retire()
	{
		assert.FatalIf(t, added != 3, "replacement document admission events = %d, want 3", added)
	}
	_, retained, attempt = s.GetEmission(raw.Named())
	attempt.Finish()
	{
		assert.FatalIf(
			t,
			!manifest.ValueEqual(retained, replacement),
			"%s",
			"explicit document did not replace equal object's authority",
		)
	}
	s.AddObject(changed, replacement)
	{
		assert.FatalIf(t, added != 3, "equal document admission events = %d, want 3", added)
	}
	_, _, completed := s.GetEmission(raw.Named())
	defer completed.Finish()
	completed.Complete = true
	s.RecordEmitted(raw.Named(), &completed, nil)
	{
		assert.FatalIf(
			t,
			s.GetArtifact(raw.Named(), ArtifactContent) == nil,
			"%s",
			"completed document input is unavailable",
		)
	}
	s.AddObject(changed, doc)
	{
		assert.FatalIf(
			t,
			s.GetArtifact(raw.Named(), ArtifactContent) != nil,
			"%s",
			"changed document retained completed work for the same object pointer",
		)
	}
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

func testRecordEmittedPayloadPrecedenceCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedPayloadPrecedenceCaseCompletionState,
) {
	t.Helper()
	var opened fs.File
	start, done := make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	for i := range *scope.owners {
		tasks.Go(t.Context(), (*scope.owners)[i].Name, func(context.Context) {
			if i == 1 {
				tasks.YieldSlot(func() { <-start })
			}
			(*scope.s).RecordEmitted(
				(*scope.owners)[i],
				&Emission{
					Children:  []Child{(*scope.children)[i]},
					Manifests: testutil.DocumentOccurrences([]map[string]any{(*scope.children)[i].Doc}),
					Complete:  true,
				},
				map[manifest.NamedResource]*SourceArtifact{(*scope.target): (*scope.payloads)[i]},
			)
			if i == 0 {
				close(start)
				tasks.YieldSlot(func() { <-done })
			} else {
				close(done)
			}
		})
	}
	tasks.BlockTillDone()
	{
		assert.FatalIf(t, tasks.Failures() != 0, "%s", "publication panicked")
	}
	(*scope.s).RecordEmitted(
		(*scope.zz),
		&Emission{
			Children:  []Child{(*scope.declaration)},
			Manifests: testutil.DocumentOccurrences([]map[string]any{scope.declaration.Doc}),
			Complete:  true,
		},
		nil,
	)
	assert.Diff(
		t,
		(*scope.s).Warnings(),
		[]manifest.Warning{
			{
				Resource: (*scope.target),
				Subject:  manifest.DiagnosticFor((*scope.target), nil),
				Related:  []manifest.NamedResource{(*scope.a), (*scope.z), (*scope.zz), (*scope.zz)},
				Category: manifest.WarnConflictingProducers,
				Count:    1,
				Message:  "conflicting producers; using " + scope.zz.String(),
				Detail:   []string{scope.a.String(), scope.z.String(), scope.zz.String()},
			},
		},
	)
	seen := 0
	unsubscribe := (*scope.s).AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
		if id == (*scope.target) {
			seen++
			{
				assert.ErrorIf(t, value != (*scope.last), "%s", "replay selected a different payload")
			}
		}
	}, true)
	unsubscribe()
	{
		assert.FatalIf(t, seen != 1, "generated payload replay count = %d", seen)
	}
	{
		assert.FatalIf(
			t,
			(*scope.s).EmissionOwner((*scope.target)) != (*scope.zz) ||
				(*scope.s).GetArtifact((*scope.target), ArtifactContent) != (*scope.last),
			"%s",
			"declaration winner suppressed greatest byte contributor",
		)
	}
	checkDeclaration := func() {
		t.Helper()
		obj, doc, attempt := (*scope.s).GetEmission((*scope.target))
		defer attempt.Finish()
		assert.Equal(t, obj, scope.declaration.Obj)
		assert.Diff(t, doc, scope.declaration.Doc)
		assert.Equal(t, (*scope.s).EmissionOwner((*scope.target)), (*scope.zz))
	}
	checkDeclaration()
	(*scope.s).AddObject(scope.declaration.Obj, scope.declaration.Doc)
	assert.Equal(t, (*scope.s).DeleteObject((*scope.target)), true)
	{
		assert.FatalIf(
			t,
			(*scope.s).GetArtifact((*scope.target), ArtifactContent) != (*scope.last),
			"%s",
			"static deletion discarded the surviving emitted payload",
		)
	}
	checkDeclaration()
	{
		data, readFileErr := fs.ReadFile((*scope.first).FS, "file")
		assert.FatalIf(t, readFileErr != nil || string(data) != "a", "losing payload = %q, %v", data, readFileErr)
	}
	opened, (*scope.err) = (*scope.last).FS.Open("file")
	{
		assert.NoError(t, (*scope.err))
	}
	defer func() { _ = opened.Close() }()
	(*scope.s).RecordEmitted((*scope.z), &Emission{Complete: true}, nil)
	{
		assert.FatalIf(
			t,
			(*scope.s).GetArtifact((*scope.target), ArtifactContent) != (*scope.first),
			"%s",
			"withdrawal did not select the whole surviving descriptor",
		)
	}
	checkDeclaration()
	{
		data, err := fs.ReadFile(
			(*scope.s).GetArtifact((*scope.target), ArtifactContent).(*SourceArtifact).FS,
			"file",
		)
		assert.FatalIf(t, err != nil ||
			string(data) != "a", "surviving payload = %q, %v", data, err)
	}
	(*scope.s).RecordEmitted((*scope.a), &Emission{Complete: true}, nil)
	{
		assert.FatalIf(
			t,
			(*scope.s).GetObject((*scope.target)) == nil ||
				(*scope.s).GetArtifact((*scope.target), ArtifactContent) != nil,
			"%s",
			"declaration-only survivor retained byte authority",
		)
	}
	checkDeclaration()
	{
		data, err := io.ReadAll(opened)
		assert.FatalIf(
			t,
			err != nil || string(data) != "z",
			"already-open withdrawn backing = %q, %v",
			data,
			err,
		)
	}
	{
		assert.FatalIf(
			t,
			(*scope.peer).GetArtifact((*scope.target), ArtifactContent) != (*scope.first),
			"%s",
			"withdrawal discarded another store's shared cache backing",
		)
	}
	(*scope.peer).RecordEmitted((*scope.a), &Emission{Complete: true}, nil)
	for _, payload := range []*SourceArtifact{(*scope.first), (*scope.last)} {
		{
			data, err := fs.ReadFile(
				payload.FS,
				"file",
			)
			assert.FatalIf(t, err != nil ||
				string(data) != payload.Metadata["origin"], "unowned cache backing = %q, %v", data, err)
		}
	}
}
