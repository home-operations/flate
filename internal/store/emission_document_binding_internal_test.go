package store

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

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
	assert.FatalIf(t, accepted == nil, "%s", "fresh inventory unavailable")
	s.RecordEmitted(
		parent,
		&Emission{Children: []Child{child}, Complete: true, binding: first, Fingerprint: "changed"},
		nil,
	)
	_, doc, equivalent := s.GetEmission(obj.Named())
	defer equivalent.Finish()
	assert.FatalIf(t, equivalent.inputHead.binding != first || !manifest.ValueEqual(doc, child.Doc) ||
		func() bool { output, ok := equivalent.Replay("success"); return !ok || output.Fingerprint != "success" }() ||
		s.GetArtifact(
			obj.Named(),
			ArtifactContent,
		) != accepted, "%s", "equivalent declaration storage invalidated accepted work")
	s.RecordEmitted(parent, &Emission{Children: []Child{child}, Complete: true, binding: second}, nil)
	_, doc, rebound := s.GetEmission(obj.Named())
	defer rebound.Finish()
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

func TestRecordEmitted_AbsentArtifactPublication(t *testing.T) {
	t.Parallel()
	for _, rebound := range []bool{false, true} {
		t.Run(fmt.Sprintf("rebound=%t", rebound), func(t *testing.T) {
			t.Parallel()
			st := New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			owner := &manifest.RawObject{
				APIVersion: "example.test/v1",
				Kind:       "Producer",
				Name:       "owner",
				Namespace:  "apps",
			}
			st.AddObject(parent)
			binding := sourceBinding{source: manifest.BootstrapSourceID, artifact: &SourceArtifact{Key: "first"}}
			st.RecordEmitted(
				parent.Named(),
				&Emission{Children: []Child{{Obj: owner}}, Complete: true, binding: binding},
				nil,
			)
			publish := func() {
				var reads ReadSet
				reads.Init(st, owner.Named(), nil)
				_, _, next := reads.GetEmission(owner.Named())
				defer next.Finish()
				next.Complete, next.Fingerprint = true, "same"
				st.RecordEmitted(owner.Named(), &next, nil)
			}
			if rebound {
				publish()
				binding.artifact = &SourceArtifact{Key: "second"}
				st.RecordEmitted(
					parent.Named(),
					&Emission{Children: []Child{{Obj: owner}}, Complete: true, binding: binding},
					nil,
				)
			}
			var pending ReadSet
			pending.Init(st, ownerID("consumer"), nil)
			defer pending.Retire()
			captured, published := make(chan struct{}), make(chan struct{})
			tasks := task.NewBounded(2)
			tasks.Go(t.Context(), "read", func(context.Context) {
				assert.Equal(t, pending.GetArtifact(owner.Named(), ArtifactContent), Artifact(nil))
				assert.Equal(t, pending.Valid(), true)
				close(captured)
				tasks.YieldSlot(func() { <-published })
			})
			tasks.Go(t.Context(), "publish", func(context.Context) {
				tasks.YieldSlot(func() { <-captured })
				publish()
				close(published)
			})
			tasks.BlockTillDone()
			assert.Equal(t, tasks.Failures(), int64(0))
			assert.Equal(t, pending.Valid(), false)
			assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), pending.ID()), true)
			assert.Equal(t, pending.Valid(), false)
			assert.Equal(t, pending.GetArtifact(owner.Named(), ArtifactContent), Artifact(nil))
			var fresh ReadSet
			fresh.Init(st, ownerID("fresh"), nil)
			defer fresh.Retire()
			accepted, ok := fresh.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
			assert.FatalIf(t, !ok, "%s", "fresh execution lacks accepted output")
			assert.Equal(t, accepted.Complete && accepted.OutputReads.Valid() && fresh.Valid(), true)
		})
	}
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
	assert.FatalIf(t, added != 1, "initial admission events = %d, want 1", added)
	if *testCaseInput {
		s.AddObjects([]manifest.BaseManifest{raw.Clone()})
	} else {
		s.AddObject(raw.Clone())
	}
	assert.FatalIf(t, added != 1, "equal object-only admission events = %d, want 1", added)
	_, retained, attempt := s.GetEmission(raw.Named())
	attempt.Finish()
	assert.FatalIf(t, !manifest.ValueEqual(retained, doc), "%s", "equal object-only admission discarded document")
	changed := raw.Clone()
	changed.Spec["value"] = "second"
	if *testCaseInput {
		s.AddObjects([]manifest.BaseManifest{changed})
	} else {
		s.AddObject(changed)
	}
	assert.FatalIf(t, added != 2, "changed object-only admission events = %d, want 2", added)
	_, retained, attempt = s.GetEmission(raw.Named())
	attempt.Finish()
	assert.FatalIf(t, retained != nil, "%s", "changed object-only admission retained stale authority")
	replacement := map[string]any{"metadata": map[string]any{"uid": "replacement"}}
	var captured ReadSet
	captured.Init(s, ownerID("document-reader"), nil)
	captured.GetObject(raw.Named())
	s.AddObject(changed, replacement)
	assert.FatalIf(
		t,
		captured.Valid(),
		"%s",
		"meaningful document-only replacement kept the captured generator valid",
	)
	captured.Retire()
	assert.FatalIf(t, added != 3, "replacement document admission events = %d, want 3", added)
	_, retained, attempt = s.GetEmission(raw.Named())
	attempt.Finish()
	assert.FatalIf(
		t,
		!manifest.ValueEqual(retained, replacement),
		"%s",
		"explicit document did not replace equal object's authority",
	)
	s.AddObject(changed, replacement)
	assert.FatalIf(t, added != 3, "equal document admission events = %d, want 3", added)
	_, _, completed := s.GetEmission(raw.Named())
	defer completed.Finish()
	completed.Complete = true
	s.RecordEmitted(raw.Named(), &completed, nil)
	assert.FatalIf(
		t,
		s.GetArtifact(raw.Named(), ArtifactContent) == nil,
		"%s",
		"completed document input is unavailable",
	)
	s.AddObject(changed, doc)
	assert.FatalIf(
		t,
		s.GetArtifact(raw.Named(), ArtifactContent) != nil,
		"%s",
		"changed document retained completed work for the same object pointer",
	)
}
