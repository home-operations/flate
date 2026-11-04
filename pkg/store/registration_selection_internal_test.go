package store

import (
	"fmt"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

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
