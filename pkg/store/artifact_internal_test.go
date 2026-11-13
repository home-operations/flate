package store

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type hookFS struct{ open func(string) (fs.File, error) }

func (h *hookFS) Open(name string) (fs.File, error) { return h.open(name) }

func TestSetArtifact_PayloadComparedByKey(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "n",
		Name:      "a",
	}
	events := 0
	var ids []manifest.NamedResource
	s.AddListener(EventArtifactUpdated, func(eventID manifest.NamedResource, _ any) {
		events++
		ids = append(ids, eventID)
	}, false)
	mk := func(key string) *SourceArtifact {
		return &SourceArtifact{
			LocalPath:          "label",
			Key:                key,
			Content:            key,
			FS:                 &hookFS{open: func(string) (fs.File, error) { return nil, fs.ErrNotExist }},
			ContentFingerprint: func(context.Context, fs.FS) (string, error) { t.Fatal("dedup reads content"); return "", nil },
		}
	}
	s.SetArtifact(id, mk("pin"))
	s.SetArtifact(id, mk("pin"))
	assert.FatalIf(t, events != 1, "equal Key re-fired the artifact event: %d", events)
	foreign := id
	foreign.Group = "example.test"
	s.SetArtifact(foreign, mk("pin"))
	assert.Equal(t, s.GetArtifact(id, ArtifactContent).(*SourceArtifact).Key, "pin")
	assert.Equal(t, s.GetArtifact(foreign, ArtifactContent).(*SourceArtifact).Key, "pin")
	assert.Diff(t, ids, []manifest.NamedResource{id, foreign})
	s.SetArtifact(id, mk("other"))
	assert.FatalIf(t, events != 3, "changed Key did not fire: %d", events)
	for _, tc := range []struct{ revision, digest string }{
		{revision: "one", digest: "aaa"},
		{revision: "two", digest: "aaa"},
		{revision: "two", digest: "bbb"},
	} {
		artifact := mk("other")
		artifact.Revision, artifact.Digest = tc.revision, tc.digest
		before := events
		s.SetArtifact(id, artifact)
		assert.FatalIf(t, events != before+1, "generation %s/%s did not fire: %d", tc.revision, tc.digest, events)
		equal := *artifact
		equal.FS = mk("other").FS
		s.SetArtifact(id, &equal)
		assert.FatalIf(t, events != before+1, "%s", "equal family and generation re-fired the artifact event")
	}

	for _, text := range []string{"old", "new"} {
		s.SetArtifact(id, &SourceArtifact{LocalPath: "label", FS: fstest.MapFS{"file": {Data: []byte(text)}}})
		got, err := fs.ReadFile(s.GetArtifact(id, ArtifactContent).(*SourceArtifact).FS, "file")
		assert.FatalIf(
			t,
			err != nil || string(got) != text,
			"mutable source bytes = %q, %v; want %q",
			got,
			err,
			text,
		)
	}
	assert.Equal(t, s.GetArtifact(foreign, ArtifactContent).(*SourceArtifact).Key, "pin")
	assert.Diff(t, ids, []manifest.NamedResource{id, foreign, id, id, id, id, id, id})
	for _, tc := range []struct {
		name string
		fs   fs.FS
	}{
		{name: "readable", fs: &hookFS{}},
		{name: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSetArtifactPayloadComparedByKeyCase(t, &testSetArtifactPayloadComparedByKeyCaseInputs{id: &id, tc: &tc})
		},
		)
	}
}

func TestSetArtifact_DroppedPayloadReplacesEqualKey(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "n",
		Name:      "a",
	}
	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)
	s.SetArtifact(id, &SourceArtifact{LocalPath: "label", Key: "pin", Content: "pin", FS: &hookFS{}})
	s.SetArtifact(id, &SourceArtifact{LocalPath: "label", Key: "pin"})
	{
		got := s.GetArtifact(id, ArtifactContent).(*SourceArtifact)
		assert.FatalIf(t, got.FS != nil, "%s", "republish without a payload kept the previous payload")
	}
	assert.FatalIf(t, events != 2, "dropped payload did not fire the artifact event: %d", events)
}

func TestSourceArtifact_HistoryIdentity(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "content"}
	for _, mode := range []string{"generation", "alias", "foreign", "unavailable", "stale", "composed", "distinct composition", "local pin"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testSourceArtifactHistoryIdentityCase(t, &testSourceArtifactHistoryIdentityCaseInputs{id: &id, mode: &mode})
		},
		)
	}
}

type testSetArtifactPayloadComparedByKeyCaseInputs struct {
	id *manifest.NamedResource
	tc *struct {
		name string
		fs   fs.FS
	}
}

func testSetArtifactPayloadComparedByKeyCase(
	t *testing.T,
	testCaseInput *testSetArtifactPayloadComparedByKeyCaseInputs,
) {
	t.Helper()

	st := New()
	events := 0
	st.AddListener(EventArtifactUpdated, func(manifest.NamedResource, any) { events++ }, false)
	first := &SourceArtifact{Key: "same", Content: "same", FS: testCaseInput.tc.fs}
	st.SetArtifact((*testCaseInput.id), first)
	next := *first
	next.Warnings = []manifest.Warning{{Category: manifest.WarnSkippedSubmodule, Detail: []string{"module"}}}
	assert.Equal(t, artifactValueEqual(first, &next), true)
	if next.FS != nil {
		next.FS = &hookFS{}
	}
	st.SetArtifact((*testCaseInput.id), &next)
	st.SetArtifact((*testCaseInput.id), first)
	assert.FatalIf(t, events != 1, "diagnostic metadata fired %d artifact events", events)
}

type testSourceArtifactHistoryIdentityCaseInputs struct {
	id   *manifest.NamedResource
	mode *string
}

func testSourceArtifactHistoryIdentityCase(t *testing.T, testCaseInput *testSourceArtifactHistoryIdentityCaseInputs) {
	t.Helper()

	input := &SourceArtifact{
		Kind:      testCaseInput.id.Kind,
		URL:       "https://fixture.invalid/repo",
		Key:       "family",
		Content:   "before",
		LocalPath: "/snapshot",
		FS:        fstest.MapFS{},
	}
	next := *input
	next.Content, next.LocalPath = "after", "/other-snapshot"
	if *testCaseInput.mode == "composed" || *testCaseInput.mode == "distinct composition" {
		input.LocalRoot, input.LocalPath = "/sources/content", "/sources/content"
		next.LocalRoot, next.LocalPath, next.Key = input.LocalRoot, input.LocalPath, "new-input-generation"
		if *testCaseInput.mode == "distinct composition" {
			next.LocalRoot, next.LocalPath = "/sources/other", "/sources/other"
		}
	}
	if *testCaseInput.mode == "local pin" {
		input.LocalRoot, next.LocalRoot = "/origin", "/origin"
		next.Key = "other-pin"
	}
	if (*testCaseInput.mode) == "alias" {
		input.InputAlias, next.InputAlias = true, true
		input.LocalRoot, next.LocalRoot = "/origin", "/origin"
	}
	if (*testCaseInput.mode) == "foreign" {
		next.URL = "https://fixture.invalid/other"
	}
	if (*testCaseInput.mode) == "unavailable" {
		next.FS = nil
	}
	if (*testCaseInput.mode) == "stale" {
		st := New()
		object := &manifest.ConfigMap{Name: "input"}
		st.AddObject(object)
		var reads ReadSet
		reads.Init(st, object.Named(), nil)
		defer reads.Retire()
		reads.GetObject(object.Named())
		st.AddObject(&manifest.ConfigMap{Name: "input", Data: map[string]any{"value": "changed"}})
		next.Reads = &reads
	}
	identity := next.HistoryIdentity((*testCaseInput.id))
	assert.Equal(
		t,
		identity == input.HistoryIdentity((*testCaseInput.id)),
		(*testCaseInput.mode) == "generation" || (*testCaseInput.mode) == "alias" || *testCaseInput.mode == "composed",
	)
	assert.Equal(
		t,
		identity == (manifest.SourceIdentity{}),
		(*testCaseInput.mode) == "unavailable" || (*testCaseInput.mode) == "stale",
	)
	if (*testCaseInput.mode) == "alias" {
		bootstrap := &SourceArtifact{LocalPath: "/origin", FS: fstest.MapFS{}}
		assert.Equal(t, identity, bootstrap.HistoryIdentity(manifest.BootstrapSourceID))
	}
}
