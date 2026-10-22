package store

import (
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
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "n", Name: "a"}
	events := 0
	var ids []manifest.NamedResource
	s.AddListener(EventArtifactUpdated, func(eventID manifest.NamedResource, _ any) {
		events++
		ids = append(ids, eventID)
	}, false)
	mk := func(key string) *SourceArtifact {
		return &SourceArtifact{LocalPath: "label", Key: key, FS: &hookFS{open: func(string) (fs.File, error) { return nil, fs.ErrNotExist }}}
	}
	s.SetArtifact(id, mk("pin"))
	s.SetArtifact(id, mk("pin"))
	if events != 1 {
		t.Fatalf("equal Key re-fired the artifact event: %d", events)
	}
	foreign := id
	foreign.Group = "example.test"
	s.SetArtifact(foreign, mk("pin"))
	assert.Equal(t, s.GetArtifact(id).(*SourceArtifact).Key, "pin")
	assert.Equal(t, s.GetArtifact(foreign).(*SourceArtifact).Key, "pin")
	assert.Diff(t, ids, []manifest.NamedResource{id, foreign})
	s.SetArtifact(id, mk("other"))
	if events != 3 {
		t.Fatalf("changed Key did not fire: %d", events)
	}
	for _, tc := range []struct{ revision, digest string }{
		{revision: "one", digest: "aaa"},
		{revision: "two", digest: "aaa"},
		{revision: "two", digest: "bbb"},
	} {
		artifact := mk("other")
		artifact.Revision, artifact.Digest = tc.revision, tc.digest
		before := events
		s.SetArtifact(id, artifact)
		if events != before+1 {
			t.Fatalf("generation %s/%s did not fire: %d", tc.revision, tc.digest, events)
		}
		equal := *artifact
		equal.FS = mk("other").FS
		s.SetArtifact(id, &equal)
		if events != before+1 {
			t.Fatal("equal family and generation re-fired the artifact event")
		}
	}

	for _, text := range []string{"old", "new"} {
		s.SetArtifact(id, &SourceArtifact{LocalPath: "label", FS: fstest.MapFS{"file": {Data: []byte(text)}}})
		got, err := fs.ReadFile(s.GetArtifact(id).(*SourceArtifact).FS, "file")
		if err != nil || string(got) != text {
			t.Fatalf("mutable source bytes = %q, %v; want %q", got, err, text)
		}
	}
	assert.Equal(t, s.GetArtifact(foreign).(*SourceArtifact).Key, "pin")
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
			st := New()
			events := 0
			st.AddListener(EventArtifactUpdated, func(manifest.NamedResource, any) { events++ }, false)
			first := &SourceArtifact{Key: "same", FS: tc.fs}
			st.SetArtifact(id, first)
			next := *first
			next.Warnings = []manifest.Warning{{Category: manifest.WarnSkippedSubmodule, Detail: []string{"module"}}}
			if next.FS != nil {
				next.FS = &hookFS{}
			}
			st.SetArtifact(id, &next)
			st.SetArtifact(id, first)
			if events != 1 {
				t.Fatalf("diagnostic metadata fired %d artifact events", events)
			}
		})
	}
}

func TestSetArtifact_DroppedPayloadReplacesEqualKey(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "n", Name: "a"}
	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)
	s.SetArtifact(id, &SourceArtifact{LocalPath: "label", Key: "pin", FS: &hookFS{}})
	s.SetArtifact(id, &SourceArtifact{LocalPath: "label", Key: "pin"})
	if got := s.GetArtifact(id).(*SourceArtifact); got.FS != nil {
		t.Fatal("republish without a payload kept the previous payload")
	}
	if events != 2 {
		t.Fatalf("dropped payload did not fire the artifact event: %d", events)
	}
}
