package store

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type hookFS struct{ open func(string) (fs.File, error) }

func (h *hookFS) Open(name string) (fs.File, error) { return h.open(name) }

func TestSourceArtifact_HistoryInput(t *testing.T) {
	t.Parallel()
	assert.Equal(t, (*SourceArtifact)(nil).HistoryInput(), ArtifactHistory{})
	artifact := &SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "first", Key: "family", Content: "generation",
		Revision: "revision", Digest: "digest", LocalPath: "/source", LocalRoot: "/origin", InputAlias: true,
		FS: fstest.MapFS{}, Metadata: map[string]string{"org.opencontainers.image.revision": "origin"},
		Reads: &ReadSet{}, ContentFingerprint: func(context.Context, fs.FS) (string, error) {
			t.Fatal("history projection evaluated content")
			return "", nil
		},
	}
	prior := artifact.HistoryInput()
	assert.Equal(t, prior, ArtifactHistory{
		Present:       true,
		Readable:      true,
		Kind:          artifact.Kind,
		Key:           "family",
		Content:       "generation",
		Digest:        "digest",
		LocalRoot:     "/origin",
		LocalPath:     "/source",
		InputAlias:    true,
		Revision:      "revision",
		Origin:        "origin",
		OriginPresent: true, URL: "first",
	})
	material := artifact.Input(ArtifactContent | ArtifactRevision | ArtifactOrigin)
	artifact.URL = "second"
	assert.Equal(t, artifact.Input(ArtifactContent|ArtifactRevision|ArtifactOrigin), material)
	assert.Equal(t, artifact.HistoryInput() == prior, false)
	artifact.Metadata["org.opencontainers.image.revision"] = "changed"
	artifact.Revision = "changed"
	assert.Equal(t, prior.URL, "first")
	assert.Equal(t, prior.Origin, "origin")
	assert.Equal(t, prior.Revision, "revision")
	assert.Equal(t, map[ArtifactHistory]bool{prior: true}[prior], true)
}

func TestSourceArtifact_Coordinates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, root, source, index string
		input                     *SourceArtifact
		known                     bool
	}{
		{name: "equal", root: "/work", input: &SourceArtifact{LocalPath: "/work"}, source: "file", index: "file", known: true},
		{name: "normalized", root: "/work", input: &SourceArtifact{LocalPath: "/work"}, source: "dir/../file", index: "file", known: true},
		{name: "nested", root: "/work/nested", input: &SourceArtifact{LocalPath: "/work"}, source: "nested/file", index: "file", known: true},
		{name: "source directory", root: "/work", input: &SourceArtifact{LocalPath: "/work/nested"}, source: ".", index: "nested", known: true},
		{name: "index directory", root: "/work/nested", input: &SourceArtifact{LocalPath: "/work"}, source: "nested", index: ".", known: true},
		{name: "empty source directory", root: "/work", input: &SourceArtifact{LocalPath: "/work/nested"}, index: "nested", known: true},
		{name: "alias", root: "/work/nested", input: &SourceArtifact{LocalPath: "/cache", LocalRoot: "/work", InputAlias: true, Content: "pinned"}, source: "nested/file", index: "file", known: true},
		{name: "mutable", root: "/work/nested", input: &SourceArtifact{LocalPath: "/cache", LocalRoot: "/work"}, source: "nested/file", index: "file", known: true},
		{name: "pinned", root: "/work", input: &SourceArtifact{LocalPath: "/cache", LocalRoot: "/work", Content: "pinned"}, source: "file", index: "../cache/file", known: true},
		{name: "external", root: "/work/nested", input: &SourceArtifact{LocalPath: "/other"}, source: "file", index: "../../other/file", known: true},
		{name: "absent discovery root", input: &SourceArtifact{LocalPath: "/work"}, source: "dir/../file", index: "file", known: true},
		{name: "absent source root", root: "/work", input: &SourceArtifact{}, source: "dir/../file", index: "file", known: true},
		{name: "nil artifact", root: "/work", source: "dir/../file", index: "file", known: true},
		{name: "incompatible source root", root: "/work", input: &SourceArtifact{LocalPath: "relative"}, source: "file"},
		{name: "incompatible discovery root", root: "relative", input: &SourceArtifact{LocalPath: "/work"}, source: "file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			indexed, known := tc.input.IndexPath(tc.root, tc.source)
			assert.Equal(t, indexed, tc.index)
			assert.Equal(t, known, tc.known)
			source, known := tc.input.SourcePath(tc.root, tc.index)
			want := ""
			if tc.known {
				want = filepath.ToSlash(filepath.Clean(tc.source))
			}
			assert.Equal(t, source, want)
			assert.Equal(t, known, tc.known)
		})
	}
}

func TestSourceArtifact_SameMaterial(t *testing.T) {
	t.Parallel()
	st := New()
	object := &manifest.ConfigMap{Name: "input"}
	st.AddObject(object)
	var stale ReadSet
	stale.Init(st, ownerID("reader"), nil)
	t.Cleanup(stale.Retire)
	stale.GetObject(object.Named())
	st.AddObject(&manifest.ConfigMap{Name: "input", Data: map[string]any{"value": "changed"}})
	assert.Equal(t, stale.Valid(), false)
	immutable := &SourceArtifact{Content: "generation", FS: &hookFS{open: func(string) (fs.File, error) {
		t.Fatal("material equality opened a filesystem")
		return nil, fs.ErrNotExist
	}}}
	peer := *immutable
	mutable := &SourceArtifact{FS: immutable.FS, Revision: "revision"}
	independent := *mutable
	wrapper := mutable.WithReads(&ReadSet{}).WithReads(&stale)
	all := ArtifactContent | ArtifactRevision | ArtifactOrigin
	for _, read := range []ArtifactRead{0, ArtifactContent, ArtifactRevision, ArtifactOrigin, ArtifactContent | ArtifactRevision, ArtifactContent | ArtifactOrigin, ArtifactRevision | ArtifactOrigin, all} {
		t.Run(fmt.Sprintf("identity/%d", read), func(t *testing.T) {
			t.Parallel()
			var absent *SourceArtifact
			assert.Equal(t, absent.SameMaterial(nil, read), true)
			assert.Equal(t, mutable.SameMaterial(mutable, read), true)
			assert.Equal(t, wrapper.SameMaterial(wrapper, read), true)
			assert.Equal(t, absent.SameMaterial(immutable, read), false)
			assert.Equal(t, immutable.SameMaterial(absent, read), false)
		})
	}
	for _, tc := range []struct {
		name        string
		first, next *SourceArtifact
		read        ArtifactRead
		equal       bool
	}{
		{"immutable peers", immutable, &peer, ArtifactContent, true},
		{"mutable wrappers", mutable, wrapper, all, true},
		{"independent mutable", mutable, &independent, ArtifactContent, false},
		{"metadata revision", mutable, &independent, ArtifactRevision, true},
		{"metadata origin", mutable, &independent, ArtifactOrigin, true},
		{"metadata union", mutable, &independent, ArtifactRevision | ArtifactOrigin, true},
		{"empty mask", immutable, mutable, 0, true},
		{"unreadable peers", &SourceArtifact{}, &SourceArtifact{}, ArtifactContent, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.first.SameMaterial(tc.next, tc.read), tc.equal)
			assert.Equal(t, tc.next.SameMaterial(tc.first, tc.read), tc.equal)
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*SourceArtifact)
		read   ArtifactRead
	}{
		{"kind", func(a *SourceArtifact) { a.Kind = "other" }, ArtifactContent},
		{"key", func(a *SourceArtifact) { a.Key = "other" }, ArtifactContent},
		{"generation", func(a *SourceArtifact) { a.Content = "other" }, ArtifactContent},
		{"digest", func(a *SourceArtifact) { a.Digest = "other" }, ArtifactContent},
		{"local root", func(a *SourceArtifact) { a.LocalRoot = "other" }, ArtifactContent},
		{"local path", func(a *SourceArtifact) { a.LocalPath = "other" }, ArtifactContent},
		{"alias", func(a *SourceArtifact) { a.InputAlias = true }, ArtifactContent},
		{"availability", func(a *SourceArtifact) { a.FS = nil }, ArtifactContent},
		{"revision", func(a *SourceArtifact) { a.Revision = "other" }, ArtifactRevision},
		{"origin presence", func(a *SourceArtifact) { a.Metadata = map[string]string{"org.opencontainers.image.revision": ""} }, ArtifactOrigin},
		{"origin value", func(a *SourceArtifact) { a.Metadata = map[string]string{"org.opencontainers.image.revision": "other"} }, ArtifactOrigin},
		{"URL", func(a *SourceArtifact) { a.URL = "other" }, 0},
		{"reads", func(a *SourceArtifact) { a.Reads = &stale }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			next := *immutable
			tc.change(&next)
			assert.Equal(t, immutable.SameMaterial(&next, all), tc.read == 0)
			assert.Equal(t, immutable.SameMaterial(&next, all&^tc.read), true)
			if tc.read != 0 {
				assert.Equal(t, immutable.SameMaterial(&next, tc.read), false)
			}
		})
	}
	t.Cleanup(func() { assert.Equal(t, wrapper.Reads.Valid(), false) })
}

func BenchmarkSourceArtifact_SameMaterial(b *testing.B) {
	immutable := &SourceArtifact{Content: "generation", FS: fstest.MapFS{}}
	peer := *immutable
	mutable := &SourceArtifact{FS: immutable.FS, Revision: "revision"}
	independent := *mutable
	for _, tc := range []struct {
		name        string
		first, next *SourceArtifact
		read        ArtifactRead
		equal       bool
	}{
		{"identical", mutable, mutable, ArtifactContent, true},
		{"immutable", immutable, &peer, ArtifactContent, true},
		{"wrappers", mutable, mutable.WithReads(&ReadSet{}).WithReads(&ReadSet{}), ArtifactContent, true},
		{"independent", mutable, &independent, ArtifactContent, false},
		{"metadata", mutable, &independent, ArtifactRevision | ArtifactOrigin, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if tc.first.SameMaterial(tc.next, tc.read) != tc.equal {
					b.Fatal("unexpected material equality")
				}
			}
		})
	}
}

func TestArtifactValueEqual_SourceFacts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*SourceArtifact)
		equal  bool
	}{
		{name: "kind", change: func(a *SourceArtifact) { a.Kind = "other" }},
		{name: "URL", change: func(a *SourceArtifact) { a.URL = "other" }},
		{name: "local path", change: func(a *SourceArtifact) { a.LocalPath = "other" }},
		{name: "revision", change: func(a *SourceArtifact) { a.Revision = "other" }},
		{name: "content", change: func(a *SourceArtifact) { a.Content = "other" }},
		{name: "digest", change: func(a *SourceArtifact) { a.Digest = "other" }},
		{name: "size", change: func(a *SourceArtifact) { a.Size++ }},
		{name: "local root", change: func(a *SourceArtifact) { a.LocalRoot = "other" }},
		{name: "key", change: func(a *SourceArtifact) { a.Key = "other" }},
		{name: "input alias", change: func(a *SourceArtifact) { a.InputAlias = true }},
		{name: "metadata value", change: func(a *SourceArtifact) { a.Metadata = map[string]string{"origin": "other"} }},
		{name: "metadata key", change: func(a *SourceArtifact) { a.Metadata = map[string]string{"other": "revision"} }},
		{name: "equal metadata", change: func(a *SourceArtifact) { a.Metadata = map[string]string{"origin": "revision"} }, equal: true},
		{name: "filesystem absence", change: func(a *SourceArtifact) { a.FS = nil }},
		{name: "unknown content", change: func(a *SourceArtifact) { a.Content = "" }},
		{name: "filesystem handle", change: func(a *SourceArtifact) { a.FS = &hookFS{} }, equal: true},
		{name: "reads", change: func(a *SourceArtifact) { a.Reads = new(ReadSet) }, equal: true},
		{name: "material", change: func(a *SourceArtifact) { a.material = new(SourceArtifact) }, equal: true},
		{
			name: "warnings", equal: true,
			change: func(a *SourceArtifact) {
				a.Warnings = []manifest.Warning{{Category: manifest.WarnSkippedSubmodule, Detail: []string{"module"}}}
			},
		},
		{name: "fingerprint callback", change: func(a *SourceArtifact) { a.ContentFingerprint = nil }, equal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := &SourceArtifact{
				Kind: "GitRepository", URL: "https://fixture.invalid/repo", LocalPath: "/snapshot",
				Revision: "revision", Content: "content", Digest: "digest", Size: 1,
				LocalRoot: "/origin", Key: "key", Metadata: map[string]string{"origin": "revision"},
				FS: &hookFS{open: func(string) (fs.File, error) {
					t.Fatal("artifact equality opened a filesystem")
					return nil, fs.ErrNotExist
				}},
				ContentFingerprint: func(context.Context, fs.FS) (string, error) {
					t.Fatal("artifact equality called a fingerprint callback")
					return "", nil
				},
			}
			next := *first
			tc.change(&next)
			assert.Equal(t, artifactValueEqual(first, &next), tc.equal)
			assert.Equal(t, artifactValueEqual(&next, first), tc.equal)
		})
	}
}

func TestArtifactValueEqual_NilAndVariants(t *testing.T) {
	t.Parallel()
	mutable := &SourceArtifact{FS: &hookFS{}}
	emission := &Emission{}
	for _, tc := range []struct {
		name        string
		first, next Artifact
		equal       bool
	}{
		{name: "nil", equal: true},
		{name: "same source", first: mutable, next: mutable, equal: true},
		{name: "same emission", first: emission, next: emission, equal: true},
		{name: "nil sources", first: (*SourceArtifact)(nil), next: (*SourceArtifact)(nil), equal: true},
		{name: "nil emissions", first: (*Emission)(nil), next: (*Emission)(nil), equal: true},
		{name: "nil and source", next: &SourceArtifact{}},
		{name: "nil and emission", next: emission},
		{name: "nil and typed source", next: (*SourceArtifact)(nil)},
		{name: "nil and typed emission", next: (*Emission)(nil)},
		{name: "typed nil and source", first: (*SourceArtifact)(nil), next: &SourceArtifact{}},
		{name: "typed nil and emission", first: (*Emission)(nil), next: emission},
		{name: "mixed variants", first: &SourceArtifact{}, next: emission},
		{name: "mixed nil variants", first: (*SourceArtifact)(nil), next: (*Emission)(nil)},
		{name: "nil source and emission", first: (*SourceArtifact)(nil), next: emission},
		{name: "source and nil emission", first: &SourceArtifact{}, next: (*Emission)(nil)},
		{name: "nil metadata", first: &SourceArtifact{}, next: &SourceArtifact{}, equal: true},
		{name: "empty metadata", first: &SourceArtifact{Metadata: map[string]string{}}, next: &SourceArtifact{Metadata: map[string]string{}}, equal: true},
		{name: "nil and empty metadata", first: &SourceArtifact{}, next: &SourceArtifact{Metadata: map[string]string{}}},
		{name: "unknown content", first: mutable, next: &SourceArtifact{FS: &hookFS{}}},
		{name: "nil and empty children", first: &Emission{}, next: &Emission{Children: []Child{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, artifactValueEqual(tc.first, tc.next), tc.equal)
			assert.Equal(t, artifactValueEqual(tc.next, tc.first), tc.equal)
		})
	}
}

func TestArtifactValueEqual_Emission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Emission)
		equal  bool
	}{
		{name: "equal", change: func(*Emission) {}, equal: true},
		{name: "completeness", change: func(e *Emission) { e.Complete = false }},
		{name: "child order", change: func(e *Emission) { e.Children = []Child{e.Children[1], e.Children[0]} }},
		{name: "child declaration", change: func(e *Emission) { e.Children = []Child{emissionChild("other", "same"), e.Children[1]} }},
		{name: "fingerprint", change: func(e *Emission) { e.Fingerprint = "other" }},
		{name: "failures", change: func(e *Emission) { e.Failures = []manifest.DocOccurrence{{Ordinal: 1}} }},
		{
			name: "content",
			change: func(e *Emission) {
				e.Manifests = []manifest.DocOccurrence{{Ordinal: 0, Document: map[string]any{"value": "other"}}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := &Emission{
				Children:    []Child{emissionChild("first", "same"), emissionChild("second", "same")},
				Manifests:   []manifest.DocOccurrence{{Ordinal: 0, Document: map[string]any{"value": "same"}}},
				Fingerprint: "same", Complete: true,
			}
			next := *first
			tc.change(&next)
			assert.Equal(t, artifactValueEqual(first, &next), tc.equal)
			assert.Equal(t, artifactValueEqual(&next, first), tc.equal)
		})
	}
}

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
