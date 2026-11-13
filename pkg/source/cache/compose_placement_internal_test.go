package cache

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/tree"
)

type compositionPlacementCase struct {
	name        string
	base, input fstest.MapFS
	copies      []Copy
	ignore      string
	want        map[string]compositionFile
	fail        bool
}

func TestCompose_PlaceTree(t *testing.T) {
	t.Parallel()
	for _, tc := range []compositionPlacementCase{
		{
			name: "exact file and base", base: fstest.MapFS{"keep": {Data: []byte("base")}},
			input:  fstest.MapFS{"file": {Data: []byte("input"), Mode: 0o755}},
			copies: []Copy{{From: "file", To: "nested/renamed", Mode: PlaceTree}},
			want:   map[string]compositionFile{"keep": {Data: "base", Mode: 0o600}, "nested": {Mode: fs.ModeDir | 0o750}, "nested/renamed": {Data: "input", Mode: 0o700}},
		},
		{
			name: "root selection", input: fstest.MapFS{"file": {Data: []byte("input")}},
			copies: []Copy{{From: ".", To: "repository", Mode: PlaceTree}},
			want:   map[string]compositionFile{"repository": {Mode: fs.ModeDir | 0o750}, "repository/file": {Data: "input", Mode: 0o600}},
		},
		{
			name: "selected directory", input: fstest.MapFS{"source/file": {Data: []byte("input")}},
			copies: []Copy{{From: "source", To: "destination", Mode: PlaceTree}},
			want:   map[string]compositionFile{"destination": {Mode: fs.ModeDir | 0o750}, "destination/file": {Data: "input", Mode: 0o600}},
		},
		{
			name: "existing empty directory", base: fstest.MapFS{"destination": {Mode: fs.ModeDir | 0o755}},
			input:  fstest.MapFS{"source/file": {Data: []byte("input")}},
			copies: []Copy{{From: "source", To: "destination", Mode: PlaceTree}},
			fail:   true,
		},
		{
			name: "empty selected directory", input: fstest.MapFS{"empty": {Mode: fs.ModeDir | 0o711}},
			copies: []Copy{{From: "empty", To: "destination", Mode: PlaceTree}},
			want:   map[string]compositionFile{"destination": {Mode: fs.ModeDir | 0o750}},
		},
		{
			name: "root destination", base: fstest.MapFS{}, input: fstest.MapFS{"source/file": {Data: []byte("input")}},
			copies: []Copy{{From: "source", To: ".", Mode: PlaceTree}},
			fail:   true,
		},
		{
			name: "occupied root", base: fstest.MapFS{"keep": {}}, input: fstest.MapFS{"file": {}},
			copies: []Copy{{To: ".", Mode: PlaceTree}}, fail: true,
		},
		{
			name: "nonempty destination", base: fstest.MapFS{"destination/keep": {}}, input: fstest.MapFS{"source/file": {}},
			copies: []Copy{{From: "source", To: "destination", Mode: PlaceTree}}, fail: true,
		},
		{
			name: "directory over file", base: fstest.MapFS{"destination": {}}, input: fstest.MapFS{"source/file": {}},
			copies: []Copy{{From: "source", To: "destination", Mode: PlaceTree}}, fail: true,
		},
		{
			name: "file over directory", base: fstest.MapFS{"destination": {Mode: fs.ModeDir | 0o755}}, input: fstest.MapFS{"file": {}},
			copies: []Copy{{From: "file", To: "destination", Mode: PlaceTree}}, fail: true,
		},
		{
			name: "invalid parent", base: fstest.MapFS{"parent": {}}, input: fstest.MapFS{"file": {}},
			copies: []Copy{{From: "file", To: "parent/file", Mode: PlaceTree}}, fail: true,
		},
		{name: "missing source", input: fstest.MapFS{}, copies: []Copy{{From: "missing", To: "destination", Mode: PlaceTree}}, fail: true},
		{
			name: "literal glob", input: fstest.MapFS{"literal[*]": {Data: []byte("literal")}, "literalx": {Data: []byte("wrong")}},
			copies: []Copy{{From: "literal[*]", To: "destination", Mode: PlaceTree}},
			want:   map[string]compositionFile{"destination": {Data: "literal", Mode: 0o600}},
		},
		{
			name: "secure normalization", input: fstest.MapFS{"file": {Data: []byte("input")}},
			copies: []Copy{{From: "/../../dir/../file", To: "/../../dir/../destination", Mode: PlaceTree}},
			want:   map[string]compositionFile{"destination": {Data: "input", Mode: 0o600}},
		},
		{
			name: "repeated file", input: fstest.MapFS{"first": {Data: []byte("first")}, "last": {Data: []byte("last"), Mode: 0o711}},
			copies: []Copy{{From: "first", To: "destination", Mode: PlaceTree}, {From: "last", To: "destination", Mode: PlaceTree}},
			want:   map[string]compositionFile{"destination": {Data: "last", Mode: 0o700}},
		},
		{
			name: "failure after successful copy", input: fstest.MapFS{"file": {Data: []byte("input")}},
			copies: []Copy{{From: "file", To: "first", Mode: PlaceTree}, {From: "missing", To: "second", Mode: PlaceTree}}, fail: true,
		},
		{
			name: "copied ignore and reinclusion", base: fstest.MapFS{"hidden.yaml": {Data: []byte("keep")}, "drop.yaml": {Data: []byte("drop")}},
			input:  fstest.MapFS{"policy": {Data: []byte("*.yaml\n!hidden.yaml\n")}},
			copies: []Copy{{From: "policy", To: ".sourceignore", Mode: PlaceTree}},
			want:   map[string]compositionFile{"hidden.yaml": {Data: "keep", Mode: 0o600}, ".sourceignore": {Data: "*.yaml\n!hidden.yaml\n", Mode: 0o600}},
		},
		{
			name: "authored ignore after copied policy", base: fstest.MapFS{"keep": {Data: []byte("keep")}},
			input: fstest.MapFS{"file": {Data: []byte("drop")}}, ignore: "destination\n",
			copies: []Copy{{From: "file", To: "destination", Mode: PlaceTree}},
			want:   map[string]compositionFile{"keep": {Data: "keep", Mode: 0o600}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, backend := range []string{"memory", "disk"} {
				t.Run(backend, func(t *testing.T) {
					t.Parallel()
					testComposePlaceTree(t, &tc, backend)
				})
			}
		})
	}
}

func testComposePlaceTree(t *testing.T, tc *compositionPlacementCase, backend string) {
	t.Helper()
	root := ""
	if backend == "disk" {
		root = t.TempDir()
	}
	owner := cachetest.New(t, root, New)
	request := Composition{
		Inputs: []CompositionInput{{FS: tc.input, Content: "input"}},
		Copies: tc.copies,
		Policy: "source",
		Ignore: tc.ignore,
	}
	if tc.base != nil {
		request.Base = CompositionInput{FS: tc.base, Content: "base"}
	}
	beforeInput := compositionFiles(t, tc.input)
	var beforeBase map[string]compositionFile
	if tc.base != nil {
		beforeBase = compositionFiles(t, tc.base)
	}
	for range 2 {
		output, descriptor, key, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
		if tc.fail {
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, output == nil, true)
			assert.Equal(t, key, "")
			assert.Equal(t, descriptor.Digest.String(), "")
		} else {
			assert.NoError(t, err)
			assert.Diff(t, compositionFiles(t, output), tc.want)
		}
	}
	assert.Diff(t, compositionFiles(t, tc.input), beforeInput)
	if tc.base != nil {
		assert.Diff(t, compositionFiles(t, tc.base), beforeBase)
	}
}

func TestCompose_PlacementLinks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target, from, to string
		fail                   bool
	}{
		{name: "destination alias", target: "actual", from: "file", to: "alias/file"},
		{name: "absolute destination alias", target: "/actual", from: "file", to: "alias/file"},
		{name: "confined destination alias", target: "../../actual", from: "file", to: "alias/file"},
		{name: "cyclic destination", target: "alias", from: "file", to: "alias/file", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := tree.NewBuilder(fstest.MapFS{"actual": {Mode: fs.ModeDir | 0o755}})
			assert.NoError(t, base.AddSymlink("alias", tc.target))
			owner := cachetest.New(t, "", New)
			output, _, key, err := owner.Compose(t.Context(), Composition{
				Base:   CompositionInput{FS: base, Content: tc.name},
				Inputs: []CompositionInput{{FS: fstest.MapFS{"file": {Data: []byte("input")}}, Content: "input"}},
				Copies: []Copy{{From: tc.from, To: tc.to, Mode: PlaceTree}}, Policy: "source",
			}, &Policy{MaxAge: Forever})
			if tc.fail {
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.Equal(t, output == nil, true)
				assert.Equal(t, key, "")
				return
			}
			assert.NoError(t, err)
			assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{
				"actual": {Mode: fs.ModeDir | 0o750}, "actual/file": {Data: "input", Mode: 0o600},
			})
			link, err := fs.ReadLink(base, "alias")
			assert.NoError(t, err)
			assert.Equal(t, link, tc.target)
		})
	}
}

func TestCompose_SourceLinkSelection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target string
		fail         bool
	}{
		{name: "relative", target: "file"},
		{name: "absolute", target: "/file"},
		{name: "dangling", target: "missing", fail: true},
		{name: "cyclic", target: "alias", fail: true},
		{name: "escape cannot read outside", target: "../../outside", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := tree.NewBuilder(fstest.MapFS{"file": {Data: []byte("input")}})
			assert.NoError(t, input.AddSymlink("alias", tc.target))
			owner := cachetest.New(t, "", New)
			output, _, key, err := owner.Compose(t.Context(), Composition{
				Inputs: []CompositionInput{{FS: input, Content: tc.name}},
				Copies: []Copy{{From: "alias", To: "out", Mode: PlaceTree}}, Policy: "source",
			}, &Policy{MaxAge: Forever})
			if tc.fail {
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.Equal(t, output == nil, true)
				assert.Equal(t, key, "")
				return
			}
			assert.NoError(t, err)
			assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{"out": {Data: "input", Mode: 0o600}})
		})
	}
}
