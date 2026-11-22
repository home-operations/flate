package cache

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func TestCompose_CacheDerivesSemanticIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		change      func(*Composition)
		treeChanges bool
	}{
		{"base generation", func(r *Composition) { r.Base.Content = "base-new" }, false},
		{"used input generation", func(r *Composition) { r.Inputs[0].Content = "input-new" }, false},
		{"input alias", func(r *Composition) { r.Inputs[0].Alias = "renamed" }, false},
		{"unused input generation", func(r *Composition) { r.Inputs[1].Content = "unused-new" }, false},
		{"input order", func(r *Composition) { r.Inputs[0], r.Inputs[1] = r.Inputs[1], r.Inputs[0] }, false},
		{"selected input", func(r *Composition) { r.Copies[0].Input = 1 }, false},
		{"operation order", func(r *Composition) { r.Copies[0], r.Copies[1] = r.Copies[1], r.Copies[0] }, false},
		{"source spelling", func(r *Composition) { r.Copies[0].From += "/" }, false},
		{"source path", func(r *Composition) { r.Copies[0].From = "other" }, false},
		{"destination spelling", func(r *Composition) { r.Copies[0].To += "/" }, true},
		{"destination path", func(r *Composition) { r.Copies[0].To = "elsewhere" }, true},
		{"mode", func(r *Composition) { r.Copies[0].Mode = PlaceTree }, false},
		{"optional copy", func(r *Composition) { r.Copies[0].Optional = true }, false},
		{"exclusions", func(r *Composition) { r.Copies[0].Exclude = []string{"missing"} }, false},
		{"empty exclusions", func(r *Composition) { r.Copies[0].Exclude = []string{} }, false},
		{"ignore", func(r *Composition) { r.Ignore = "missing\n" }, false},
		{"policy", func(r *Composition) { r.Policy = "source" }, true},
		{"absent base", func(r *Composition) { r.Base = CompositionInput{} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", New)
			files := fstest.MapFS{"file": {Data: []byte("bytes")}, "other": {Data: []byte("bytes")}}
			request := Composition{
				Base: CompositionInput{FS: fstest.MapFS{}, Content: "base"},
				Inputs: []CompositionInput{
					{FS: files, Content: "first", Alias: "repo"},
					{FS: files, Content: "second"},
				},
				Copies: []Copy{{From: "file", To: "out"}, {From: "file", To: "second"}}, Policy: "fixed",
			}
			_, firstTree, first, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
			assert.NoError(t, err)
			tc.change(&request)
			_, secondTree, second, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
			assert.NoError(t, err)
			assert.Equal(t, first == second, false)
			firstDigest, secondDigest := firstTree.Annotations["flate.tree-digest"], secondTree.Annotations["flate.tree-digest"]
			assert.Equal(t, firstDigest == "", false)
			if request.Policy == "fixed" {
				assert.Equal(t, secondDigest == "", false)
			}
			assert.Equal(t, firstDigest != secondDigest, tc.treeChanges)
		})
	}
}

func TestCompose_GitIncludeIdentity(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", New)
	out, _, key, err := owner.Compose(t.Context(), Composition{
		Base: CompositionInput{
			FS: fstest.MapFS{"base.txt": {Data: []byte("base")}}, Content: "base-generation",
		},
		Inputs: []CompositionInput{{
			FS: fstest.MapFS{"dir/added.txt": {Data: []byte("include")}}, Content: "include-generation",
		}},
		Copies: []Copy{{From: "dir", To: "vendor", Mode: PlaceTree}},
		Policy: "source",
	}, &Policy{MaxAge: Forever})
	assert.NoError(t, err)
	assert.Equal(t, key, "3edd8ef334e1397ee2824d76aaa7a9fedcaf193aea6a6340c88427d0c381e038")
	assert.Equal(t, AcquisitionIdentity(out), "69bfa21e52312102d9408a83023d6a8514a693daaae26d4a04cde1a97e605993")
}

func TestCompose_MutableBaseRefresh(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", New)
	base := fstest.MapFS{"base": {Data: []byte("aaa"), Mode: 0o755}}
	request := Composition{Base: CompositionInput{FS: base}, Policy: "source"}
	_, _, absent, err := owner.Compose(t.Context(), Composition{Policy: "source"}, &Policy{MaxAge: Forever})
	assert.NoError(t, err)
	var previous string
	for _, text := range []string{"aaa", "bbb"} {
		base["base"].Data = []byte(text)
		output, _, key, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
		assert.NoError(t, err)
		assert.Equal(t, key == absent, false)
		if previous != "" {
			assert.Equal(t, key, previous)
		}
		assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{"base": {Data: text, Mode: 0o700}})
		previous = key
	}
}

func TestCompose_ExtractionIdentityIncludesInput(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", New)
	for _, generation := range []string{"first", "second"} {
		files := fstest.MapFS{
			"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"file": {Data: []byte(generation)}})},
		}
		output, _, _, err := owner.Compose(t.Context(), Composition{
			Inputs: []CompositionInput{{FS: files, Content: generation}},
			Copies: []Copy{{
				From: "archive.tgz", To: "out", Mode: ExtractArchive,
			}}, Policy: "fixed",
		}, &Policy{MaxAge: Forever})
		assert.NoError(t, err)
		assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{
			"out": {Mode: fs.ModeDir | 0o750}, "out/file": {Data: generation, Mode: 0o600},
		})
	}
}
