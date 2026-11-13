package cache

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func TestCompose_CacheDerivesSemanticIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Composition)
	}{
		{"base generation", func(r *Composition) { r.Base.Content = "base-new" }},
		{"used input generation", func(r *Composition) { r.Inputs[0].Content = "input-new" }},
		{"unused input generation", func(r *Composition) { r.Inputs[1].Content = "unused-new" }},
		{"input order", func(r *Composition) { r.Inputs[0], r.Inputs[1] = r.Inputs[1], r.Inputs[0] }},
		{"selected input", func(r *Composition) { r.Copies[0].Input = 1 }},
		{"operation order", func(r *Composition) { r.Copies[0], r.Copies[1] = r.Copies[1], r.Copies[0] }},
		{"source path", func(r *Composition) { r.Copies[0].From = "other" }},
		{"destination path", func(r *Composition) { r.Copies[0].To = "elsewhere" }},
		{"mode", func(r *Composition) { r.Copies[0].Mode = PlaceTree }},
		{"exclusions", func(r *Composition) { r.Copies[0].Exclude = []string{"missing"} }},
		{"empty exclusions", func(r *Composition) { r.Copies[0].Exclude = []string{} }},
		{"ignore", func(r *Composition) { r.Ignore = "missing\n" }},
		{"policy", func(r *Composition) { r.Policy = "fixed" }},
		{"absent base", func(r *Composition) { r.Base = CompositionInput{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", New)
			files := fstest.MapFS{"file": {Data: []byte("bytes")}, "other": {Data: []byte("bytes")}}
			request := Composition{
				Content: "unchanged caller facts", Base: CompositionInput{FS: fstest.MapFS{}, Content: "base"},
				Inputs: []CompositionInput{{FS: files, Content: "first"}, {FS: files, Content: "second"}},
				Copies: []Copy{{From: "file", To: "out"}, {From: "file", To: "second"}}, Policy: "source",
			}
			_, _, first, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
			assert.NoError(t, err)
			tc.change(&request)
			_, _, second, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
			assert.NoError(t, err)
			assert.Equal(t, first == second, false)
		})
	}
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
			Content: "unchanged context", Inputs: []CompositionInput{{FS: files, Content: generation}},
			Copies: []Copy{{
				From: "archive.tgz", To: "out", Mode: ExtractArchive,
				ExtractContent: func(string, string) any { return "unchanged extraction context" },
			}}, Policy: "fixed",
		}, &Policy{MaxAge: Forever})
		assert.NoError(t, err)
		assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{
			"out": {Mode: fs.ModeDir | 0o750}, "out/file": {Data: generation, Mode: 0o600},
		})
	}
}
