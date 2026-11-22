package artifactgenerator

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestComposition_Lowering(t *testing.T) {
	t.Parallel()
	e := evaluation{
		identity: []inputIdentity{
			{Alias: "second", Input: store.ArtifactInput{Content: "second generation"}},
			{Alias: "first", Input: store.ArtifactInput{Content: "first generation"}},
			{Alias: "unused", Input: store.ArtifactInput{Content: "unused generation"}},
		},
		inputs: map[string]fs.FS{
			"first":  fstest.MapFS{"file": {Data: []byte("first")}},
			"second": fstest.MapFS{"file": {Data: []byte("second")}},
			"unused": fstest.MapFS{"file": {Data: []byte("unused")}},
		},
	}
	req := request{Output: manifest.ArtifactGeneratorOutput{Copy: []manifest.ArtifactGeneratorCopy{
		{From: "@first/dir/", To: "@artifact/out/", Exclude: []string{"skip"}},
		{From: "@second/file", To: "@artifact/value", Strategy: "Overwrite"},
		{From: "@first/overlay", To: "@artifact/value", Strategy: "Merge"},
		{From: "@first/**/*.TGZ", To: "@artifact/one/", Strategy: "Extract"},
		{From: "@second/*.tar.gz", To: "@artifact/two", Strategy: "Extract"},
	}}}
	got, err := e.composition(&req)
	assert.NoError(t, err)
	assert.Equal(t, len(got.Inputs), 3)
	for i, want := range []string{"second", "first", "unused"} {
		data, err := fs.ReadFile(got.Inputs[i].FS, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), want)
		assert.Equal(t, got.Inputs[i].Content, want+" generation")
	}
	wantCopies := []cache.Copy{
		{Input: 1, From: "dir/", To: "out/", Exclude: []string{"skip"}, Mode: cache.CopyFiles},
		{Input: 0, From: "file", To: "value", Mode: cache.CopyFiles},
		{Input: 1, From: "overlay", To: "value", Mode: cache.MergeYAML},
		{Input: 1, From: "**/*.TGZ", To: "one/", Mode: cache.ExtractArchive},
		{Input: 0, From: "*.tar.gz", To: "two", Mode: cache.ExtractArchive},
	}
	assert.Equal(t, len(got.Copies), len(wantCopies))
	for i, want := range wantCopies {
		op := got.Copies[i]
		assert.Equal(t, op.Input, want.Input)
		assert.Equal(t, op.From, want.From)
		assert.Equal(t, op.To, want.To)
		assert.Equal(t, op.Mode, want.Mode)
		assert.Diff(t, op.Exclude, want.Exclude)
	}
}
