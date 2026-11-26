package artifactgenerator

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	sourcewatcherv1 "github.com/fluxcd/source-watcher/api/v2/v1beta1"

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
	req := request{Output: sourcewatcherv1.OutputArtifact{Copy: []sourcewatcherv1.CopyOperation{
		{From: "@first/dir/", To: "@artifact/out/", Exclude: []string{"skip"}},
		{From: "@second/file", To: "@artifact/value", Strategy: sourcewatcherv1.OverwriteStrategy},
		{From: "@first/overlay", To: "@artifact/value", Strategy: sourcewatcherv1.MergeStrategy},
		{From: "@first/**/*.TGZ", To: "@artifact/one/", Strategy: sourcewatcherv1.ExtractStrategy},
		{From: "@second/*.tar.gz", To: "@artifact/two", Strategy: sourcewatcherv1.ExtractStrategy},
	}}}
	got, err := e.composition(prepareCompositionRequest(t, &e, &req))
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

func TestComposition_ResolvedOutputs(t *testing.T) {
	t.Parallel()
	spec := manifest.ArtifactGeneratorSpec{
		PathPattern: "@repo/apps/{App}",
		Sources: []sourcewatcherv1.SourceReference{
			{Alias: "repo", Name: "repo", Kind: manifest.KindGitRepository},
		},
		OutputArtifacts: []sourcewatcherv1.OutputArtifact{{
			Name: "{App}", InputsFrom: "@artifact/{App}.yaml",
			Copy: []sourcewatcherv1.CopyOperation{
				{From: "@repo/apps/{App}/values.yaml", To: "@artifact/{App}.yaml", Exclude: []string{"{App}.bak"}},
				{From: "@repo/apps/{App}/values.yaml", To: "@artifact/backup/{App}.yaml"},
				{From: "@repo/shared.yaml", To: "@artifact/shared.yaml", Strategy: sourcewatcherv1.MergeStrategy},
				{From: "@repo/apps/{App}/values.yaml", To: "@artifact/again/{App}.yaml"},
				{
					From:     "@repo/shared.yaml",
					To:       "@artifact/backup/shared.yaml",
					Strategy: sourcewatcherv1.MergeStrategy,
				},
			},
		}},
	}
	prepared, err := validateSpec(&spec, "apps")
	assert.NoError(t, err)
	e := evaluation{
		identity:  []inputIdentity{{Alias: "repo"}},
		inputs:    map[string]fs.FS{"repo": fstest.MapFS{"apps/Prod/values.yaml": {}, "apps/Stage/values.yaml": {}}},
		selectors: &prepared.selectors, copyModes: prepared.copyModes,
	}
	key := selectorKey{use: copySourceSelector, text: "@repo/shared.yaml"}
	parsed, ok := prepared.selectors.more[key]
	assert.FatalIf(t, !ok, "missing prepared copy source %q", key.text)
	parsed.selector.Path = "prepared.yaml"
	prepared.selectors.more[key] = parsed
	authoredFirst, authoredCount := prepared.selectors.first, len(prepared.selectors.more)
	requests, err := prepared.expand(t.Context(), "apps", e.inputs)
	assert.NoError(t, err)
	assert.Equal(t, len(requests), 2)
	for i, dir := range []string{"Prod", "Stage"} {
		req := &requests[i]
		got, err := e.composition(req)
		assert.NoError(t, err)
		from := "apps/" + dir + "/values.yaml"
		assert.Diff(t, got.Copies, []cache.Copy{
			{From: from, To: dir + ".yaml", Exclude: []string{"{App}.bak"}},
			{From: from, To: "backup/" + dir + ".yaml"},
			{From: "prepared.yaml", To: "shared.yaml", Mode: cache.MergeYAML},
			{From: from, To: "again/" + dir + ".yaml"},
			{From: "prepared.yaml", To: "backup/shared.yaml", Mode: cache.MergeYAML},
		})
		assert.Equal(t, req.inputsFrom.Path, dir+".yaml")
		assert.Equal(t, req.inputsFrom.StagedFile(), true)
	}
	assert.Equal(t, prepared.selectors.first, authoredFirst)
	assert.Equal(t, len(prepared.selectors.more), authoredCount)
	for _, tc := range []struct {
		name, from, to, want string
	}{
		{"invalid expanded source", "@repo/{App}:file", "@artifact/file", "invalid copy source '@repo/A:file':"},
		{"invalid expanded destination", "@repo/file", "@artifact/{App}:file", "invalid copy destination '@artifact/A:file':"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			invalid := spec
			invalid.OutputArtifacts = []sourcewatcherv1.OutputArtifact{{
				Name: "{App}", Copy: []sourcewatcherv1.CopyOperation{{From: tc.from, To: tc.to}},
			}}
			prepared, err := validateSpec(&invalid, "apps")
			assert.NoError(t, err)
			e := evaluation{
				identity:  []inputIdentity{{Alias: "repo"}},
				inputs:    map[string]fs.FS{"repo": fstest.MapFS{"apps/A/file": {}}},
				selectors: &prepared.selectors,
			}
			requests, err := prepared.expand(t.Context(), "apps", e.inputs)
			assert.NoError(t, err)
			assert.FatalIf(t, len(requests) != 1, "got %d requests, want one", len(requests))
			_, err = e.composition(&requests[0])
			assert.ErrorIf(
				t, err == nil || !strings.HasPrefix(err.Error(), tc.want), "got %v, want prefix %q", err, tc.want,
			)
		})
	}
}

func prepareCompositionRequest(t *testing.T, e *evaluation, req *request) *request {
	t.Helper()
	var prepared generatorSpec
	for i := range req.Output.Copy {
		assert.NoError(t, prepared.prepareOperation(&req.Output.Copy[i], false))
	}
	e.selectors, e.copyModes = &prepared.selectors, prepared.copyModes
	return req
}
