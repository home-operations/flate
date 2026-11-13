package artifactgenerator

import (
	"encoding/json"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:musttag // Identity serialization MUST retain its existing untagged fields.
func TestComposition_IdentityFacts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*evaluation, *request)
	}{
		{name: "unchanged", change: func(*evaluation, *request) {}},
		{name: "input generation", change: func(e *evaluation, _ *request) { e.identity[0].Input.Content = "changed" }},
		{name: "unused generation", change: func(e *evaluation, _ *request) { e.identity[1].Input.Content = "changed" }},
		{name: "input order", change: func(e *evaluation, _ *request) { e.identity[0], e.identity[1] = e.identity[1], e.identity[0] }},
		{name: "operation order", change: func(_ *evaluation, r *request) {
			r.Output.Copy[0], r.Output.Copy[1] = r.Output.Copy[1], r.Output.Copy[0]
		}},
		{name: "authored source", change: func(_ *evaluation, r *request) { r.Output.Copy[0].From += "/" }},
		{name: "authored destination", change: func(_ *evaluation, r *request) { r.Output.Copy[0].To += "/" }},
		{name: "strategy", change: func(_ *evaluation, r *request) { r.Output.Copy[0].Strategy = "Merge" }},
		{name: "exclusion", change: func(_ *evaluation, r *request) { r.Output.Copy[0].Exclude = []string{"ignored"} }},
		{name: "empty exclusions", change: func(_ *evaluation, r *request) { r.Output.Copy[0].Exclude = []string{} }},
		{name: "name", change: func(_ *evaluation, r *request) { r.Output.Name = "changed" }},
		{name: "capture", change: func(_ *evaluation, r *request) { r.Captures["App"] = "changed" }},
		{name: "revision selector", change: func(_ *evaluation, r *request) { r.Output.Revision = "@unused" }},
		{name: "origin selector", change: func(_ *evaluation, r *request) { r.Output.OriginRevision = "@unused" }},
		{name: "consumed revision", change: func(e *evaluation, _ *request) { e.identity[0].Input.Revision = "changed" }},
		{name: "consumed origin", change: func(e *evaluation, _ *request) { e.identity[0].Input.Origin = "changed" }},
		{name: "origin presence", change: func(e *evaluation, _ *request) { e.identity[0].Input.OriginPresent = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{
				Group:     "source.extensions.fluxcd.io",
				Kind:      "ArtifactGenerator",
				Namespace: "ns",
				Name:      "generator",
			}
			e := evaluation{
				identity: []inputIdentity{
					{
						Alias: "repo", Source: manifest.NamedResource{Kind: "GitRepository", Name: "source"},
						Input: store.ArtifactInput{
							Present:       true,
							Readable:      true,
							Kind:          "GitRepository",
							Key:           "family",
							Content:       "generation",
							Digest:        "digest",
							LocalRoot:     "root",
							LocalPath:     "path",
							InputAlias:    true,
							Revision:      "revision",
							Origin:        "origin",
							OriginPresent: true,
						},
					},
					{
						Alias: "unused", Source: manifest.NamedResource{Kind: "OCIRepository", Name: "unused"},
						Input: store.ArtifactInput{Content: "unused generation"},
					},
				},
				inputs: map[string]fs.FS{
					"unused": fstest.MapFS{"unused": {}},
					"repo":   fstest.MapFS{"file": {}},
				},
			}
			req := request{
				Output: manifest.ArtifactGeneratorOutput{
					Name:           "artifact",
					Revision:       "@repo",
					OriginRevision: "@repo",
					Copy: []manifest.ArtifactGeneratorCopy{
						{
							From: "@repo/file",
							To:   "@artifact/out",
						},
						{From: "@repo/*.tgz", To: "@artifact/extracted", Strategy: "Extract"},
					},
				},
				Captures: map[string]string{"App": "app"},
			}
			baseline, err := e.composition(id, &req)
			assert.NoError(t, err)
			baselineBytes, err := json.Marshal(baseline.Content)
			assert.NoError(t, err)
			tc.change(&e, &req)
			before, err := json.Marshal(req)
			assert.NoError(t, err)
			got, err := e.composition(id, &req)
			assert.NoError(t, err)
			want := struct {
				Namespace string
				Generator manifest.NamedResource
				Request   request
				Inputs    []inputIdentity
			}{"artifactgenerator/output/v1", id, req, e.identity}
			wantBytes, err := json.Marshal(want)
			assert.NoError(t, err)
			gotBytes, err := json.Marshal(got.Content)
			assert.NoError(t, err)
			assert.Equal(t, string(gotBytes), string(wantBytes))
			assert.Equal(t, string(gotBytes) == string(baselineBytes), tc.name == "unchanged")
			after, err := json.Marshal(req)
			assert.NoError(t, err)
			assert.Equal(t, string(after), string(before))
			assert.Equal(t, got.Policy, "fixed")
			wantContent := map[string]string{"repo": "generation", "unused": "unused generation"}
			switch tc.name {
			case "input generation":
				wantContent["repo"] = "changed"
			case "unused generation":
				wantContent["unused"] = "changed"
			}
			for i := range e.identity {
				assert.Equal(t, got.Inputs[i].Content, wantContent[e.identity[i].Alias])
			}
		})
	}
}

//nolint:musttag // Extraction identity MUST retain its existing untagged fields.
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
	got, err := e.composition(manifest.NamedResource{}, &req)
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
		assert.Equal(t, op.ExtractContent != nil, i >= 3)
		if i < 3 {
			continue
		}
		for _, selected := range []string{"selected/archive.TGZ", "other/archive.TGZ"} {
			alias := "first"
			if i == 4 {
				alias = "second"
			}
			want := struct {
				Namespace string
				Inputs    []inputIdentity
				Selection archiveSelection
			}{"artifactgenerator/extract/v1", e.identity, archiveSelection{alias, selected, "normalized"}}
			wantBytes, err := json.Marshal(want)
			assert.NoError(t, err)
			gotBytes, err := json.Marshal(op.ExtractContent(selected, "normalized"))
			assert.NoError(t, err)
			assert.Equal(t, string(gotBytes), string(wantBytes))
		}
	}
}
