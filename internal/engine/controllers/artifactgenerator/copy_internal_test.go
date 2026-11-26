package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func compressedForTest(t *testing.T, files fstest.MapFS) []byte {
	t.Helper()
	var data bytes.Buffer
	gz := gzip.NewWriter(&data)
	w := tar.NewWriter(gz)
	{
		err := w.AddFS(files)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := w.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := gz.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	return data.Bytes()
}

func TestEvaluate(t *testing.T) {
	t.Parallel()
	t.Run("optional_and_required_cache", func(t *testing.T) {
		t.Parallel()
		owner := cachetest.New(t, "", cache.New)
		e := evaluation{
			identity: []inputIdentity{{Alias: "repo", Input: store.ArtifactInput{Content: "generation"}}},
			inputs:   map[string]fs.FS{"repo": fstest.MapFS{"app/file": {Data: []byte("file")}}},
		}
		for _, tc := range []struct {
			source, cause string
			exclude       []string
		}{
			{"@repo/missing", "source 'missing' does not exist", []string{"missing"}},
			{"@repo/empty/*.txt", "no files match pattern 'empty/*.txt' in source 'repo'", nil},
			{"@repo/app/*", "all files matching pattern 'app/*' in source 'repo' were excluded", []string{"file"}},
		} {
			for _, optional := range []bool{true, false} {
				req := request{Output: manifest.ArtifactGeneratorOutput{Copy: []manifest.ArtifactGeneratorCopy{
					{From: tc.source, To: "@artifact/", Optional: optional, Exclude: tc.exclude},
				}}}
				composition, err := e.composition(prepareCompositionRequest(t, &e, &req))
				assert.NoError(t, err)
				out, _, _, err := owner.Compose(t.Context(), composition, &cache.Policy{MaxAge: cache.Forever})
				if optional {
					assert.NoError(t, err)
					entries, readErr := fs.ReadDir(out, ".")
					assert.NoError(t, readErr)
					assert.Equal(t, len(entries), 0)
					continue
				}
				assert.FatalIf(t, out != nil || err == nil || !strings.Contains(err.Error(), tc.cause),
					"required miss for %s reused the optional result: %v, %v", tc.source, out, err)
			}
		}
	})
	for _, tc := range []struct{ name, source string }{
		{"optional_extract_literal_failure", "@repo/bad.tgz"},
		{"optional_extract_glob_failure", "@repo/*.tgz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", cache.New)
			e := evaluation{
				identity: []inputIdentity{{Alias: "repo", Input: store.ArtifactInput{Content: "generation"}}},
				inputs:   map[string]fs.FS{"repo": fstest.MapFS{"bad.tgz": {Data: []byte("invalid gzip header")}}},
			}
			req := request{Output: manifest.ArtifactGeneratorOutput{Copy: []manifest.ArtifactGeneratorCopy{
				{From: tc.source, To: "@artifact/", Optional: true, Strategy: strategyExtract},
			}}}
			composition, err := e.composition(prepareCompositionRequest(t, &e, &req))
			assert.NoError(t, err)
			out, _, _, err := owner.Compose(t.Context(), composition, &cache.Policy{MaxAge: cache.Forever})
			assert.Equal(t, out == nil, true)
			assert.ErrorIf(t, !errors.Is(err, gzip.ErrHeader), "optional extract %q: got %v, want %v",
				tc.source, err, gzip.ErrHeader)
		})
	}
	for _, tc := range []struct {
		name, from, to, want string
	}{
		{"unknown_alias", "@absent/file", "@artifact/file", "source alias 'absent' not found"},
		{"bare_alias", "@repo", "@artifact/", "invalid copy source '@repo': source format must be '@alias/pattern'"},
		{"destination_prefix", "@repo/app/file", "file", "invalid copy destination 'file': destination must start with '@artifact/'"},
		{"destination_before_alias", "@absent/file", "file", "invalid copy destination 'file': destination must start with '@artifact/'"},
		{"source_comma_limit_first", "@absent/{" + strings.Repeat("a,", 21) + "z}", "file", "glob contains more than 20 brace commas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := evaluation{
				identity: []inputIdentity{{Alias: "repo"}},
				inputs: map[string]fs.FS{
					"repo": fstest.MapFS{"app/file": {Data: []byte("file")}},
				},
			}
			output := manifest.ArtifactGeneratorOutput{
				Copy: []manifest.ArtifactGeneratorCopy{{From: tc.from, To: tc.to}},
			}
			_, err := e.composition(&request{Output: output})
			assert.FatalIf(
				t,
				err == nil || !strings.HasSuffix(err.Error(), tc.want),
				"composition() = %v; want %s",
				err,
				tc.want,
			)
		})
	}
}

func TestEvaluate_ExtractIdentity(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", cache.New)
	identities := []inputIdentity{
		{Alias: "repo", Input: store.ArtifactInput{Key: "family", Content: "generation", Digest: "generation"}},
	}
	recorder := &manifest.WorkRecorder{
		Key: manifest.WorkKey{Owner: manifest.NamedResource{Kind: "ArtifactGenerator", Name: "generator"}},
	}
	scope := recorder.Scope()
	e := evaluation{
		maxAge:   cache.Forever,
		identity: identities,
		inputs: map[string]fs.FS{
			"repo": fstest.MapFS{
				"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"file": {Data: []byte("bytes")}})},
			},
		},
	}
	req := request{
		Output: manifest.ArtifactGeneratorOutput{
			Name: "artifact",
			Copy: []manifest.ArtifactGeneratorCopy{
				{From: "@repo/archive.tgz", To: "@artifact/out", Strategy: strategyExtract},
			},
		},
	}
	req.work.Scope = scope
	composition, err := e.composition(prepareCompositionRequest(t, &e, &req))
	assert.FatalIf(t, err != nil, "%v", err)
	_, _, _, err = owner.Compose(t.Context(), composition, &cache.Policy{MaxAge: cache.Forever, Work: &req.work})
	assert.FatalIf(t, err != nil, "%v", err)
	called := false
	composition.Copies[0].To = "other"
	composition.Inputs[0].FS = extractionProbeFS{FS: composition.Inputs[0].FS, opened: &called}
	input, _, id, err := owner.Compose(
		t.Context(),
		composition,
		&cache.Policy{MaxAge: cache.Forever},
	)
	assert.FatalIf(
		t,
		err != nil || input == nil || id == "" || called,
		"nested extraction identity = %v, %q, %v; produced=%v",
		input,
		id,
		err,
		called,
	)
	data, err := fs.ReadFile(input, "other/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "bytes")
}

type extractionProbeFS struct {
	fs.FS

	opened *bool
}

func (f extractionProbeFS) Open(string) (fs.File, error) {
	*f.opened = true
	return nil, errors.New("unexpected extraction")
}

func (f extractionProbeFS) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(f.FS, name)
}
