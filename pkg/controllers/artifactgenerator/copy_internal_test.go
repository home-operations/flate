package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
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
	for _, tc := range []struct {
		name       string
		operations []manifest.ArtifactGeneratorCopy
		invalid    bool
	}{
		{"unknown_alias", []manifest.ArtifactGeneratorCopy{{From: "@absent/file", To: "@artifact/file"}}, true},
		{"bare_alias", []manifest.ArtifactGeneratorCopy{{From: "@repo", To: "@artifact/"}}, true},
		{"destination_prefix", []manifest.ArtifactGeneratorCopy{{From: "@repo/app/file", To: "file"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := evaluation{
				identity: []inputIdentity{{Alias: "repo"}},
				inputs: map[string]fs.FS{
					"repo": fstest.MapFS{"app/file": {Data: []byte("file")}},
				},
			}
			_, err := e.composition(
				manifest.NamedResource{},
				&request{Output: manifest.ArtifactGeneratorOutput{Copy: tc.operations}},
			)
			assert.FatalIf(t, (err != nil) != tc.invalid, "evaluate() = %v; invalid=%v", err, tc.invalid)
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
	selection := archiveSelection{Alias: "repo", Path: "archive.tgz", Destination: "out"}
	req := request{
		Output: manifest.ArtifactGeneratorOutput{
			Name: "artifact",
			Copy: []manifest.ArtifactGeneratorCopy{
				{From: "@repo/archive.tgz", To: "@artifact/out", Strategy: strategyExtract},
			},
		},
	}
	req.work.Scope = scope
	composition, err := e.composition(recorder.Key.Owner, &req)
	assert.FatalIf(t, err != nil, "%v", err)
	_, _, _, err = owner.Compose(t.Context(), composition, &cache.Policy{MaxAge: cache.Forever, Work: &req.work})
	assert.FatalIf(t, err != nil, "%v", err)
	want := struct {
		Namespace string
		Inputs    []inputIdentity
		Selection archiveSelection
	}{"artifactgenerator/extract/v1", identities, selection}
	//nolint:musttag // The fixture MUST match the production identity's untagged JSON names.
	wantBytes, err := json.Marshal(want)
	assert.NoError(t, err)
	gotBytes, err := json.Marshal(composition.Copies[0].ExtractContent(selection.Path, selection.Destination))
	assert.NoError(t, err)
	assert.Equal(t, string(gotBytes), string(wantBytes))
	called := false
	composition.Content = "another outer composition with the same extraction"
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
